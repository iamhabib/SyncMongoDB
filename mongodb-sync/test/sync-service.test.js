const test = require('node:test');
const assert = require('node:assert');

process.env.REMOTE_MONGODB_URL = 'mongodb://localhost:27017/source';
process.env.LOCAL_MONGO_URL = 'mongodb://localhost:27017/target';

const OplogSyncService = require('../oplog-sync-service');
const SyncManager = require('../sync-manager');

test('OplogSyncService.processChange() unit tests', async (t) => {
  const createMockService = () => {
    const service = new OplogSyncService();

    service.syncManager = {
      updateSyncStateCalls: [],
      async updateSyncState(collectionName, operationTime, resumeToken, count) {
        this.updateSyncStateCalls.push({ collectionName, operationTime, resumeToken, count });
      },
      async resetSyncStateForReSync() {},
      async completeInitialSync() {}
    };

    const mockCollection = {
      replaceOneCalls: [],
      deleteOneCalls: [],
      dropCalls: 0,
      renameCalls: [],
      async replaceOne(query, doc, options) {
        this.replaceOneCalls.push({ query, doc, options });
        return { acknowledged: true };
      },
      async deleteOne(query) {
        this.deleteOneCalls.push({ query });
        return { acknowledged: true };
      },
      async drop() {
        this.dropCalls += 1;
      },
      async rename(newName, options) {
        this.renameCalls.push({ newName, options });
      }
    };

    return { service, mockCollection };
  };

  await t.test('should process INSERT change event using replaceOne (idempotent)', async () => {
    const { service, mockCollection } = createMockService();
    const changeEvent = {
      _id: { _data: 'resume_token_123' },
      operationType: 'insert',
      documentKey: { _id: 'doc_abc' },
      fullDocument: { _id: 'doc_abc', name: 'Alice', age: 30 },
      clusterTime: new Date('2026-08-23T12:00:00Z')
    };

    await service.processChange('users', changeEvent, mockCollection);

    assert.strictEqual(mockCollection.replaceOneCalls.length, 1);
    assert.deepStrictEqual(mockCollection.replaceOneCalls[0], {
      query: { _id: 'doc_abc' },
      doc: { _id: 'doc_abc', name: 'Alice', age: 30 },
      options: { upsert: true }
    });
    assert.strictEqual(mockCollection.deleteOneCalls.length, 0);

    assert.strictEqual(service.syncManager.updateSyncStateCalls.length, 1);
    assert.deepStrictEqual(service.syncManager.updateSyncStateCalls[0], {
      collectionName: 'users',
      operationTime: changeEvent.clusterTime,
      resumeToken: changeEvent._id,
      count: 1
    });
  });

  await t.test('should process UPDATE/REPLACE event with fullDocument using replaceOne', async () => {
    const { service, mockCollection } = createMockService();
    const changeEvent = {
      _id: { _data: 'resume_token_456' },
      operationType: 'update',
      documentKey: { _id: 'doc_abc' },
      fullDocument: { _id: 'doc_abc', name: 'Bob', age: 31 },
      clusterTime: new Date('2026-08-23T12:05:00Z')
    };

    await service.processChange('users', changeEvent, mockCollection);

    assert.strictEqual(mockCollection.replaceOneCalls.length, 1);
    assert.deepStrictEqual(mockCollection.replaceOneCalls[0], {
      query: { _id: 'doc_abc' },
      doc: { _id: 'doc_abc', name: 'Bob', age: 31 },
      options: { upsert: true }
    });
  });

  await t.test('should fallback to deleteOne on UPDATE/REPLACE event if fullDocument is missing', async () => {
    const { service, mockCollection } = createMockService();
    const changeEvent = {
      _id: { _data: 'resume_token_789' },
      operationType: 'update',
      documentKey: { _id: 'doc_abc' },
      fullDocument: null,
      clusterTime: new Date('2026-08-23T12:10:00Z')
    };

    await service.processChange('users', changeEvent, mockCollection);

    assert.strictEqual(mockCollection.replaceOneCalls.length, 0);
    assert.strictEqual(mockCollection.deleteOneCalls.length, 1);
    assert.deepStrictEqual(mockCollection.deleteOneCalls[0], {
      query: { _id: 'doc_abc' }
    });
  });

  await t.test('should process DELETE event using deleteOne', async () => {
    const { service, mockCollection } = createMockService();
    const changeEvent = {
      _id: { _data: 'resume_token_000' },
      operationType: 'delete',
      documentKey: { _id: 'doc_abc' },
      clusterTime: new Date('2026-08-23T12:15:00Z')
    };

    await service.processChange('users', changeEvent, mockCollection);

    assert.strictEqual(mockCollection.deleteOneCalls.length, 1);
  });

  await t.test('should drop local collection on remote drop', async () => {
    const { service, mockCollection } = createMockService();
    service.syncedCollections.add('users');
    let resetCalled = false;
    service.syncManager.resetSyncStateForReSync = async () => {
      resetCalled = true;
    };

    await service.processChange('users', { operationType: 'drop', ns: { coll: 'users' } }, mockCollection);

    assert.strictEqual(mockCollection.dropCalls, 1);
    assert.strictEqual(service.syncedCollections.has('users'), false);
    assert.strictEqual(resetCalled, true);
  });

  await t.test('should verify idempotency (repeating same INSERT/UPDATE/DELETE events)', async () => {
    const { service, mockCollection } = createMockService();
    const insertEvent = {
      _id: { _data: 'resume_token_1' },
      operationType: 'insert',
      documentKey: { _id: 'doc_1' },
      fullDocument: { _id: 'doc_1', data: 'hello' }
    };

    await service.processChange('items', insertEvent, mockCollection);
    await service.processChange('items', insertEvent, mockCollection);

    assert.strictEqual(mockCollection.replaceOneCalls.length, 2);
    assert.strictEqual(service.syncManager.updateSyncStateCalls.length, 2);

    const deleteEvent = {
      _id: { _data: 'resume_token_2' },
      operationType: 'delete',
      documentKey: { _id: 'doc_1' }
    };

    await service.processChange('items', deleteEvent, mockCollection);
    await service.processChange('items', deleteEvent, mockCollection);

    assert.strictEqual(mockCollection.deleteOneCalls.length, 2);
  });

  await t.test('should perform initial sync and copy documents in batches', async () => {
    const { service, mockCollection } = createMockService();
    service.isRunning = true;

    service.syncManager.updateInitialSyncProgressCalls = [];
    service.syncManager.completeInitialSyncCalls = [];

    service.syncManager.updateInitialSyncProgress = async (collectionName, lastCopiedId, resumeToken, count) => {
      service.syncManager.updateInitialSyncProgressCalls.push({
        collectionName,
        lastCopiedId,
        resumeToken,
        count
      });
    };

    service.syncManager.completeInitialSync = async (collectionName) => {
      service.syncManager.completeInitialSyncCalls.push({ collectionName });
    };

    const mockRemoteCollection = {
      watchCallsCount: 0,
      watch() {
        this.watchCallsCount++;
        return {
          resumeToken: { _data: 'initial_resume_token' },
          async tryNext() {},
          async close() {}
        };
      },
      async indexes() {
        return [
          { name: '_id_', key: { _id: 1 }, v: 2 },
          { name: 'name_1', key: { name: 1 }, unique: true, v: 2, ns: 'db.users' }
        ];
      },
      findCalls: [],
      find(query) {
        this.findCalls.push({ query });
        return {
          sort() {
            return {
              limit() {
                return {
                  async toArray() {
                    if (!query._id) {
                      return [
                        { _id: 'doc_1', name: 'Doc 1' },
                        { _id: 'doc_2', name: 'Doc 2' }
                      ];
                    }
                    return [];
                  }
                };
              }
            };
          }
        };
      }
    };

    mockCollection.createIndexCalls = [];
    mockCollection.createIndex = async (key, options) => {
      mockCollection.createIndexCalls.push({ key, options });
    };
    mockCollection.indexes = async () => [{ name: '_id_', key: { _id: 1 } }];
    mockCollection.bulkWriteCalls = [];
    mockCollection.bulkWrite = async (operations, options) => {
      mockCollection.bulkWriteCalls.push({ operations, options });
    };

    const syncState = {
      resumeToken: null,
      lastCopiedId: null,
      initialSyncCompleted: false
    };

    await service.performInitialSync('users', mockRemoteCollection, mockCollection, syncState);

    assert.strictEqual(mockCollection.createIndexCalls.length, 1);
    assert.deepStrictEqual(mockCollection.createIndexCalls[0], {
      key: { name: 1 },
      options: { name: 'name_1', unique: true }
    });
    assert.ok(!('v' in mockCollection.createIndexCalls[0].options));
    assert.ok(!('ns' in mockCollection.createIndexCalls[0].options));

    assert.strictEqual(mockRemoteCollection.watchCallsCount, 1);
    assert.strictEqual(mockCollection.bulkWriteCalls.length, 1);
    assert.strictEqual(service.syncManager.completeInitialSyncCalls.length, 1);
  });
});

test('SyncManager checkpoint batching unit tests', async (t) => {
  await t.test('should buffer checkpoints and flush at threshold or interval', async () => {
    const syncManager = new SyncManager('mongodb://localhost:27017/target');

    const updateOneCalls = [];
    syncManager.db = {
      collection() {
        return {
          async updateOne(filter, update, options) {
            updateOneCalls.push({ filter, update, options });
            return { acknowledged: true };
          }
        };
      }
    };
    syncManager.flushInterval = null;

    const operationTime = new Date('2026-08-23T12:00:00Z');
    const resumeToken = { _data: 'resume_token' };

    for (let i = 0; i < 499; i++) {
      syncManager.updateSyncState('orders', operationTime, resumeToken, 1);
    }
    assert.strictEqual(updateOneCalls.length, 0);

    syncManager.updateSyncState('orders', operationTime, resumeToken, 1);
    await new Promise((resolve) => setImmediate(resolve));

    assert.strictEqual(updateOneCalls.length, 1);
    assert.strictEqual(updateOneCalls[0].update.$inc.totalSynced, 500);

    syncManager.updateSyncState('orders', operationTime, resumeToken, 10);
    assert.strictEqual(updateOneCalls.length, 1);

    await syncManager.flushAllCheckpoints();
    assert.strictEqual(updateOneCalls.length, 2);
    assert.strictEqual(updateOneCalls[1].update.$inc.totalSynced, 10);
  });

  await t.test('should batch database resume tokens until flush', async () => {
    const syncManager = new SyncManager('mongodb://localhost:27017/target');
    const updateOneCalls = [];
    syncManager.db = {
      collection() {
        return {
          async updateOne(filter, update, options) {
            updateOneCalls.push({ filter, update, options });
            return { acknowledged: true };
          }
        };
      }
    };

    syncManager.queueDbResumeToken({ _data: 'tok_1' });
    syncManager.queueDbResumeToken({ _data: 'tok_2' });
    assert.strictEqual(updateOneCalls.length, 0);
    assert.deepStrictEqual(await syncManager.getDbResumeToken(), { _data: 'tok_2' });

    await syncManager.flushAllCheckpoints();
    assert.strictEqual(updateOneCalls.length, 1);
    assert.deepStrictEqual(updateOneCalls[0].filter, { collection: '__db_stream__' });
    assert.deepStrictEqual(updateOneCalls[0].update.$set.resumeToken, { _data: 'tok_2' });
  });

  await t.test('sanitizeIndexOptions strips driver metadata', () => {
    const { key, options } = SyncManager.sanitizeIndexOptions({
      v: 2,
      key: { email: 1 },
      name: 'email_1',
      unique: true,
      ns: 'db.users',
      sparse: true
    });
    assert.deepStrictEqual(key, { email: 1 });
    assert.deepStrictEqual(options, { name: 'email_1', unique: true, sparse: true });
  });
});

test('OplogSyncService - M5, M7, M10 and recovery unit tests', async (t) => {
  const createService = () => {
    const service = new OplogSyncService();

    service.syncManager = {
      savedTokens: [],
      queuedTokens: [],
      queueDbResumeToken(token) {
        this.queuedTokens.push(token);
      },
      async saveDbResumeToken(token) {
        this.savedTokens.push(token);
      },
      async getDbResumeToken() {
        return this.savedTokens[this.savedTokens.length - 1] || null;
      },
      async getSyncState() {
        return { initialSyncCompleted: true };
      },
      async getMetrics() {
        return [{ collection: 'users', totalSynced: 10 }];
      },
      async resetSyncStateForReSync() {}
    };

    return service;
  };

  await t.test('M5: processDbChange should route insert event and queue db resume token', async () => {
    const service = createService();
    service.syncedCollections.add('orders');
    let processChangeCalled = false;
    let targetCollectionPassed = null;

    service.processChange = async (colName) => {
      processChangeCalled = true;
      targetCollectionPassed = colName;
    };

    const mockLocalDb = {
      collection(name) {
        return { name };
      }
    };

    const changeEvent = {
      _id: { _data: 'db_resume_token_123' },
      operationType: 'insert',
      ns: { db: 'source', coll: 'orders' },
      documentKey: { _id: 'doc_123' },
      fullDocument: { _id: 'doc_123', item: 'Laptop' }
    };

    await service.processDbChange(changeEvent, mockLocalDb);

    assert.strictEqual(processChangeCalled, true);
    assert.strictEqual(targetCollectionPassed, 'orders');
    assert.strictEqual(service.syncedCollections.has('orders'), true);
    assert.strictEqual(service.syncManager.queuedTokens.length, 1);
    assert.deepStrictEqual(service.syncManager.queuedTokens[0], changeEvent._id);
  });

  await t.test('processDbChange should not apply events while initial sync is in progress', async () => {
    const service = createService();
    let processChangeCalled = false;
    service.initialSyncInProgress.add('orders');
    service.processChange = async () => {
      processChangeCalled = true;
    };

    await service.processDbChange(
      {
        _id: { _data: 't' },
        operationType: 'insert',
        ns: { db: 'source', coll: 'orders' },
        documentKey: { _id: '1' },
        fullDocument: { _id: '1' }
      },
      { collection: () => ({}) }
    );

    assert.strictEqual(processChangeCalled, false);
    assert.strictEqual(service.syncManager.queuedTokens.length, 0);
  });

  await t.test('M7: runDivergenceCheck should calculate difference between remote and local', async () => {
    const service = createService();

    service.atlasClient = {
      db() {
        return {
          listCollections() {
            return { toArray: async () => [{ name: 'products' }] };
          },
          collection() {
            return {
              async countDocuments() {
                return 150;
              }
            };
          }
        };
      }
    };

    service.localClient = {
      db() {
        return {
          collection() {
            return {
              async countDocuments() {
                return 145;
              }
            };
          }
        };
      }
    };

    await service.runDivergenceCheck();

    assert.strictEqual(service.divergences.get('products'), 5);

    const health = await service.getHealth();
    assert.deepStrictEqual(health.divergences, { products: 5 });
  });

  await t.test('M10: syncAllIndexes should trigger indexes replication', async () => {
    const service = createService();
    let syncIndexesCollection = null;

    service.syncIndexes = async (name) => {
      syncIndexesCollection = name;
    };

    service.atlasClient = {
      db() {
        return {
          listCollections() {
            return { toArray: async () => [{ name: 'payments' }] };
          },
          collection(name) {
            return { name };
          }
        };
      }
    };

    service.localClient = {
      db() {
        return {
          collection(name) {
            return { name };
          }
        };
      }
    };

    await service.syncAllIndexes();
    assert.strictEqual(syncIndexesCollection, 'payments');
  });

  await t.test('performFullReSync should reset, drop, re-copy, and restart stream', async () => {
    const service = createService();
    service.isRunning = true;

    const dropped = [];
    const synced = [];
    let streamStarted = 0;

    service.atlasClient = {
      db() {
        return {
          listCollections() {
            return { toArray: async () => [{ name: 'users' }, { name: '_sync_metadata' }] };
          },
          watch() {
            return {
              resumeToken: { _data: 'fresh_token' },
              async tryNext() {},
              async close() {}
            };
          },
          collection(name) {
            return { name };
          }
        };
      }
    };

    service.localClient = {
      db() {
        return {
          collection(name) {
            return {
              async drop() {
                dropped.push(name);
              }
            };
          }
        };
      }
    };

    service.syncManager.resetSyncStateForReSync = async (name) => {
      synced.push(`reset:${name}`);
    };
    service.ensureCollectionInitialSync = async (name) => {
      synced.push(`sync:${name}`);
    };
    service.startDatabaseStream = async () => {
      streamStarted += 1;
    };

    await service.performFullReSync();

    assert.deepStrictEqual(dropped, ['users']);
    assert.ok(synced.includes('reset:users'));
    assert.ok(synced.includes('sync:users'));
    assert.strictEqual(streamStarted, 1);
    assert.strictEqual(service.syncedCollections.has('users'), true);
    assert.ok(service.syncManager.savedTokens.includes(null));
    assert.ok(service.syncManager.savedTokens.some((t) => t && t._data === 'fresh_token'));
  });

  await t.test('withStreamPaused should close stream, run work, and restart', async () => {
    const service = createService();
    service.isRunning = true;
    let closed = false;
    let started = 0;
    let workRan = false;

    service.dbChangeStream = {
      resumeToken: { _data: 'pause_tok' },
      async close() {
        closed = true;
      }
    };
    service.startDatabaseStream = async () => {
      started += 1;
    };

    await service.withStreamPaused(async () => {
      workRan = true;
      assert.strictEqual(service.dbChangeStream, null);
    });

    assert.strictEqual(closed, true);
    assert.strictEqual(workRan, true);
    assert.strictEqual(started, 1);
    assert.deepStrictEqual(service.syncManager.savedTokens[service.syncManager.savedTokens.length - 1], {
      _data: 'pause_tok'
    });
  });
});
