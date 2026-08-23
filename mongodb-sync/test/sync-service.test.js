const test = require('node:test');
const assert = require('node:assert');

// Set mock environment variables before importing
process.env.REMOTE_MONGODB_URL = 'mongodb://localhost:27017/source';
process.env.LOCAL_MONGO_URL = 'mongodb://localhost:27017/target';

const OplogSyncService = require('../oplog-sync-service');
const SyncManager = require('../sync-manager');

test('OplogSyncService.processChange() unit tests', async (t) => {
  // Helper to create a service instance with mocked dependencies
  const createMockService = () => {
    const service = new OplogSyncService();
    
    // Stub syncManager
    service.syncManager = {
      updateSyncStateCalls: [],
      async updateSyncState(collectionName, operationTime, resumeToken, count) {
        this.updateSyncStateCalls.push({ collectionName, operationTime, resumeToken, count });
      }
    };
    
    // Mock localCollection
    const mockCollection = {
      replaceOneCalls: [],
      deleteOneCalls: [],
      async replaceOne(query, doc, options) {
        this.replaceOneCalls.push({ query, doc, options });
        return { acknowledged: true };
      },
      async deleteOne(query) {
        this.deleteOneCalls.push({ query });
        return { acknowledged: true };
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

    // Verify local collection calls
    assert.strictEqual(mockCollection.replaceOneCalls.length, 1);
    assert.deepStrictEqual(mockCollection.replaceOneCalls[0], {
      query: { _id: 'doc_abc' },
      doc: { _id: 'doc_abc', name: 'Alice', age: 30 },
      options: { upsert: true }
    });
    assert.strictEqual(mockCollection.deleteOneCalls.length, 0);

    // Verify sync manager state update
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
    assert.strictEqual(mockCollection.deleteOneCalls.length, 0);
  });

  await t.test('should fallback to deleteOne on UPDATE/REPLACE event if fullDocument is missing', async () => {
    const { service, mockCollection } = createMockService();
    const changeEvent = {
      _id: { _data: 'resume_token_789' },
      operationType: 'update',
      documentKey: { _id: 'doc_abc' },
      fullDocument: null, // document was deleted before updateLookup could run
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

    assert.strictEqual(mockCollection.replaceOneCalls.length, 0);
    assert.strictEqual(mockCollection.deleteOneCalls.length, 1);
    assert.deepStrictEqual(mockCollection.deleteOneCalls[0], {
      query: { _id: 'doc_abc' }
    });
  });

  await t.test('should verify idempotency (repeating same INSERT/UPDATE/DELETE events)', async () => {
    const { service, mockCollection } = createMockService();
    const insertEvent = {
      _id: { _data: 'resume_token_1' },
      operationType: 'insert',
      documentKey: { _id: 'doc_1' },
      fullDocument: { _id: 'doc_1', data: 'hello' }
    };

    // Process event twice (e.g., resume replay)
    await service.processChange('items', insertEvent, mockCollection);
    await service.processChange('items', insertEvent, mockCollection);

    // Assert it executes the same database write operation twice without issues/throwing
    assert.strictEqual(mockCollection.replaceOneCalls.length, 2);
    assert.strictEqual(service.syncManager.updateSyncStateCalls.length, 2);
    
    // Also verify delete idempotency
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
    
    // Set running state
    service.isRunning = true;

    // Stub syncManager initial sync methods
    service.syncManager.updateInitialSyncProgressCalls = [];
    service.syncManager.completeInitialSyncCalls = [];
    
    service.syncManager.updateInitialSyncProgress = async (collectionName, lastCopiedId, resumeToken, count) => {
      service.syncManager.updateInitialSyncProgressCalls.push({ collectionName, lastCopiedId, resumeToken, count });
    };
    
    service.syncManager.completeInitialSync = async (collectionName) => {
      service.syncManager.completeInitialSyncCalls.push({ collectionName });
    };

    // Mock remote collection with search/find capability returning cursor
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
          { name: '_id_', key: { _id: 1 } },
          { name: 'name_1', key: { name: 1 }, unique: true }
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
                    // Return mock documents if it's the first query, else empty array to break the loop
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

    // Assert indexes were synchronized (skipping _id_)
    assert.strictEqual(mockCollection.createIndexCalls.length, 1);
    assert.deepStrictEqual(mockCollection.createIndexCalls[0], {
      key: { name: 1 },
      options: { name: 'name_1', unique: true }
    });

    // Assert watch called to get resume token
    assert.strictEqual(mockRemoteCollection.watchCallsCount, 1);
    
    // Assert bulkWrite was called once for the batch
    assert.strictEqual(mockCollection.bulkWriteCalls.length, 1);
    assert.deepStrictEqual(mockCollection.bulkWriteCalls[0].operations, [
      { replaceOne: { filter: { _id: 'doc_1' }, replacement: { _id: 'doc_1', name: 'Doc 1' }, upsert: true } },
      { replaceOne: { filter: { _id: 'doc_2' }, replacement: { _id: 'doc_2', name: 'Doc 2' }, upsert: true } }
    ]);

    // Assert progress updated
    assert.strictEqual(service.syncManager.updateInitialSyncProgressCalls.length, 1);
    assert.deepStrictEqual(service.syncManager.updateInitialSyncProgressCalls[0], {
      collectionName: 'users',
      lastCopiedId: 'doc_2',
      resumeToken: { _data: 'initial_resume_token' },
      count: 2
    });

    // Assert initial sync completed
    assert.strictEqual(service.syncManager.completeInitialSyncCalls.length, 1);
    assert.deepStrictEqual(service.syncManager.completeInitialSyncCalls[0], {
      collectionName: 'users'
    });
  });
});

test('SyncManager checkpoint batching unit tests', async (t) => {
  await t.test('should buffer checkpoints and flush at threshold or interval', async () => {
    const syncManager = new SyncManager('mongodb://localhost:27017/target');
    
    // Mock db and collection
    const updateOneCalls = [];
    syncManager.db = {
      collection(name) {
        return {
          async updateOne(filter, update, options) {
            updateOneCalls.push({ filter, update, options });
            return { acknowledged: true };
          }
        };
      }
    };

    // Initialize with mock interval (skip full initialize to avoid connection)
    syncManager.flushInterval = null;

    const operationTime = new Date('2026-08-23T12:00:00Z');
    const resumeToken = { _data: 'resume_token' };

    // 1. Update state 499 times - should NOT write to DB
    for (let i = 0; i < 499; i++) {
      syncManager.updateSyncState('orders', operationTime, resumeToken, 1);
    }
    assert.strictEqual(updateOneCalls.length, 0);

    // 2. 500th update - should trigger flush
    syncManager.updateSyncState('orders', operationTime, resumeToken, 1);
    
    // Wait a brief tick for async flush to execute
    await new Promise(resolve => setImmediate(resolve));
    
    assert.strictEqual(updateOneCalls.length, 1);
    assert.deepStrictEqual(updateOneCalls[0].filter, { collection: 'orders' });
    assert.strictEqual(updateOneCalls[0].update.$inc.totalSynced, 500);

    // 3. Update state 10 times - should NOT write to DB yet
    syncManager.updateSyncState('orders', operationTime, resumeToken, 10);
    assert.strictEqual(updateOneCalls.length, 1);

    // 4. Call flushAllCheckpoints manually - should write the remaining 10
    await syncManager.flushAllCheckpoints();
    assert.strictEqual(updateOneCalls.length, 2);
    assert.strictEqual(updateOneCalls[1].update.$inc.totalSynced, 10);
  });
});
