const test = require('node:test');
const assert = require('node:assert');

// Set mock environment variables before importing
process.env.REMOTE_MONGODB_URL = 'mongodb://localhost:27017/source';
process.env.LOCAL_MONGO_URL = 'mongodb://localhost:27017/target';

const OplogSyncService = require('../oplog-sync-service');

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
});
