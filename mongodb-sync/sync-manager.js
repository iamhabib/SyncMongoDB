const { MongoClient } = require('mongodb');
const logger = require('./logger');

class SyncManager {
  constructor(localMongoUrl) {
    this.client = new MongoClient(localMongoUrl);
    this.db = null;
  }

  async initialize() {
    try {
      await this.client.connect();
      this.db = this.client.db();
      const dbName = this.db.databaseName;

      const collections = await this.db.listCollections().toArray();
      const exists = collections.some(c => c.name === '_sync_metadata');

      if (!exists) {
        await this.db.createCollection('_sync_metadata');
        await this.db.collection('_sync_metadata').createIndex({ collection: 1 }, { unique: true });
        logger.info('Created _sync_metadata collection');
      }
    } catch (err) {
      logger.error('Failed to initialize sync manager', { error: err.message });
      throw err;
    }
  }

  async getSyncState(collectionName) {
    try {
      const state = await this.db
        .collection('_sync_metadata')
        .findOne({ collection: collectionName });

      return {
        collection: collectionName,
        lastTimestamp: state?.lastTimestamp || new Date(0),
        lastOperationTime: state?.lastOperationTime || null,
        syncedAt: state?.syncedAt || null,
        totalSynced: state?.totalSynced || 0,
        resumeToken: state?.resumeToken || null
      };
    } catch (err) {
      logger.error('Error getting sync state', { collection: collectionName, error: err.message });
      return {
        collection: collectionName,
        lastTimestamp: new Date(0),
        lastOperationTime: null,
        resumeToken: null,
        totalSynced: 0
      };
    }
  }

  async updateSyncState(collectionName, operationTime, resumeToken = null, count = 1) {
    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $set: {
            collection: collectionName,
            lastTimestamp: new Date(),
            lastOperationTime: operationTime,
            syncedAt: new Date(),
            resumeToken: resumeToken
          },
          $inc: { totalSynced: count }
        },
        { upsert: true }
      );
    } catch (err) {
      logger.error('Error updating sync state', {
        collection: collectionName,
        error: err.message
      });
    }
  }

  async clearResumeToken(collectionName) {
    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $unset: { resumeToken: "" }
        }
      );
    } catch (err) {
      logger.error('Error clearing resume token', {
        collection: collectionName,
        error: err.message
      });
    }
  }

  async getMetrics() {
    try {
      return await this.db.collection('_sync_metadata').find({}).toArray();
    } catch (err) {
      logger.error('Error getting metrics', { error: err.message });
      return [];
    }
  }

  async close() {
    await this.client.close();
  }
}

module.exports = SyncManager;
