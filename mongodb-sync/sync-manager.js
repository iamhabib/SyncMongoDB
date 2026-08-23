const { MongoClient } = require('mongodb');
const logger = require('./logger');

class SyncManager {
  constructor(localMongoUrl) {
    this.client = new MongoClient(localMongoUrl);
    this.db = null;
    this.pendingCheckpoints = new Map();
    this.flushInterval = null;
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

      this.flushInterval = setInterval(() => this.flushAllCheckpoints(), 2000);
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
        resumeToken: state?.resumeToken || null,
        initialSyncCompleted: state?.initialSyncCompleted || false,
        lastCopiedId: state?.lastCopiedId || null
      };
    } catch (err) {
      logger.error('Error getting sync state', { collection: collectionName, error: err.message });
      return {
        collection: collectionName,
        lastTimestamp: new Date(0),
        lastOperationTime: null,
        resumeToken: null,
        totalSynced: 0,
        initialSyncCompleted: false,
        lastCopiedId: null
      };
    }
  }

  async updateInitialSyncProgress(collectionName, lastCopiedId, resumeToken, count) {
    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $set: {
            collection: collectionName,
            lastTimestamp: new Date(),
            syncedAt: new Date(),
            resumeToken: resumeToken,
            lastCopiedId: lastCopiedId,
            initialSyncCompleted: false
          },
          $inc: { totalSynced: count }
        },
        { upsert: true }
      );
    } catch (err) {
      logger.error('Error updating initial sync progress', {
        collection: collectionName,
        error: err.message
      });
    }
  }

  async completeInitialSync(collectionName) {
    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $set: {
            initialSyncCompleted: true,
            syncedAt: new Date()
          },
          $unset: { lastCopiedId: "" }
        }
      );
      logger.info(`[${collectionName}] Initial sync completed and marked in metadata`);
    } catch (err) {
      logger.error('Error completing initial sync', {
        collection: collectionName,
        error: err.message
      });
    }
  }

  async updateSyncState(collectionName, operationTime, resumeToken = null, count = 1) {
    let pending = this.pendingCheckpoints.get(collectionName);
    if (!pending) {
      pending = {
        collection: collectionName,
        lastOperationTime: operationTime,
        resumeToken: resumeToken,
        count: 0
      };
      this.pendingCheckpoints.set(collectionName, pending);
    }

    pending.lastOperationTime = operationTime;
    if (resumeToken) {
      pending.resumeToken = resumeToken;
    }
    pending.count += count;

    if (pending.count >= 500) {
      // Flush asynchronously so we do not block the hot path
      this.flushCheckpoint(collectionName).catch(err => {
        logger.error(`Failed to flush checkpoint for ${collectionName}`, { error: err.message });
      });
    }
  }

  async flushCheckpoint(collectionName) {
    const pending = this.pendingCheckpoints.get(collectionName);
    if (!pending || pending.count === 0) return;

    const countToFlush = pending.count;
    pending.count = 0;

    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $set: {
            collection: collectionName,
            lastTimestamp: new Date(),
            lastOperationTime: pending.lastOperationTime,
            syncedAt: new Date(),
            resumeToken: pending.resumeToken
          },
          $inc: { totalSynced: countToFlush }
        },
        { upsert: true }
      );
    } catch (err) {
      logger.error('Error flushing sync state checkpoint', {
        collection: collectionName,
        error: err.message
      });
      // Restore count for retry
      pending.count += countToFlush;
    }
  }

  async flushAllCheckpoints() {
    const promises = [];
    for (const collectionName of this.pendingCheckpoints.keys()) {
      promises.push(this.flushCheckpoint(collectionName));
    }
    await Promise.all(promises);
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

  async resetSyncStateForReSync(collectionName) {
    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $set: {
            initialSyncCompleted: false
          },
          $unset: {
            resumeToken: "",
            lastCopiedId: ""
          }
        }
      );
      logger.info(`[${collectionName}] Reset sync state to trigger re-sync due to stream error / token expiration`);
    } catch (err) {
      logger.error('Error resetting sync state for re-sync', {
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
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    await this.flushAllCheckpoints();
    await this.client.close();
  }
}

module.exports = SyncManager;
