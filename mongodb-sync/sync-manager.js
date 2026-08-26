const { MongoClient } = require('mongodb');
const logger = require('./logger');

/** Index option keys that createIndex accepts (strip driver metadata from indexes()). */
const CREATE_INDEX_OPTION_KEYS = new Set([
  'unique', 'sparse', 'background', 'name', 'expireAfterSeconds', 'partialFilterExpression',
  'collation', 'wildcardProjection', 'hidden', 'storageEngine', 'weights', 'default_language',
  'language_override', 'textIndexVersion', '2dsphereIndexVersion', 'bits', 'min', 'max',
  'bucketSize', 'dropDups'
]);

class SyncManager {
  /**
   * @param {string|import('mongodb').MongoClient} localMongoUrlOrClient
   *        Pass an existing MongoClient to share the connection pool, or a URL string.
   */
  constructor(localMongoUrlOrClient) {
    if (typeof localMongoUrlOrClient === 'string') {
      this.client = new MongoClient(localMongoUrlOrClient);
      this._ownsClient = true;
    } else {
      this.client = localMongoUrlOrClient;
      this._ownsClient = false;
    }
    this.db = null;
    this.pendingCheckpoints = new Map();
    this.pendingDbResumeToken = null;
    this.pendingDbResumeTokenDirty = false;
    this.flushInterval = null;
  }

  async initialize() {
    try {
      if (this._ownsClient) {
        await this.client.connect();
      }
      this.db = this.client.db();

      const collections = await this.db.listCollections({ name: '_sync_metadata' }).toArray();
      if (collections.length === 0) {
        await this.db.createCollection('_sync_metadata');
        await this.db.collection('_sync_metadata').createIndex({ collection: 1 }, { unique: true });
        logger.info('Created _sync_metadata collection');
      }

      this.flushInterval = setInterval(() => {
        this.flushAllCheckpoints().catch((err) => {
          logger.error('Periodic checkpoint flush failed', { error: err.message });
        });
      }, 2000);
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
            collection: collectionName,
            initialSyncCompleted: true,
            syncedAt: new Date()
          },
          $unset: { lastCopiedId: '' }
        },
        { upsert: true }
      );
      logger.info(`[${collectionName}] Initial sync completed and marked in metadata`);
    } catch (err) {
      logger.error('Error completing initial sync', {
        collection: collectionName,
        error: err.message
      });
      throw err;
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
      this.flushCheckpoint(collectionName).catch((err) => {
        logger.error(`Failed to flush checkpoint for ${collectionName}`, { error: err.message });
      });
    }
  }

  /**
   * Buffer the database-level resume token; flushed with collection checkpoints
   * so the hot path does not await a write on every event.
   */
  queueDbResumeToken(resumeToken) {
    if (!resumeToken) return;
    this.pendingDbResumeToken = resumeToken;
    this.pendingDbResumeTokenDirty = true;
  }

  async flushDbResumeToken() {
    if (!this.pendingDbResumeTokenDirty) return;
    const token = this.pendingDbResumeToken;
    this.pendingDbResumeTokenDirty = false;
    try {
      await this._persistDbResumeToken(token);
    } catch (err) {
      this.pendingDbResumeTokenDirty = true;
      logger.error('Error flushing db resume token', { error: err.message });
    }
  }

  async _persistDbResumeToken(resumeToken) {
    await this.db.collection('_sync_metadata').updateOne(
      { collection: '__db_stream__' },
      {
        $set: {
          collection: '__db_stream__',
          resumeToken: resumeToken,
          lastTimestamp: new Date()
        }
      },
      { upsert: true }
    );
  }

  async flushCheckpoint(collectionName) {
    const pending = this.pendingCheckpoints.get(collectionName);
    if (!pending || pending.count === 0) return;

    const countToFlush = pending.count;
    const lastOperationTime = pending.lastOperationTime;
    const resumeToken = pending.resumeToken;
    pending.count = 0;

    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        {
          $set: {
            collection: collectionName,
            lastTimestamp: new Date(),
            lastOperationTime,
            syncedAt: new Date(),
            resumeToken
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
      pending.count += countToFlush;
    }
  }

  async flushAllCheckpoints() {
    const promises = [];
    for (const collectionName of this.pendingCheckpoints.keys()) {
      promises.push(this.flushCheckpoint(collectionName));
    }
    promises.push(this.flushDbResumeToken());
    await Promise.all(promises);
  }

  async clearResumeToken(collectionName) {
    try {
      await this.db.collection('_sync_metadata').updateOne(
        { collection: collectionName },
        { $unset: { resumeToken: '' } }
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
          $set: { initialSyncCompleted: false },
          $unset: { resumeToken: '', lastCopiedId: '' }
        },
        { upsert: true }
      );
      logger.info(`[${collectionName}] Reset sync state to trigger re-sync due to stream error / token expiration`);
    } catch (err) {
      logger.error('Error resetting sync state for re-sync', {
        collection: collectionName,
        error: err.message
      });
      throw err;
    }
  }

  async getDbResumeToken() {
    try {
      // Prefer in-memory pending token so we do not lose position between flushes
      if (this.pendingDbResumeTokenDirty && this.pendingDbResumeToken) {
        return this.pendingDbResumeToken;
      }
      const doc = await this.db.collection('_sync_metadata').findOne({ collection: '__db_stream__' });
      return doc ? doc.resumeToken : null;
    } catch (err) {
      logger.error('Error getting db resume token', { error: err.message });
      return null;
    }
  }

  async saveDbResumeToken(resumeToken) {
    this.pendingDbResumeToken = resumeToken;
    this.pendingDbResumeTokenDirty = false;
    try {
      await this._persistDbResumeToken(resumeToken);
    } catch (err) {
      this.pendingDbResumeTokenDirty = true;
      logger.error('Error saving db resume token', { error: err.message });
    }
  }

  async getMetrics() {
    try {
      return await this.db
        .collection('_sync_metadata')
        .find({ collection: { $ne: '__db_stream__' } })
        .toArray();
    } catch (err) {
      logger.error('Error getting metrics', { error: err.message });
      return [];
    }
  }

  /**
   * Strip non-createIndex fields from a remote index spec.
   */
  static sanitizeIndexOptions(index) {
    const { key, name, ...rest } = index;
    const options = { name };
    for (const [k, v] of Object.entries(rest)) {
      if (CREATE_INDEX_OPTION_KEYS.has(k)) {
        options[k] = v;
      }
    }
    return { key, options };
  }

  async close() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    await this.flushAllCheckpoints();
    if (this._ownsClient) {
      await this.client.close();
    }
  }
}

module.exports = SyncManager;
