const { MongoClient } = require('mongodb');
require('dotenv').config();
const logger = require('./logger');
const SyncManager = require('./sync-manager');

const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '10', 10);
const RETRY_DELAY_MS = parseInt(process.env.RETRY_DELAY_MS || '5000', 10);

function interpolateUrl(url) {
  if (!url) return url;
  return url
    .replace(/{LOCAL_MONGO_PORT}/g, process.env.LOCAL_MONGO_PORT || '27017')
    .replace(/{MONGO_DATABASE_NAME}/g, process.env.MONGO_DATABASE_NAME || 'sync_db');
}

class OplogSyncService {
  constructor() {
    if (!process.env.REMOTE_MONGODB_URL) {
      throw new Error('REMOTE_MONGODB_URL is required but not defined in environmental variables');
    }
    if (!process.env.LOCAL_MONGO_URL) {
      throw new Error('LOCAL_MONGO_URL is required but not defined in environmental variables');
    }

    const remoteUrl = interpolateUrl(process.env.REMOTE_MONGODB_URL);
    const localUrl = interpolateUrl(process.env.LOCAL_MONGO_URL);

    this.atlasClient = new MongoClient(remoteUrl);
    this.localClient = new MongoClient(localUrl);
    this.syncManager = new SyncManager(localUrl);
    this.changeStreams = new Map();
    this.retryCount = new Map();
    this.isRunning = false;
    this.expectedCollectionsCount = 0;
  }

  async initialize() {
    try {
      logger.info('Initializing Oplog Sync Service...');

      await this.atlasClient.connect();
      await this.localClient.connect();
      await this.syncManager.initialize();

      const atlasDb = this.atlasClient.db();
      const collections = await atlasDb.listCollections().toArray();

      const collectionNames = collections
        .map(c => c.name)
        .filter(name => !name.startsWith('_') && name !== 'system.profile');

      this.expectedCollectionsCount = collectionNames.length;

      logger.info('Connected to Atlas & Local MongoDB', {
        collectionsCount: this.expectedCollectionsCount
      });

      this.isRunning = true;
      await this.startAllChangeStreams(collectionNames);

      logger.info('Oplog Sync Service is running');
    } catch (err) {
      logger.error('Initialization failed', { error: err.message, stack: err.stack });
      await this.shutdown();
      process.exit(1);
    }
  }

  async startAllChangeStreams(collectionNames) {
    for (const collectionName of collectionNames) {
      await this.startChangeStream(collectionName);
      // Stagger startup so we don't open too many streams at once
      await new Promise(r => setTimeout(r, 100));
    }
  }

  async startChangeStream(collectionName) {
    // Close old stream if it exists
    const oldStream = this.changeStreams.get(collectionName);
    if (oldStream) {
      try {
        await oldStream.close();
        logger.info(`[${collectionName}] Closed existing change stream before restart`);
      } catch (err) {
        logger.warn(`[${collectionName}] Error closing existing stream: ${err.message}`);
      }
      this.changeStreams.delete(collectionName);
    }

    try {
      const atlasDb = this.atlasClient.db();
      const localDb = this.localClient.db();

      const collection = atlasDb.collection(collectionName);
      const localCollection = localDb.collection(collectionName);

      const syncState = await this.syncManager.getSyncState(collectionName);

      logger.info(`[${collectionName}] Starting Change Stream`, {
        lastSyncTime: syncState.lastTimestamp,
        totalSynced: syncState.totalSynced
      });

      const pipeline = [
        {
          $match: {
            operationType: { $in: ['insert', 'update', 'replace', 'delete'] }
          }
        }
      ];

      const options = {
        fullDocument: 'updateLookup',
        maxAwaitTimeMS: 10000
      };
      // Only pass resumeAfter when we actually have a token, otherwise the
      // driver throws on `resumeAfter: undefined` in some versions.
      if (syncState.resumeToken) {
        options.resumeAfter = syncState.resumeToken;
      }

      const changeStream = collection.watch(pipeline, options);
      this.changeStreams.set(collectionName, changeStream);

      // Start asynchronous consumer loop
      (async () => {
        try {
          for await (const change of changeStream) {
            if (!this.isRunning) break;
            await this.processChange(collectionName, change, localCollection);
          }
        } catch (err) {
          // If the loop terminated because of shutdown/close, ignore
          if (!this.isRunning) return;
          logger.error(`[${collectionName}] Change Stream cursor error`, { error: err.message });
          await this.handleStreamError(collectionName, err);
        }
      })();

    } catch (err) {
      logger.error(`[${collectionName}] Failed to start change stream`, {
        error: err.message,
        stack: err.stack
      });
      await this.handleStreamError(collectionName, err);
    }
  }

  async handleStreamError(collectionName, error) {
    // Close old stream if it exists
    const oldStream = this.changeStreams.get(collectionName);
    if (oldStream) {
      try {
        await oldStream.close();
      } catch (err) {
        // Ignore close error
      }
      this.changeStreams.delete(collectionName);
    }

    // Check if error is due to resume token expired / invalid
    const isResumeError =
      error.code === 280 ||
      error.code === 286 ||
      (error.message && (
        error.message.includes('resume of change stream was not possible') ||
        error.message.includes('resume point may no longer be in the oplog') ||
        error.message.includes('resumeAfter')
      ));

    if (isResumeError) {
      logger.warn(`[${collectionName}] Resume token invalid or expired. Clearing token and restarting from present.`, {
        errorCode: error.code,
        errorMessage: error.message
      });
      await this.syncManager.clearResumeToken(collectionName);
      // Immediately retry without incrementing retry count (as we resolved the issue by clearing the token)
      setTimeout(() => this.startChangeStream(collectionName), 100);
      return;
    }

    // Normal retry logic
    const retries = this.retryCount.get(collectionName) || 0;
    if (retries < MAX_RETRIES) {
      logger.info(`[${collectionName}] Retrying in ${RETRY_DELAY_MS}ms (${retries + 1}/${MAX_RETRIES})`);
      this.retryCount.set(collectionName, retries + 1);
      setTimeout(() => this.startChangeStream(collectionName), RETRY_DELAY_MS);
    } else {
      logger.error(`[${collectionName}] Max retries exceeded`, { maxRetries: MAX_RETRIES });
    }
  }

  async processChange(collectionName, change, localCollection) {
    const docId = change.documentKey._id;
    const operationType = change.operationType;
    const operationTime = change.clusterTime || new Date();

    try {
      switch (operationType) {
        case 'insert':
          await localCollection.replaceOne(
            { _id: docId },
            change.fullDocument,
            { upsert: true }
          );
          logger.info(`[${collectionName}] INSERT`, { docId, timestamp: operationTime });
          break;

        case 'update':
        case 'replace':
          if (change.fullDocument) {
            await localCollection.replaceOne(
              { _id: docId },
              change.fullDocument,
              { upsert: true }
            );
          } else {
            // Document was deleted before we could look it up
            await localCollection.deleteOne({ _id: docId });
          }
          logger.info(`[${collectionName}] UPDATE`, { docId, timestamp: operationTime });
          break;

        case 'delete':
          await localCollection.deleteOne({ _id: docId });
          logger.info(`[${collectionName}] DELETE`, { docId, timestamp: operationTime });
          break;
      }

      await this.syncManager.updateSyncState(
        collectionName,
        operationTime,
        change._id, // resume token
        1
      );

      if (this.retryCount.has(collectionName)) {
        this.retryCount.delete(collectionName);
      }
    } catch (err) {
      logger.error(`[${collectionName}] Failed to apply change`, {
        error: err.message,
        docId,
        operationType
      });
      throw err;
    }
  }

  async getHealth() {
    const metrics = await this.syncManager.getMetrics();

    return {
      isRunning: this.isRunning,
      connectedCollections: this.changeStreams.size,
      expectedCollectionsCount: this.expectedCollectionsCount,
      syncMetrics: metrics,
      timestamp: new Date()
    };
  }

  async shutdown() {
    logger.info('Shutting down gracefully...');

    this.isRunning = false;

    for (const [collectionName, stream] of this.changeStreams) {
      try {
        await stream.close();
        logger.info(`Closed change stream for ${collectionName}`);
      } catch (err) {
        logger.error(`Error closing stream for ${collectionName}`, { error: err.message });
      }
    }

    try {
      await this.atlasClient.close();
      await this.localClient.close();
      await this.syncManager.close();
      logger.info('Shutdown complete');
    } catch (err) {
      logger.error('Error during shutdown', { error: err.message });
    }
  }
}

module.exports = OplogSyncService;
