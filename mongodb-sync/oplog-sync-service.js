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
    .replace(/{MONGO_DATABASE_NAME}/g, process.env.MONGO_DATABASE_NAME || 'sync_db')
    .replace(/{LOCAL_MONGO_ROOT_USER}/g, process.env.LOCAL_MONGO_ROOT_USER || 'admin')
    .replace(/{LOCAL_MONGO_ROOT_PASSWORD}/g, process.env.LOCAL_MONGO_ROOT_PASSWORD || '');
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
    this.discoveryInterval = null;
    this.failedStreams = new Set();
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

      // Periodically check for newly added collections every 60 seconds
      this.discoveryInterval = setInterval(() => this.discoverNewCollections(), 60000);

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

      let syncState = await this.syncManager.getSyncState(collectionName);

      if (!syncState.initialSyncCompleted) {
        await this.performInitialSync(collectionName, collection, localCollection, syncState);
        // Refresh sync state to get the saved resume token and updated completion status
        syncState = await this.syncManager.getSyncState(collectionName);
      }

      logger.info(`[${collectionName}] Starting Change Stream`, {
        lastSyncTime: syncState.lastTimestamp,
        totalSynced: syncState.totalSynced
      });

      const pipeline = [
        {
          $match: {
            operationType: { $in: ['insert', 'update', 'replace', 'delete', 'drop', 'rename', 'invalidate'] }
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
      logger.warn(`[${collectionName}] Resume token invalid or expired. Resetting sync state to trigger a full backfill/re-sync.`, {
        errorCode: error.code,
        errorMessage: error.message
      });
      await this.syncManager.resetSyncStateForReSync(collectionName);
      // Immediately retry to initiate the re-sync
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
      logger.error(`[${collectionName}] Max retries exceeded. Mark stream as permanently failed.`, { maxRetries: MAX_RETRIES });
      this.failedStreams.add(collectionName);
    }
  }

  async processChange(collectionName, change, localCollection) {
    const operationType = change.operationType;
    
    let operationTime;
    if (change.clusterTime) {
      if (typeof change.clusterTime.getHighOrder === 'function') {
        operationTime = new Date(change.clusterTime.getHighOrder() * 1000);
      } else {
        operationTime = new Date(change.clusterTime);
      }
    } else {
      operationTime = new Date();
    }

    try {
      switch (operationType) {
        case 'insert': {
          const docId = change.documentKey._id;
          await localCollection.replaceOne(
            { _id: docId },
            change.fullDocument,
            { upsert: true }
          );
          logger.debug(`[${collectionName}] INSERT`, { docId, timestamp: operationTime });
          break;
        }

        case 'update':
        case 'replace': {
          const docId = change.documentKey._id;
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
          logger.debug(`[${collectionName}] UPDATE`, { docId, timestamp: operationTime });
          break;
        }

        case 'delete': {
          const docId = change.documentKey._id;
          await localCollection.deleteOne({ _id: docId });
          logger.debug(`[${collectionName}] DELETE`, { docId, timestamp: operationTime });
          break;
        }

        case 'drop': {
          try {
            await localCollection.drop();
            logger.info(`[${collectionName}] DROPPED collection locally due to remote drop`);
          } catch (err) {
            if (err.codeName !== 'NamespaceNotFound') throw err;
          }
          break;
        }

        case 'rename': {
          const newName = change.to.split('.').pop();
          await localCollection.rename(newName);
          logger.info(`[${collectionName}] RENAMED collection locally to ${newName} due to remote rename`);
          break;
        }

        case 'invalidate': {
          logger.warn(`[${collectionName}] Stream invalidated`);
          throw new Error('Stream invalidated');
        }
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

  async syncIndexes(collectionName, remoteCollection, localCollection) {
    try {
      logger.info(`[${collectionName}] Syncing indexes...`);
      const remoteIndexes = await remoteCollection.indexes();

      for (const index of remoteIndexes) {
        if (index.name === '_id_') {
          continue; // Default primary index is auto-created
        }

        const { key, name, ...options } = index;
        logger.info(`[${collectionName}] Replicating index: ${name}`, { key });
        await localCollection.createIndex(key, { name, ...options });
      }
      logger.info(`[${collectionName}] Indexes synced successfully.`);
    } catch (err) {
      logger.warn(`[${collectionName}] Failed to sync indexes. Syncing documents will proceed.`, { error: err.message });
    }
  }

  async performInitialSync(collectionName, remoteCollection, localCollection, syncState) {
    logger.info(`[${collectionName}] Starting Initial Sync...`);

    // Sync indexes before copying data
    await this.syncIndexes(collectionName, remoteCollection, localCollection);

    let resumeToken = syncState.resumeToken;
    
    // 1. Get a resume token to mark our starting position if not already present
    if (!resumeToken) {
      logger.info(`[${collectionName}] Retrieving starting resume token...`);
      const tempStream = remoteCollection.watch([], { maxAwaitTimeMS: 1000 });
      try {
        await tempStream.tryNext();
      } catch (err) {
        // ignore errors during tryNext (e.g. timeout)
      }
      resumeToken = tempStream.resumeToken;
      await tempStream.close();

      if (!resumeToken) {
        logger.warn(`[${collectionName}] Could not obtain starting resume token from watch. Change stream will resume from default context.`);
      }
    }

    // 2. Start copying documents in batches
    let lastCopiedId = syncState.lastCopiedId;
    const batchSize = 1000;
    let totalSyncedInThisSession = 0;

    logger.info(`[${collectionName}] Copying documents in batches of ${batchSize}...`, { lastCopiedId });

    while (true) {
      if (!this.isRunning) {
        throw new Error('Service shutdown during initial sync');
      }

      const query = lastCopiedId ? { _id: { $gt: lastCopiedId } } : {};
      const batch = await remoteCollection
        .find(query)
        .sort({ _id: 1 })
        .limit(batchSize)
        .toArray();

      if (batch.length === 0) {
        break;
      }

      // Write batch using bulkWrite with replaceOne upserts to be idempotent and efficient
      const operations = batch.map(doc => ({
        replaceOne: {
          filter: { _id: doc._id },
          replacement: doc,
          upsert: true
        }
      }));

      await localCollection.bulkWrite(operations, { ordered: false });

      lastCopiedId = batch[batch.length - 1]._id;
      totalSyncedInThisSession += batch.length;

      // Update sync manager with progress
      await this.syncManager.updateInitialSyncProgress(
        collectionName,
        lastCopiedId,
        resumeToken,
        batch.length
      );

      logger.info(`[${collectionName}] Copied ${totalSyncedInThisSession} documents (last _id: ${lastCopiedId})`);
    }

    // 3. Mark initial sync complete
    await this.syncManager.completeInitialSync(collectionName);
    logger.info(`[${collectionName}] Initial Sync completed successfully.`);
  }

  async discoverNewCollections() {
    if (!this.isRunning) return;
    try {
      const atlasDb = this.atlasClient.db();
      const collections = await atlasDb.listCollections().toArray();
      const collectionNames = collections
        .map(c => c.name)
        .filter(name => !name.startsWith('_') && name !== 'system.profile');

      for (const name of collectionNames) {
        if (!this.changeStreams.has(name)) {
          logger.info(`[${name}] Dynamic collection discovery detected new collection. Starting sync...`);
          // Start the change stream (which handles initial sync and streams) asynchronously
          this.startChangeStream(name).catch(err => {
            logger.error(`[${name}] Failed to start dynamically discovered stream`, { error: err.message });
          });
        }
      }
      this.expectedCollectionsCount = collectionNames.length;
    } catch (err) {
      logger.warn('Failed to dynamically check for new collections', { error: err.message });
    }
  }

  async getHealth() {
    let isAtlasConnected = false;
    let isLocalConnected = false;
    try {
      await this.atlasClient.db().admin().ping();
      isAtlasConnected = true;
    } catch (err) {
      logger.warn('Atlas health check ping failed', { error: err.message });
    }

    try {
      await this.localClient.db().admin().ping();
      isLocalConnected = true;
    } catch (err) {
      logger.warn('Local database health check ping failed', { error: err.message });
    }

    const metrics = await this.syncManager.getMetrics().catch(() => []);

    return {
      isRunning: this.isRunning && isAtlasConnected && isLocalConnected,
      connectedCollections: this.changeStreams.size,
      expectedCollectionsCount: this.expectedCollectionsCount,
      failedStreams: Array.from(this.failedStreams),
      isAtlasConnected,
      isLocalConnected,
      syncMetrics: metrics,
      timestamp: new Date()
    };
  }

  async shutdown() {
    logger.info('Shutting down gracefully...');

    this.isRunning = false;

    if (this.discoveryInterval) {
      clearInterval(this.discoveryInterval);
      this.discoveryInterval = null;
    }

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
