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

const DIVERGENCE_CHECK_INTERVAL_MS = parseInt(process.env.DIVERGENCE_CHECK_INTERVAL_MS || '21600000', 10);

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
    this.dbChangeStream = null;
    this.syncedCollections = new Set();
    this.retryCount = new Map();
    this.isRunning = false;
    this.expectedCollectionsCount = 0;
    this.discoveryInterval = null;
    this.reconciliationInterval = null;
    this.failedStreams = new Set();
    this.divergences = new Map();
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

      // 1. Check if we need to obtain a starting database-level resume token before initial syncs
      let resumeToken = await this.syncManager.getDbResumeToken();
      if (!resumeToken) {
        logger.info('No database-level resume token found. Fetching current starting resume token...');
        const tempStream = atlasDb.watch([], { maxAwaitTimeMS: 1000 });
        try {
          await tempStream.tryNext();
        } catch (err) {
          // ignore timeout / empty oplog error
        }
        resumeToken = tempStream.resumeToken;
        await tempStream.close();
        if (resumeToken) {
          await this.syncManager.saveDbResumeToken(resumeToken);
          logger.info('Saved initial database-level resume token');
        }
      }

      // 2. Perform initial sync for all collections that need it
      const localDb = this.localClient.db();
      for (const name of collectionNames) {
        const remoteCollection = atlasDb.collection(name);
        const localCollection = localDb.collection(name);

        let syncState = await this.syncManager.getSyncState(name);
        if (!syncState.initialSyncCompleted) {
          await this.performInitialSync(name, remoteCollection, localCollection, syncState);
        }
        this.syncedCollections.add(name);
      }

      // 3. Start single database-level change stream
      await this.startDatabaseStream();

      // Periodically check for newly added collections every 60 seconds
      this.discoveryInterval = setInterval(() => this.discoverNewCollections(), 60000);

      // Periodically check for divergence and sync indexes
      this.reconciliationInterval = setInterval(() => this.runReconciliation(), DIVERGENCE_CHECK_INTERVAL_MS);
      // Run initial reconciliation checks in background shortly after boot
      setTimeout(() => this.runReconciliation(), 5000);

      logger.info('Oplog Sync Service is running');
    } catch (err) {
      logger.error('Initialization failed', { error: err.message, stack: err.stack });
      await this.shutdown();
      process.exit(1);
    }
  }

  async startDatabaseStream() {
    if (this.dbChangeStream) {
      try {
        await this.dbChangeStream.close();
        logger.info('Closed existing database change stream before restart');
      } catch (err) {
        logger.warn(`Error closing existing database stream: ${err.message}`);
      }
      this.dbChangeStream = null;
    }

    try {
      const atlasDb = this.atlasClient.db();
      const localDb = this.localClient.db();

      let resumeToken = await this.syncManager.getDbResumeToken();

      logger.info('Starting Database Change Stream', {
        hasResumeToken: !!resumeToken
      });

      const pipeline = [
        {
          $match: {
            'ns.coll': { $regex: /^(?!_)(?!system\.)/ },
            operationType: { $in: ['insert', 'update', 'replace', 'delete', 'drop', 'rename', 'invalidate'] }
          }
        }
      ];

      const options = {
        fullDocument: 'updateLookup',
        maxAwaitTimeMS: 10000
      };

      if (resumeToken) {
        options.resumeAfter = resumeToken;
      }

      this.dbChangeStream = atlasDb.watch(pipeline, options);

      // Start asynchronous consumer loop
      (async () => {
        try {
          for await (const change of this.dbChangeStream) {
            if (!this.isRunning) break;
            await this.processDbChange(change, localDb);
          }
        } catch (err) {
          if (!this.isRunning) return;
          logger.error('Database Change Stream cursor error', { error: err.message });
          await this.handleDatabaseStreamError(err);
        }
      })();

    } catch (err) {
      logger.error('Failed to start database change stream', {
        error: err.message,
        stack: err.stack
      });
      await this.handleDatabaseStreamError(err);
    }
  }

  async processDbChange(change, localDb) {
    const ns = change.ns;
    if (!ns || !ns.coll) {
      if (change.operationType === 'invalidate') {
        logger.warn('Received invalidate event on database stream');
        throw new Error('Database stream invalidated');
      }
      return;
    }

    const collectionName = ns.coll;
    if (collectionName.startsWith('_') || collectionName.startsWith('system.')) {
      return;
    }

    this.syncedCollections.add(collectionName);
    const localCollection = localDb.collection(collectionName);

    await this.processChange(collectionName, change, localCollection);

    if (change._id) {
      await this.syncManager.saveDbResumeToken(change._id);
    }

    if (this.retryCount.has('__db_stream__')) {
      this.retryCount.delete('__db_stream__');
    }
  }

  async handleDatabaseStreamError(error) {
    if (this.dbChangeStream) {
      try {
        await this.dbChangeStream.close();
      } catch (err) {}
      this.dbChangeStream = null;
    }

    const isResumeError =
      error.code === 280 ||
      error.code === 286 ||
      (error.message && (
        error.message.includes('resume of change stream was not possible') ||
        error.message.includes('resume point may no longer be in the oplog') ||
        error.message.includes('resumeAfter')
      ));

    if (isResumeError) {
      logger.warn('Database resume token invalid or expired. Resetting database sync state.');
      await this.syncManager.saveDbResumeToken(null);
      
      const atlasDb = this.atlasClient.db();
      try {
        const collections = await atlasDb.listCollections().toArray();
        for (const col of collections) {
          const name = col.name;
          if (!name.startsWith('_') && name !== 'system.profile') {
            await this.syncManager.resetSyncStateForReSync(name);
          }
        }
      } catch (err) {
        logger.error('Failed to reset collection sync states', { error: err.message });
      }
      
      setTimeout(() => this.startDatabaseStream(), 100);
      return;
    }

    const retries = this.retryCount.get('__db_stream__') || 0;
    if (retries < MAX_RETRIES) {
      logger.info(`Retrying database stream in ${RETRY_DELAY_MS}ms (${retries + 1}/${MAX_RETRIES})`);
      this.retryCount.set('__db_stream__', retries + 1);
      setTimeout(() => this.startDatabaseStream(), RETRY_DELAY_MS);
    } else {
      logger.error('Max database stream retries exceeded. Mark all collections as failed.', { maxRetries: MAX_RETRIES });
      const atlasDb = this.atlasClient.db();
      try {
        const collections = await atlasDb.listCollections().toArray();
        for (const col of collections) {
          const name = col.name;
          if (!name.startsWith('_') && name !== 'system.profile') {
            this.failedStreams.add(name);
          }
        }
      } catch (e) {
        for (const name of this.syncedCollections) {
          this.failedStreams.add(name);
        }
      }
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
          const toNs = change.to;
          const newName = toNs && toNs.coll ? toNs.coll : (typeof toNs === 'string' ? toNs.split('.').pop() : null);
          if (newName) {
            await localCollection.rename(newName);
            logger.info(`[${collectionName}] RENAMED collection locally to ${newName} due to remote rename`);
          } else {
            logger.warn(`[${collectionName}] Received rename event but could not determine new name`, { to: toNs });
          }
          break;
        }

        case 'invalidate': {
          logger.warn(`[${collectionName}] Stream invalidated`);
          throw new Error('Stream invalidated');
        }
      }

      // Only checkpoint for data events, not DDL events like drop/rename/invalidate
      if (['insert', 'update', 'replace', 'delete'].includes(operationType)) {
        await this.syncManager.updateSyncState(
          collectionName,
          operationTime,
          change._id, // resume token
          1
        );
      }

      if (this.retryCount.has(collectionName)) {
        this.retryCount.delete(collectionName);
      }
    } catch (err) {
      const docId = change.documentKey ? change.documentKey._id : undefined;
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
      const localDb = this.localClient.db();
      const collections = await atlasDb.listCollections().toArray();
      const collectionNames = collections
        .map(c => c.name)
        .filter(name => !name.startsWith('_') && name !== 'system.profile');

      for (const name of collectionNames) {
        if (!this.syncedCollections.has(name)) {
          logger.info(`[${name}] Dynamic collection discovery detected new collection. Starting sync...`);
          this.syncedCollections.add(name);
          const remoteCollection = atlasDb.collection(name);
          const localCollection = localDb.collection(name);

          (async () => {
            try {
              const syncState = await this.syncManager.getSyncState(name);
              if (!syncState.initialSyncCompleted) {
                await this.performInitialSync(name, remoteCollection, localCollection, syncState);
              }
            } catch (err) {
              logger.error(`[${name}] Failed to sync dynamically discovered collection`, { error: err.message });
            }
          })();
        }
      }
      this.expectedCollectionsCount = collectionNames.length;
    } catch (err) {
      logger.warn('Failed to dynamically check for new collections', { error: err.message });
    }
  }

  async runReconciliation() {
    if (!this.isRunning) return;
    try {
      await this.runDivergenceCheck();
      await this.syncAllIndexes();
    } catch (err) {
      logger.error('Error during periodic reconciliation', { error: err.message });
    }
  }

  async runDivergenceCheck() {
    logger.info('Starting divergence check...');
    const atlasDb = this.atlasClient.db();
    const localDb = this.localClient.db();

    const collections = await atlasDb.listCollections().toArray();
    const collectionNames = collections
      .map(c => c.name)
      .filter(name => !name.startsWith('_') && name !== 'system.profile');

    const newDivergences = new Map();

    for (const name of collectionNames) {
      try {
        const remoteColl = atlasDb.collection(name);
        const localColl = localDb.collection(name);

        const remoteCount = await remoteColl.countDocuments();
        const localCount = await localColl.countDocuments();

        const diff = Math.abs(remoteCount - localCount);
        newDivergences.set(name, diff);

        if (diff > 0) {
          logger.warn(`[${name}] Divergence detected! Remote count: ${remoteCount}, Local count: ${localCount}. Difference: ${diff}`);
        } else {
          logger.info(`[${name}] In sync. Count: ${remoteCount}`);
        }
      } catch (err) {
        logger.error(`[${name}] Failed to run divergence check`, { error: err.message });
      }
    }
    this.divergences = newDivergences;
  }

  async syncAllIndexes() {
    logger.info('Starting periodic index synchronization...');
    const atlasDb = this.atlasClient.db();
    const localDb = this.localClient.db();

    const collections = await atlasDb.listCollections().toArray();
    const collectionNames = collections
      .map(c => c.name)
      .filter(name => !name.startsWith('_') && name !== 'system.profile');

    for (const name of collectionNames) {
      try {
        const remoteColl = atlasDb.collection(name);
        const localColl = localDb.collection(name);
        await this.syncIndexes(name, remoteColl, localColl);
      } catch (err) {
        logger.error(`[${name}] Failed to periodically sync indexes`, { error: err.message });
      }
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

    const divergenceObj = {};
    for (const [col, diff] of this.divergences.entries()) {
      divergenceObj[col] = diff;
    }

    return {
      isRunning: this.isRunning && isAtlasConnected && isLocalConnected,
      connectedCollections: this.syncedCollections.size,
      expectedCollectionsCount: this.expectedCollectionsCount,
      failedStreams: Array.from(this.failedStreams),
      isAtlasConnected,
      isLocalConnected,
      syncMetrics: metrics,
      divergences: divergenceObj,
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

    if (this.reconciliationInterval) {
      clearInterval(this.reconciliationInterval);
      this.reconciliationInterval = null;
    }

    if (this.dbChangeStream) {
      try {
        await this.dbChangeStream.close();
        logger.info('Closed database change stream');
      } catch (err) {
        logger.error('Error closing database change stream', { error: err.message });
      }
      this.dbChangeStream = null;
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
