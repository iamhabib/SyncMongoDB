const { MongoClient } = require('mongodb');
require('dotenv').config();
const logger = require('./logger');
const SyncManager = require('./sync-manager');

const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '10', 10);
const RETRY_DELAY_MS = parseInt(process.env.RETRY_DELAY_MS || '5000', 10);
const DISCOVERY_INTERVAL_MS = parseInt(process.env.DISCOVERY_INTERVAL_MS || '60000', 10);
const DIVERGENCE_CHECK_INTERVAL_MS = parseInt(process.env.DIVERGENCE_CHECK_INTERVAL_MS || '21600000', 10);
const AUTO_REPAIR_ON_DIVERGENCE = String(process.env.AUTO_REPAIR_ON_DIVERGENCE || 'false').toLowerCase() === 'true';
const DIVERGENCE_REPAIR_THRESHOLD = parseInt(process.env.DIVERGENCE_REPAIR_THRESHOLD || '1', 10);
/** When true, each applied change is logged at info (handy with monitor.sh option 7). */
const LOG_REPLICATION_EVENTS =
  String(process.env.LOG_REPLICATION_EVENTS || 'false').toLowerCase() === 'true';

function interpolateUrl(url) {
  if (!url) return url;
  return url
    .replace(/{LOCAL_MONGO_PORT}/g, process.env.LOCAL_MONGO_PORT || '27017')
    .replace(/{MONGO_DATABASE_NAME}/g, process.env.MONGO_DATABASE_NAME || 'sync_db')
    .replace(/{LOCAL_MONGO_ROOT_USER}/g, process.env.LOCAL_MONGO_ROOT_USER || 'admin')
    .replace(/{LOCAL_MONGO_ROOT_PASSWORD}/g, process.env.LOCAL_MONGO_ROOT_PASSWORD || '');
}

function resolveLocalMongoUrl() {
  if (process.env.LOCAL_MONGO_URL) {
    return interpolateUrl(process.env.LOCAL_MONGO_URL);
  }
  const user = process.env.LOCAL_MONGO_ROOT_USER || 'admin';
  const pass = process.env.LOCAL_MONGO_ROOT_PASSWORD || '';
  const port = process.env.LOCAL_MONGO_PORT || '27017';
  const db = process.env.MONGO_DATABASE_NAME || 'sync_db';
  if (!pass) {
    throw new Error(
      'LOCAL_MONGO_URL is not set; provide it, or set LOCAL_MONGO_ROOT_USER / LOCAL_MONGO_ROOT_PASSWORD'
    );
  }
  // Default for local/dev; Docker Compose overrides this to host `mongo:27017`
  return `mongodb://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@localhost:${port}/${db}?authSource=admin`;
}

function isUserCollection(name) {
  return name && !name.startsWith('_') && name !== 'system.profile' && !name.startsWith('system.');
}

class OplogSyncService {
  constructor() {
    if (!process.env.REMOTE_MONGODB_URL) {
      throw new Error('REMOTE_MONGODB_URL is required but not defined in environmental variables');
    }

    const remoteUrl = interpolateUrl(process.env.REMOTE_MONGODB_URL);
    const localUrl = resolveLocalMongoUrl();

    this.atlasClient = new MongoClient(remoteUrl);
    this.localClient = new MongoClient(localUrl);
    // Share the local connection pool with SyncManager
    this.syncManager = new SyncManager(this.localClient);
    this.dbChangeStream = null;
    this.syncedCollections = new Set();
    /** Collections currently running initial sync — stream events for these are deferred via stream pause/replay. */
    this.initialSyncInProgress = new Set();
    this.retryCount = new Map();
    this.isRunning = false;
    this.expectedCollectionsCount = 0;
    this.discoveryInterval = null;
    this.reconciliationInterval = null;
    this.failedStreams = new Set();
    this.divergences = new Map();
    this._streamRestartTimer = null;
    this._discoveryRunning = false;
    this._reconciliationRunning = false;
    this._reSyncRunning = false;
    this._shutdownPromise = null;
  }

  async initialize() {
    try {
      logger.info('Initializing Oplog Sync Service...');

      await this.atlasClient.connect();
      await this.localClient.connect();
      await this.syncManager.initialize();

      const atlasDb = this.atlasClient.db();
      const collections = await atlasDb.listCollections().toArray();

      const collectionNames = collections.map((c) => c.name).filter(isUserCollection);

      this.expectedCollectionsCount = collectionNames.length;

      logger.info('Connected to Atlas & Local MongoDB', {
        collectionsCount: this.expectedCollectionsCount
      });

      this.isRunning = true;

      // 1. Capture a starting database-level resume token before any initial copy
      await this.ensureDbResumeToken(atlasDb);

      // 2. Initial sync for collections that need it
      const localDb = this.localClient.db();
      for (const name of collectionNames) {
        await this.ensureCollectionInitialSync(name, atlasDb, localDb);
        this.syncedCollections.add(name);
      }

      // 3. Start single database-level change stream
      await this.startDatabaseStream();

      this.discoveryInterval = setInterval(() => {
        this.discoverNewCollections().catch((err) => {
          logger.warn('Discovery interval error', { error: err.message });
        });
      }, DISCOVERY_INTERVAL_MS);

      this.reconciliationInterval = setInterval(() => {
        this.runReconciliation().catch((err) => {
          logger.error('Reconciliation interval error', { error: err.message });
        });
      }, DIVERGENCE_CHECK_INTERVAL_MS);

      setTimeout(() => {
        this.runReconciliation().catch((err) => {
          logger.error('Initial reconciliation error', { error: err.message });
        });
      }, 5000);

      logger.info('Oplog Sync Service is running');
    } catch (err) {
      logger.error('Initialization failed', { error: err.message, stack: err.stack });
      await this.shutdown();
      process.exit(1);
    }
  }

  async ensureDbResumeToken(atlasDb) {
    let resumeToken = await this.syncManager.getDbResumeToken();
    if (resumeToken) return resumeToken;

    logger.info('No database-level resume token found. Fetching current starting resume token...');
    const tempStream = atlasDb.watch([], { maxAwaitTimeMS: 1000 });
    try {
      await tempStream.tryNext();
    } catch (err) {
      // ignore timeout / empty cursor
    }
    resumeToken = tempStream.resumeToken;
    await tempStream.close();
    if (resumeToken) {
      await this.syncManager.saveDbResumeToken(resumeToken);
      logger.info('Saved initial database-level resume token');
    }
    return resumeToken;
  }

  async ensureCollectionInitialSync(name, atlasDb, localDb) {
    const remoteCollection = atlasDb.collection(name);
    const localCollection = localDb.collection(name);
    const syncState = await this.syncManager.getSyncState(name);
    if (!syncState.initialSyncCompleted) {
      await this.performInitialSync(name, remoteCollection, localCollection, syncState);
    }
  }

  async startDatabaseStream() {
    if (!this.isRunning) return;

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
            operationType: {
              $in: ['insert', 'update', 'replace', 'delete', 'drop', 'rename', 'invalidate']
            }
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
    if (!isUserCollection(collectionName)) {
      return;
    }

    // Skip applying events while that collection's initial sync is in progress.
    // Caller must pause the stream around initial sync so these events are replayed after.
    if (this.initialSyncInProgress.has(collectionName)) {
      logger.debug(`[${collectionName}] Deferring stream event until initial sync completes`, {
        operationType: change.operationType
      });
      return;
    }

    // Unknown collection (seen on the stream before discovery) — do not apply until
    // a paused initial sync completes. Avoid getSyncState on the hot path for known cols.
    if (!this.syncedCollections.has(collectionName)) {
      logger.info(`[${collectionName}] Change seen for unsynced collection; scheduling safe initial sync`);
      this.scheduleSafeCollectionSync(collectionName).catch((err) => {
        logger.error(`[${collectionName}] Failed scheduled initial sync`, { error: err.message });
      });
      return;
    }
    const localCollection = localDb.collection(collectionName);

    await this.processChange(collectionName, change, localCollection);

    if (change._id) {
      this.syncManager.queueDbResumeToken(change._id);
    }

    if (this.retryCount.has('__db_stream__')) {
      this.retryCount.delete('__db_stream__');
    }
  }

  /**
   * Pause the DB stream, capture/persist resume token, run work, then restart
   * the stream from that token so events during the pause are replayed in order.
   */
  async withStreamPaused(workFn) {
    const wasRunning = this.isRunning;
    let savedToken = null;

    if (this.dbChangeStream) {
      try {
        savedToken = this.dbChangeStream.resumeToken || (await this.syncManager.getDbResumeToken());
        await this.dbChangeStream.close();
      } catch (err) {
        logger.warn('Error pausing database stream', { error: err.message });
        savedToken = await this.syncManager.getDbResumeToken();
      }
      this.dbChangeStream = null;
    } else {
      savedToken = await this.syncManager.getDbResumeToken();
    }

    if (savedToken) {
      await this.syncManager.saveDbResumeToken(savedToken);
    }

    try {
      await workFn();
    } finally {
      if (wasRunning && this.isRunning) {
        await this.startDatabaseStream();
      }
    }
  }

  async scheduleSafeCollectionSync(collectionName) {
    if (this.initialSyncInProgress.has(collectionName) || this._discoveryRunning) {
      return;
    }
    await this.syncNewCollectionsSafely([collectionName]);
  }

  async syncNewCollectionsSafely(collectionNames) {
    const names = collectionNames.filter((n) => isUserCollection(n));
    if (names.length === 0) return;

    await this.withStreamPaused(async () => {
      const atlasDb = this.atlasClient.db();
      const localDb = this.localClient.db();

      // Refresh starting token immediately before copies
      await this.ensureDbResumeToken(atlasDb);

      for (const name of names) {
        this.initialSyncInProgress.add(name);
        try {
          await this.ensureCollectionInitialSync(name, atlasDb, localDb);
          this.syncedCollections.add(name);
          this.failedStreams.delete(name);
        } finally {
          this.initialSyncInProgress.delete(name);
        }
      }
    });
  }

  async handleDatabaseStreamError(error) {
    if (this.dbChangeStream) {
      try {
        await this.dbChangeStream.close();
      } catch (err) {
        // ignore
      }
      this.dbChangeStream = null;
    }

    if (!this.isRunning) return;

    const isResumeError =
      error.code === 280 ||
      error.code === 286 ||
      (error.message &&
        (error.message.includes('resume of change stream was not possible') ||
          error.message.includes('resume point may no longer be in the oplog') ||
          error.message.includes('resumeAfter') ||
          error.message.includes('Database stream invalidated') ||
          error.message.includes('Stream invalidated')));

    if (isResumeError) {
      logger.warn('Database resume token invalid or expired. Performing full re-sync.');
      await this.performFullReSync();
      return;
    }

    const retries = this.retryCount.get('__db_stream__') || 0;
    if (retries < MAX_RETRIES) {
      logger.info(`Retrying database stream in ${RETRY_DELAY_MS}ms (${retries + 1}/${MAX_RETRIES})`);
      this.retryCount.set('__db_stream__', retries + 1);
      this._clearStreamRestartTimer();
      this._streamRestartTimer = setTimeout(() => {
        this.startDatabaseStream().catch((err) => {
          logger.error('Failed to restart database stream', { error: err.message });
        });
      }, RETRY_DELAY_MS);
    } else {
      logger.error('Max database stream retries exceeded. Mark all collections as failed.', {
        maxRetries: MAX_RETRIES
      });
      const atlasDb = this.atlasClient.db();
      try {
        const collections = await atlasDb.listCollections().toArray();
        for (const col of collections) {
          if (isUserCollection(col.name)) {
            this.failedStreams.add(col.name);
          }
        }
      } catch (e) {
        for (const name of this.syncedCollections) {
          this.failedStreams.add(name);
        }
      }
    }
  }

  /**
   * Reset metadata, re-copy all collections from a fresh resume token, then restart the stream.
   * Used when the change stream resume point is no longer in the oplog.
   */
  async performFullReSync() {
    if (this._reSyncRunning) {
      logger.warn('Full re-sync already in progress; skipping duplicate request');
      return;
    }
    this._reSyncRunning = true;

    try {
      await this.syncManager.saveDbResumeToken(null);
      this.syncManager.pendingDbResumeToken = null;
      this.syncManager.pendingDbResumeTokenDirty = false;

      const atlasDb = this.atlasClient.db();
      const localDb = this.localClient.db();
      const collections = await atlasDb.listCollections().toArray();
      const collectionNames = collections.map((c) => c.name).filter(isUserCollection);

      for (const name of collectionNames) {
        await this.syncManager.resetSyncStateForReSync(name);
        this.syncedCollections.delete(name);
      }

      // Capture a NEW starting token, then re-copy everything, then start stream from that token
      await this.ensureDbResumeToken(atlasDb);

      for (const name of collectionNames) {
        this.initialSyncInProgress.add(name);
        try {
          // Drop local data so we do not keep orphans from the lost gap
          try {
            await localDb.collection(name).drop();
          } catch (err) {
            if (err.codeName !== 'NamespaceNotFound') {
              logger.warn(`[${name}] Could not drop local collection before re-sync`, {
                error: err.message
              });
            }
          }
          await this.ensureCollectionInitialSync(name, atlasDb, localDb);
          this.syncedCollections.add(name);
          this.failedStreams.delete(name);
        } finally {
          this.initialSyncInProgress.delete(name);
        }
      }

      this.expectedCollectionsCount = collectionNames.length;
      this.retryCount.delete('__db_stream__');

      if (this.isRunning) {
        await this.startDatabaseStream();
      }
      logger.info('Full re-sync completed; database change stream restarted');
    } catch (err) {
      logger.error('Full re-sync failed', { error: err.message, stack: err.stack });
      for (const name of this.syncedCollections) {
        this.failedStreams.add(name);
      }
      this._clearStreamRestartTimer();
      this._streamRestartTimer = setTimeout(() => {
        this.performFullReSync().catch((e) => {
          logger.error('Retry of full re-sync failed', { error: e.message });
        });
      }, RETRY_DELAY_MS);
    } finally {
      this._reSyncRunning = false;
    }
  }

  logReplicationEvent(collectionName, operationType, docId, operationTime) {
    const payload = { docId, timestamp: operationTime };
    const msg = `[${collectionName}] ${operationType}`;
    if (LOG_REPLICATION_EVENTS) {
      logger.info(msg, payload);
    } else {
      logger.debug(msg, payload);
    }
  }

  async processChange(collectionName, change, localCollection) {
    const operationType = change.operationType;

    let operationTime;
    if (change.clusterTime) {
      if (typeof change.clusterTime.getHighBits === 'function') {
        operationTime = new Date(change.clusterTime.getHighBits() * 1000);
      } else if (typeof change.clusterTime.getHighOrder === 'function') {
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
          await localCollection.replaceOne({ _id: docId }, change.fullDocument, { upsert: true });
          this.logReplicationEvent(collectionName, 'INSERT', docId, operationTime);
          break;
        }

        case 'update':
        case 'replace': {
          const docId = change.documentKey._id;
          if (change.fullDocument) {
            await localCollection.replaceOne({ _id: docId }, change.fullDocument, { upsert: true });
          } else {
            await localCollection.deleteOne({ _id: docId });
          }
          this.logReplicationEvent(collectionName, 'UPDATE', docId, operationTime);
          break;
        }

        case 'delete': {
          const docId = change.documentKey._id;
          await localCollection.deleteOne({ _id: docId });
          this.logReplicationEvent(collectionName, 'DELETE', docId, operationTime);
          break;
        }

        case 'drop': {
          try {
            await localCollection.drop();
            logger.info(`[${collectionName}] DROPPED collection locally due to remote drop`);
          } catch (err) {
            if (err.codeName !== 'NamespaceNotFound') throw err;
          }
          this.syncedCollections.delete(collectionName);
          await this.syncManager.resetSyncStateForReSync(collectionName);
          break;
        }

        case 'rename': {
          const toNs = change.to;
          const newName =
            toNs && toNs.coll
              ? toNs.coll
              : typeof toNs === 'string'
                ? toNs.split('.').pop()
                : null;
          if (newName) {
            await localCollection.rename(newName, { dropTarget: true });
            this.syncedCollections.delete(collectionName);
            this.syncedCollections.add(newName);
            await this.syncManager.resetSyncStateForReSync(collectionName);
            await this.syncManager.completeInitialSync(newName);
            logger.info(
              `[${collectionName}] RENAMED collection locally to ${newName} due to remote rename`
            );
          } else {
            logger.warn(`[${collectionName}] Received rename event but could not determine new name`, {
              to: toNs
            });
          }
          break;
        }

        case 'invalidate': {
          logger.warn(`[${collectionName}] Stream invalidated`);
          throw new Error('Stream invalidated');
        }
      }

      if (['insert', 'update', 'replace', 'delete'].includes(operationType)) {
        await this.syncManager.updateSyncState(collectionName, operationTime, change._id, 1);
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
      const remoteNames = new Set();

      for (const index of remoteIndexes) {
        if (index.name === '_id_') continue;
        remoteNames.add(index.name);

        const { key, options } = SyncManager.sanitizeIndexOptions(index);
        logger.info(`[${collectionName}] Replicating index: ${options.name}`, { key });
        try {
          await localCollection.createIndex(key, options);
        } catch (err) {
          // IndexOptionsConflict / IndexKeySpecsConflict — leave existing index
          if (err.code === 85 || err.code === 86) {
            logger.warn(`[${collectionName}] Index conflict for ${options.name}; leaving local index`, {
              error: err.message
            });
          } else {
            throw err;
          }
        }
      }

      // Drop local secondary indexes that no longer exist on remote
      const localIndexes = await localCollection.indexes();
      for (const index of localIndexes) {
        if (index.name === '_id_') continue;
        if (!remoteNames.has(index.name)) {
          logger.info(`[${collectionName}] Dropping obsolete local index: ${index.name}`);
          await localCollection.dropIndex(index.name);
        }
      }

      logger.info(`[${collectionName}] Indexes synced successfully.`);
    } catch (err) {
      logger.warn(`[${collectionName}] Failed to sync indexes. Syncing documents will proceed.`, {
        error: err.message
      });
    }
  }

  async performInitialSync(collectionName, remoteCollection, localCollection, syncState) {
    logger.info(`[${collectionName}] Starting Initial Sync...`);
    this.initialSyncInProgress.add(collectionName);

    try {
      await this.syncIndexes(collectionName, remoteCollection, localCollection);

      let resumeToken = syncState.resumeToken;

      if (!resumeToken) {
        logger.info(`[${collectionName}] Retrieving starting resume token...`);
        const tempStream = remoteCollection.watch([], { maxAwaitTimeMS: 1000 });
        try {
          await tempStream.tryNext();
        } catch (err) {
          // ignore
        }
        resumeToken = tempStream.resumeToken;
        await tempStream.close();

        if (!resumeToken) {
          logger.warn(
            `[${collectionName}] Could not obtain starting resume token from watch. Change stream will resume from default context.`
          );
        }
      }

      let lastCopiedId = syncState.lastCopiedId;
      const batchSize = 1000;
      let totalSyncedInThisSession = 0;

      logger.info(`[${collectionName}] Copying documents in batches of ${batchSize}...`, {
        lastCopiedId
      });

      while (true) {
        if (!this.isRunning) {
          throw new Error('Service shutdown during initial sync');
        }

        const query = lastCopiedId ? { _id: { $gt: lastCopiedId } } : {};
        const batch = await remoteCollection.find(query).sort({ _id: 1 }).limit(batchSize).toArray();

        if (batch.length === 0) {
          break;
        }

        const operations = batch.map((doc) => ({
          replaceOne: {
            filter: { _id: doc._id },
            replacement: doc,
            upsert: true
          }
        }));

        await localCollection.bulkWrite(operations, { ordered: false });

        lastCopiedId = batch[batch.length - 1]._id;
        totalSyncedInThisSession += batch.length;

        await this.syncManager.updateInitialSyncProgress(
          collectionName,
          lastCopiedId,
          resumeToken,
          batch.length
        );

        logger.info(
          `[${collectionName}] Copied ${totalSyncedInThisSession} documents (last _id: ${lastCopiedId})`
        );
      }

      await this.syncManager.completeInitialSync(collectionName);
      logger.info(`[${collectionName}] Initial Sync completed successfully.`);
    } finally {
      this.initialSyncInProgress.delete(collectionName);
    }
  }

  async discoverNewCollections() {
    if (!this.isRunning || this._discoveryRunning || this._reSyncRunning) return;
    this._discoveryRunning = true;

    try {
      const atlasDb = this.atlasClient.db();
      const collections = await atlasDb.listCollections().toArray();
      const collectionNames = collections.map((c) => c.name).filter(isUserCollection);

      const newNames = collectionNames.filter((name) => !this.syncedCollections.has(name));

      if (newNames.length > 0) {
        logger.info('Dynamic collection discovery detected new collections', {
          collections: newNames
        });
        await this.syncNewCollectionsSafely(newNames);
      }

      this.expectedCollectionsCount = collectionNames.length;
    } catch (err) {
      logger.warn('Failed to dynamically check for new collections', { error: err.message });
    } finally {
      this._discoveryRunning = false;
    }
  }

  async runReconciliation() {
    if (!this.isRunning || this._reconciliationRunning || this._reSyncRunning) return;
    this._reconciliationRunning = true;
    try {
      await this.runDivergenceCheck();
      await this.syncAllIndexes();
    } catch (err) {
      logger.error('Error during periodic reconciliation', { error: err.message });
    } finally {
      this._reconciliationRunning = false;
    }
  }

  async runDivergenceCheck() {
    logger.info('Starting divergence check...');
    const atlasDb = this.atlasClient.db();
    const localDb = this.localClient.db();

    const collections = await atlasDb.listCollections().toArray();
    const collectionNames = collections.map((c) => c.name).filter(isUserCollection);

    const newDivergences = new Map();
    const toRepair = [];

    for (const name of collectionNames) {
      try {
        const remoteColl = atlasDb.collection(name);
        const localColl = localDb.collection(name);

        // Prefer exact counts for repair decisions; fall back to estimated on timeout/failure
        let remoteCount;
        let localCount;
        try {
          remoteCount = await remoteColl.countDocuments({}, { maxTimeMS: 120000 });
          localCount = await localColl.countDocuments({}, { maxTimeMS: 120000 });
        } catch (countErr) {
          logger.warn(`[${name}] Exact count timed out; using estimatedDocumentCount`, {
            error: countErr.message
          });
          remoteCount = await remoteColl.estimatedDocumentCount();
          localCount = await localColl.estimatedDocumentCount();
        }

        const diff = Math.abs(remoteCount - localCount);
        newDivergences.set(name, diff);

        if (diff > 0) {
          logger.warn(
            `[${name}] Divergence detected! Remote count: ${remoteCount}, Local count: ${localCount}. Difference: ${diff}`
          );
          if (AUTO_REPAIR_ON_DIVERGENCE && diff >= DIVERGENCE_REPAIR_THRESHOLD) {
            toRepair.push(name);
          }
        } else {
          logger.info(`[${name}] In sync. Count: ${remoteCount}`);
        }
      } catch (err) {
        logger.error(`[${name}] Failed to run divergence check`, { error: err.message });
      }
    }
    this.divergences = newDivergences;

    if (toRepair.length > 0) {
      logger.warn('AUTO_REPAIR_ON_DIVERGENCE enabled; re-syncing diverged collections', {
        collections: toRepair
      });
      await this.repairCollections(toRepair);
    }
  }

  async repairCollections(collectionNames) {
    await this.withStreamPaused(async () => {
      const atlasDb = this.atlasClient.db();
      const localDb = this.localClient.db();
      await this.ensureDbResumeToken(atlasDb);

      for (const name of collectionNames) {
        this.initialSyncInProgress.add(name);
        try {
          await this.syncManager.resetSyncStateForReSync(name);
          try {
            await localDb.collection(name).drop();
          } catch (err) {
            if (err.codeName !== 'NamespaceNotFound') {
              logger.warn(`[${name}] Could not drop local collection during repair`, {
                error: err.message
              });
            }
          }
          await this.ensureCollectionInitialSync(name, atlasDb, localDb);
          this.syncedCollections.add(name);
          this.failedStreams.delete(name);
          this.divergences.set(name, 0);
        } finally {
          this.initialSyncInProgress.delete(name);
        }
      }
    });
  }

  async syncAllIndexes() {
    logger.info('Starting periodic index synchronization...');
    const atlasDb = this.atlasClient.db();
    const localDb = this.localClient.db();

    const collections = await atlasDb.listCollections().toArray();
    const collectionNames = collections.map((c) => c.name).filter(isUserCollection);

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
      initialSyncInProgress: Array.from(this.initialSyncInProgress),
      isAtlasConnected,
      isLocalConnected,
      reSyncRunning: this._reSyncRunning,
      syncMetrics: metrics,
      divergences: divergenceObj,
      timestamp: new Date()
    };
  }

  _clearStreamRestartTimer() {
    if (this._streamRestartTimer) {
      clearTimeout(this._streamRestartTimer);
      this._streamRestartTimer = null;
    }
  }

  async shutdown() {
    if (this._shutdownPromise) {
      return this._shutdownPromise;
    }

    this._shutdownPromise = (async () => {
      logger.info('Shutting down gracefully...');
      this.isRunning = false;
      this._clearStreamRestartTimer();

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
        await this.syncManager.close();
        await this.atlasClient.close();
        await this.localClient.close();
        logger.info('Shutdown complete');
      } catch (err) {
        logger.error('Error during shutdown', { error: err.message });
      }
    })();

    return this._shutdownPromise;
  }
}

module.exports = OplogSyncService;
