require('dotenv').config();
const logger = require('./logger');
const OplogSyncService = require('./oplog-sync-service');
const createHealthServer = require('./health-server');

async function main() {
  const syncService = new OplogSyncService();

  try {
    await syncService.initialize();
    const server = createHealthServer(syncService);

    const shutdown = async (signal) => {
      logger.info(`${signal} received, shutting down...`);
      server.close();
      await syncService.shutdown();
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
  } catch (err) {
    logger.error('Fatal error', { error: err.message, stack: err.stack });
    process.exit(1);
  }
}

main();
