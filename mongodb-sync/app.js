require('dotenv').config();
const logger = require('./logger');
const OplogSyncService = require('./oplog-sync-service');
const createHealthServer = require('./health-server');

async function main() {
  const syncService = new OplogSyncService();
  let shuttingDown = false;

  try {
    await syncService.initialize();
    const server = createHealthServer(syncService);

    const shutdown = async (signal) => {
      if (shuttingDown) return;
      shuttingDown = true;
      logger.info(`${signal} received, shutting down...`);
      try {
        await new Promise((resolve) => server.close(resolve));
        await syncService.shutdown();
        process.exit(0);
      } catch (err) {
        logger.error('Error during shutdown', { error: err.message });
        process.exit(1);
      }
    };

    process.on('SIGTERM', () => {
      shutdown('SIGTERM');
    });
    process.on('SIGINT', () => {
      shutdown('SIGINT');
    });
    process.on('unhandledRejection', (reason) => {
      logger.error('Unhandled Rejection detected', {
        error: reason instanceof Error ? reason.message : reason,
        stack: reason instanceof Error ? reason.stack : undefined
      });
      shutdown('unhandledRejection');
    });
    process.on('uncaughtException', (err) => {
      logger.error('Uncaught Exception detected', { error: err.message, stack: err.stack });
      shutdown('uncaughtException');
    });
  } catch (err) {
    logger.error('Fatal error during startup', { error: err.message, stack: err.stack });
    process.exit(1);
  }
}

main();
