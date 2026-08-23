const express = require('express');
const logger = require('./logger');

function createHealthServer(syncService, port = process.env.PORT || 3000) {
  const app = express();

  app.get('/health', async (req, res) => {
    try {
      const health = await syncService.getHealth();

      if (health.isRunning) {
        const status = health.connectedCollections === health.expectedCollectionsCount
          ? 'healthy'
          : 'initializing';
        return res.status(200).json({ status, ...health });
      }
      return res.status(503).json({ status: 'unhealthy', ...health });
    } catch (err) {
      logger.error('Health check error', { error: err.message });
      res.status(500).json({ status: 'error', error: err.message });
    }
  });

  app.get('/metrics', async (req, res) => {
    try {
      const health = await syncService.getHealth();
      res.json(health.syncMetrics);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  const server = app.listen(port, () => {
    logger.info(`Health server listening on port ${port}`);
  });

  return server;
}

module.exports = createHealthServer;
