const express = require('express');
const logger = require('./logger');

function createHealthServer(syncService, port = process.env.PORT || 3000) {
  const app = express();

  app.get('/health', async (req, res) => {
    try {
      const health = await syncService.getHealth();

      if (health.isRunning) {
        let status = 'healthy';
        if (health.failedStreams && health.failedStreams.length > 0) {
          status = 'degraded';
        } else if (health.connectedCollections < health.expectedCollectionsCount) {
          status = 'initializing';
        }
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
      const metrics = health.syncMetrics;

      let promText = '';

      // 1. Lag Metric
      promText += '# HELP mongodb_sync_lag_seconds Lag in seconds between remote and local collection\n';
      promText += '# TYPE mongodb_sync_lag_seconds gauge\n';
      for (const m of metrics) {
        let lag = 0;
        if (m.lastOperationTime) {
          let opTimeMs;
          const raw = m.lastOperationTime;
          if (raw instanceof Date) {
            opTimeMs = raw.getTime();
          } else if (typeof raw === 'number') {
            opTimeMs = raw;
          } else if (typeof raw === 'object' && raw.$timestamp) {
            // BSON Timestamp stored as {$timestamp: {t: <seconds>, i: <increment>}}
            opTimeMs = raw.$timestamp.t * 1000;
          } else if (typeof raw.getHighBits === 'function') {
            // Live BSON Timestamp object from driver
            opTimeMs = raw.getHighBits() * 1000;
          } else {
            opTimeMs = new Date(raw).getTime();
          }
          lag = Number.isFinite(opTimeMs) ? Math.max(0, (Date.now() - opTimeMs) / 1000) : 0;
        }
        promText += `mongodb_sync_lag_seconds{collection="${m.collection}"} ${lag.toFixed(3)}\n`;
      }
      promText += '\n';

      // 2. Documents Total Metric
      promText += '# HELP mongodb_sync_documents_total Total number of synchronized documents/events\n';
      promText += '# TYPE mongodb_sync_documents_total counter\n';
      for (const m of metrics) {
        promText += `mongodb_sync_documents_total{collection="${m.collection}"} ${m.totalSynced || 0}\n`;
      }
      promText += '\n';

      // 3. Stream Status Metric
      promText += '# HELP mongodb_sync_stream_running Status of collection change stream (1 = running, 0 = failed/stopped)\n';
      promText += '# TYPE mongodb_sync_stream_running gauge\n';
      for (const m of metrics) {
        const isFailed = health.failedStreams && health.failedStreams.includes(m.collection);
        const statusVal = isFailed ? 0 : 1;
        promText += `mongodb_sync_stream_running{collection="${m.collection}"} ${statusVal}\n`;
      }
      promText += '\n';

      // 4. Divergence Metric
      promText += '# HELP mongodb_sync_divergence_count Difference in document count between remote and local collection\n';
      promText += '# TYPE mongodb_sync_divergence_count gauge\n';
      for (const m of metrics) {
        const diff = (health.divergences && health.divergences[m.collection]) || 0;
        promText += `mongodb_sync_divergence_count{collection="${m.collection}"} ${diff}\n`;
      }

      res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      res.send(promText);
    } catch (err) {
      res.status(500).set('Content-Type', 'text/plain').send(`# Error getting metrics: ${err.message}\n`);
    }
  });

  const server = app.listen(port, () => {
    logger.info(`Health server listening on port ${port}`);
  });

  return server;
}

module.exports = createHealthServer;
