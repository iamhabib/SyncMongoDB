const express = require('express');
const logger = require('./logger');

function createHealthServer(syncService, port = process.env.SYNC_AGENT_PORT || process.env.PORT || 3000) {
  const app = express();
  const host = process.env.HEALTH_BIND_HOST || '127.0.0.1';

  app.get('/health', async (req, res) => {
    try {
      const health = await syncService.getHealth();

      if (health.isRunning) {
        let status = 'healthy';
        if (health.failedStreams && health.failedStreams.length > 0) {
          status = 'degraded';
        } else if (health.reSyncRunning || (health.initialSyncInProgress && health.initialSyncInProgress.length > 0)) {
          status = 'syncing';
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
            opTimeMs = raw.$timestamp.t * 1000;
          } else if (typeof raw.getHighBits === 'function') {
            opTimeMs = raw.getHighBits() * 1000;
          } else {
            opTimeMs = new Date(raw).getTime();
          }
          lag = Number.isFinite(opTimeMs) ? Math.max(0, (Date.now() - opTimeMs) / 1000) : 0;
        }
        const safeName = String(m.collection).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        promText += `mongodb_sync_lag_seconds{collection="${safeName}"} ${lag.toFixed(3)}\n`;
      }
      promText += '\n';

      promText += '# HELP mongodb_sync_documents_total Total number of synchronized documents/events\n';
      promText += '# TYPE mongodb_sync_documents_total counter\n';
      for (const m of metrics) {
        const safeName = String(m.collection).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        promText += `mongodb_sync_documents_total{collection="${safeName}"} ${m.totalSynced || 0}\n`;
      }
      promText += '\n';

      promText += '# HELP mongodb_sync_stream_running Status of collection change stream (1 = running, 0 = failed/stopped)\n';
      promText += '# TYPE mongodb_sync_stream_running gauge\n';
      for (const m of metrics) {
        const safeName = String(m.collection).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        const isFailed = health.failedStreams && health.failedStreams.includes(m.collection);
        const statusVal = isFailed ? 0 : 1;
        promText += `mongodb_sync_stream_running{collection="${safeName}"} ${statusVal}\n`;
      }
      promText += '\n';

      promText += '# HELP mongodb_sync_divergence_count Difference in document count between remote and local collection\n';
      promText += '# TYPE mongodb_sync_divergence_count gauge\n';
      for (const m of metrics) {
        const safeName = String(m.collection).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        const diff = (health.divergences && health.divergences[m.collection]) || 0;
        promText += `mongodb_sync_divergence_count{collection="${safeName}"} ${diff}\n`;
      }

      res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      res.send(promText);
    } catch (err) {
      res.status(500).set('Content-Type', 'text/plain').send(`# Error getting metrics: ${err.message}\n`);
    }
  });

  const server = app.listen(port, host, () => {
    logger.info(`Health server listening on http://${host}:${port}`);
  });

  return server;
}

module.exports = createHealthServer;
