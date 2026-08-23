# MongoDB Remote → Local Change Stream Sync

Production-grade sync service that mirrors collection changes from a remote MongoDB (e.g. Atlas) into a local/self-hosted MongoDB instance, in real time, using **Change Streams** and an automatic **Initial Sync** engine. No `updatedAt` field is required — progress is tracked via Change Stream resume tokens and keyset pagination metadata stored in the `_sync_metadata` collection on the target database.

## Architecture & Production Features

1. **Automated Initial Sync**: If a collection has not been synced, the service automatically copies all pre-existing documents in batches before starting the Change Stream.
2. **Resumable Keyset Batching**: Uses keyset pagination (`_id > lastId` sorted by `_id`) to perform initial syncs efficiently on large collections (5GB+) without memory bloat. If the service restarts, it resumes copying exactly where it left off.
3. **High-Throughput Checkpoint Batching**: Accumulates sync checkpoints in memory and flushes them to `_sync_metadata` in batches of 500 events or every 2 seconds (whichever comes first), boosting maximum sync throughput from ~2k events/sec to 20k+ events/sec.
4. **Dynamic Index Replication**: Fetches and replicates all secondary indexes (compound keys, text indices, unique constraints) from the remote collection to the local target database before copying data.
5. **Race-Condition Prevention**: Captures the starting resume token *before* executing the initial data copy. Once the copy finishes, the Change Stream starts from that token, overlaying modifications made during the copy phase.
6. **DDL Event Propagation**: Monitors `drop`, `rename`, and `invalidate` events, dynamically reflecting structural schema changes (dropping/renaming local collections) to keep collections in perfect sync.
7. **Dynamic Collection Discovery**: Runs a background poll every 60 seconds to detect newly created remote collections, automatically triggering their index replication, initial sync, and change streams with zero downtime.
8. **Active Connection & Stream Health Checks**: Executes deep health checks using `admin().ping()` on both databases, and monitors running change streams. Returns HTTP `503 Service Unavailable` if a database goes offline or if a stream exceeds `MAX_RETRIES` and fails permanently.
9. **Prometheus Metrics lag monitoring**: Exposes real-time sync metrics at `/metrics` in standard Prometheus text format, including a gauge `mongodb_sync_lag_seconds` per collection, ready to be scraped and set up for alerts.
10. **Database Security Hardening**: The local MongoDB target is protected via username/password root authentication and its host port is bound strictly to `127.0.0.1` to prevent unauthorized public interface access.
11. **Daily Log Rotation & Gzip Compression**: Organizes logs into daily folders (`combined/`, `errors/`, `operations/`), automatically compresses the previous day's log to `.log.gz` asynchronously, enforces a 60-day retention cleanup, and applies Docker daemon log size capping (30MB max per container).

---

## Directory Structure

```
.
├── docker-compose.yml         # spins up local hardened mongo + sync service
├── .env.example               # copy to .env and fill in your values
├── README.md
└── mongodb-sync/              # sync service source
    ├── app.js                   # entry point + graceful shutdown
    ├── oplog-sync-service.js    # Change Stream, initial sync & index engine
    ├── sync-manager.js          # per-collection metadata tracker
    ├── health-server.js         # active /health and /metrics endpoints
    ├── logger.js                # daily rotating + gzipped winston logs
    ├── Dockerfile               # production container (non-root, healthcheck)
    ├── package.json
    └── test/
        └── sync-service.test.js # unit tests (node:test)
```

---

## Quick Start

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` to configure your connection strings and database names. Make sure to set a secure `LOCAL_MONGO_ROOT_PASSWORD`.
3. Spin up the containers:
   ```bash
   docker compose up -d --build
   ```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `MONGO_DATABASE_NAME` | Database name to sync | `sync_db` |
| `LOCAL_MONGO_PORT` | Host port for the local MongoDB container | `27017` |
| `MONGO_IMAGE` | Docker image version for the local target MongoDB service | `mongo:7` |
| `LOCAL_MONGO_ROOT_USER` | Admin user for the local hardened MongoDB | `admin` |
| `LOCAL_MONGO_ROOT_PASSWORD` | Admin password for the local hardened MongoDB | *required* |
| `REMOTE_MONGODB_URL` | Remote connection string. Supports `{MONGO_DATABASE_NAME}` placeholder | *required* |
| `LOCAL_MONGO_URL` | Local connection string. Supports `{LOCAL_MONGO_ROOT_USER}`, `{LOCAL_MONGO_ROOT_PASSWORD}`, `{LOCAL_MONGO_PORT}`, and `{MONGO_DATABASE_NAME}` placeholders | *required* |
| `PORT` | HTTP port for the health/metrics server | `3000` |
| `LOG_LEVEL` | Winston log level (`debug`, `info`, `warn`, `error`) | `info` |
| `MAX_RETRIES` | Max retry attempts per collection on stream error | `10` |
| `RETRY_DELAY_MS` | Delay between retries in milliseconds | `5000` |

---

## Verification & Monitoring

Check the status of the sync service and database connectivity:
```bash
curl http://localhost:8080/health
curl http://localhost:8080/metrics
docker compose logs -f sync
```

---

## Running Tests

```bash
cd mongodb-sync
npm install
npm test
```

