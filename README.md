# MongoDB Remote → Local Change Stream Sync

Sync service that mirrors collection changes from a remote MongoDB (e.g. Atlas) into a local/self-hosted MongoDB instance in near real time, using **Change Streams** plus an automatic **initial sync**. Progress is tracked via Change Stream resume tokens and keyset pagination metadata in the `_sync_metadata` collection on the target database.

> **Requirement:** the remote deployment must support Change Streams (replica set or sharded cluster / Atlas). Standalone MongoDB is not supported as a source.

## Architecture & Production Features

1. **Database-level Change Stream**: One `db.watch()` stream so multi-collection transactions stay in causal order locally.
2. **Automated Initial Sync**: Unsynced collections are copied in `_id` keyset batches before (or with the stream paused around) live apply.
3. **Resumable Keyset Batching**: Initial sync uses `{ _id: { $gt: lastId } }` and checkpoints `lastCopiedId` so restarts continue mid-copy.
4. **Checkpoint Batching**: Per-collection checkpoints and the DB-level resume token are buffered in memory and flushed every 500 events or 2 seconds to keep the hot path off the metadata collection.
5. **Resume-token expiry recovery**: If the resume point leaves the oplog (or the stream is invalidated), the service clears state, drops local user collections, re-copies from a fresh token, then restarts the stream — it does not silently skip the gap.
6. **Safe dynamic discovery**: New remote collections pause the stream, capture a resume token, run initial sync, then restart from that token so copy and live events do not race.
7. **DDL propagation**: Handles `drop` / `rename` / `invalidate` and updates local metadata accordingly.
8. **Periodic reconciliation**: Every 6 hours (configurable) compares remote vs local document counts and re-applies indexes (create missing, drop obsolete). Count divergence is **detect-only** by default; set `AUTO_REPAIR_ON_DIVERGENCE=true` to drop and fully re-sync drifted collections.
9. **Health & metrics**: `/health` (JSON) and `/metrics` (Prometheus). Status can be `healthy`, `syncing`, `degraded`, `initializing`, or `unhealthy`.
10. **Hardened local target**: Root auth required (no compose password default). Mongo and health ports bind to `127.0.0.1` on the host.
11. **Daily log rotation**: `combined/` and `errors/` under the log volume, gzip of prior days, 60-day retention, Docker json-file caps (10m × 3).

---

## Directory Structure

```
.
├── docker-compose.yml
├── .env.example
├── README.md
├── monitor.sh
└── mongodb-sync/
    ├── app.js
    ├── oplog-sync-service.js
    ├── sync-manager.js
    ├── health-server.js
    ├── logger.js
    ├── Dockerfile
    ├── package.json
    └── test/
        └── sync-service.test.js
```

---

## Quick Start

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env`: set a strong `LOCAL_MONGO_ROOT_PASSWORD` and your `REMOTE_MONGODB_URL` / database name.
3. Start:
   ```bash
   docker compose up -d --build
   ```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `MONGO_DATABASE_NAME` | Database name to sync | `sync_db` |
| `LOCAL_MONGO_PORT` | Host port for local MongoDB | `27017` |
| `MONGO_IMAGE` | Local MongoDB image | `mongo:7` |
| `LOCAL_MONGO_ROOT_USER` | Local Mongo root user | *required* |
| `LOCAL_MONGO_ROOT_PASSWORD` | Local Mongo root password | *required* |
| `REMOTE_MONGODB_URL` | Remote connection string (`{MONGO_DATABASE_NAME}` ok) | *required* |
| `LOCAL_MONGO_URL` | Local connection string (placeholders supported) | *required* |
| `PORT` | Health/metrics port | `3000` |
| `HEALTH_BIND_HOST` | Bind address inside the container (`0.0.0.0` in Compose) | `127.0.0.1` |
| `LOG_LEVEL` | Winston level | `info` |
| `MAX_RETRIES` | Retries for transient stream errors before marking failed | `10` |
| `RETRY_DELAY_MS` | Delay between retries | `5000` |
| `DISCOVERY_INTERVAL_MS` | New-collection poll interval | `60000` |
| `DIVERGENCE_CHECK_INTERVAL_MS` | Count + index reconciliation interval | `21600000` (6h) |
| `AUTO_REPAIR_ON_DIVERGENCE` | If `true`, re-sync collections whose count drift ≥ threshold | `false` |
| `DIVERGENCE_REPAIR_THRESHOLD` | Minimum `|remote−local|` count to trigger auto-repair | `1` |

---

## Verification & Monitoring

```bash
./monitor.sh
```

Or manually:

```bash
curl http://127.0.0.1:3000/health
curl http://127.0.0.1:3000/metrics
docker compose logs -f sync
```

---

## Running Tests

```bash
cd mongodb-sync
npm ci
npm test
```

---

## Operational notes

- Collections whose names start with `_` (including `_sync_metadata`) and `system.*` are not synced from the remote.
- After a long outage, if the change stream resume token has fallen off the oplog, expect a **full re-sync** (local user collections dropped and recopied). Size capacity and oplog window accordingly.
- Prefer keeping `AUTO_REPAIR_ON_DIVERGENCE=false` unless you accept periodic full collection rebuilds when counts disagree (counts alone are a coarse signal).
