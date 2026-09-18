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
10. **Hardened local target**: Root auth required (no compose password default). Health stays on `127.0.0.1`; Mongo is published on the host port — lock it down with EC2 Security Group (your PC IP only), not `0.0.0.0/0`.
11. **Daily log rotation**: `combined/` and `errors/` under the log volume, gzip of prior days, 60-day retention, Docker json-file caps (10m × 3).

---

## Directory Structure

```
.
├── docker-compose.yml
├── .env.example
├── README.md
├── db-dump.sh               # compressed mongodump of local synced DB
├── monitor.sh
├── data/                    # Mongo data for this install (gitignored)
├── logs/                    # sync logs for this install (gitignored)
├── dumps/                   # .archive.gz dumps from db-dump.sh (gitignored)
└── mongodb-sync/
    └── ...
```

---

## Quick Start

1. Deploy a copy of this repo per database, e.g.:
   - `/var/www/db-retail`
   - `/var/www/db-pass`
2. In each folder:
   ```bash
   cp .env.example .env
   # edit .env — use unique LOCAL_MONGO_PORT and SYNC_AGENT_PORT on the same EC2
   docker compose up -d --build
   ```

### Multiple databases on one EC2

Use **separate folders** (recommended). Compose project name follows the directory (`db-retail`, `db-pass`), so containers do not clash. Only **host ports** must be unique:

| Install path | `LOCAL_MONGO_PORT` | `SYNC_AGENT_PORT` |
|--------------|--------------------|-----------------|
| `/var/www/db-retail` | `27018` | `3001` |
| `/var/www/db-pass` | `27019` | `3002` |

```bash
cd /var/www/db-retail && docker compose up -d --build
cd /var/www/db-pass   && docker compose up -d --build
```

Compass example (retail):

```text
mongodb://admin:PASSWORD@EC2_PUBLIC_IP:27018/DB_NAME?authSource=admin
```

Allow each Mongo port in the Security Group from your PC IP only.

---

## Environment Variables

**Required** (per install `.env`):

| Variable | Description |
|---|---|
| `MONGO_DATABASE_NAME` | Database name to sync |
| `LOCAL_MONGO_PORT` | Host Mongo port — **unique per install on this EC2** |
| `SYNC_AGENT_PORT` | Sync `/health` & `/metrics` on `127.0.0.1` — **unique per install** |
| `LOCAL_MONGO_ROOT_USER` | Local Mongo root user |
| `LOCAL_MONGO_ROOT_PASSWORD` | Local Mongo root password |
| `REMOTE_MONGODB_URL` | Atlas/source URI (`{MONGO_DATABASE_NAME}` ok) |

`LOCAL_MONGO_URL` is set by Compose to `mongo:27017` inside that install’s network.

Optional vars (`LOG_LEVEL`, `LOG_REPLICATION_EVENTS`, retries, divergence repair, etc.) are listed with defaults in [`.env.example`](./.env.example).

---

## Monitoring (`monitor.sh`)

Interactive helper for live sync watch, logs, health/metrics, and local DB queries. Uses `.env` (`SYNC_AGENT_PORT`, Mongo credentials).

```bash
chmod +x monitor.sh
./monitor.sh
```

| Option | What it does |
|--------|----------------|
| `1` | Tail sync container stdout/stderr |
| `2` | Tail today's log under `logs/combined/` |
| `3` | Tail today's error log |
| `4` | `GET /health` JSON |
| `5` | `GET /metrics` once (Prometheus) |
| `6` | **Watch live sync counters** — polls health; `totalSynced` rises when source changes replicate |
| `7` | Watch replication log lines (set `LOG_REPLICATION_EVENTS=true` in `.env`, recreate sync) |
| `8` | **Query LOCAL DB only** — against this install’s `mongo` service. Never Atlas/remote. |
| `9` | Exit |

To see each INSERT/UPDATE/DELETE in option `7`:

```bash
# in .env
LOG_REPLICATION_EVENTS=true
docker compose up -d sync
./monitor.sh   # choose 7
```

Equivalent manual checks (use your install's `SYNC_AGENT_PORT`):

```bash
curl http://127.0.0.1:3001/health
curl http://127.0.0.1:3001/metrics
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

## Database dump (`db-dump.sh`)

Creates a **gzip-compressed** `mongodump` archive of the local synced DB (small enough to SFTP, restorable with `mongorestore`).

```bash
cd /var/www/db-retail   # your install folder
chmod +x db-dump.sh
./db-dump.sh
```

Output example:

```text
dumps/db-retail-your_db_name-20260918-163000.archive.gz
```

**Download** the file with SFTP/SCP from the EC2 host, then **restore** elsewhere:

```bash
mongorestore -u admin -p 'PASSWORD' --authenticationDatabase admin \
  --gzip --archive=./db-retail-your_db_name-20260918-163000.archive.gz
```

Optional: restore into a different database name:

```bash
mongorestore -u admin -p 'PASSWORD' --authenticationDatabase admin \
  --gzip --archive=./file.archive.gz --nsFrom='old_db.*' --nsTo='new_db.*'
```

---

## Operational notes

- Collections whose names start with `_` (including `_sync_metadata`) and `system.*` are not synced from the remote.
- After a long outage, if the change stream resume token has fallen off the oplog, expect a **full re-sync** (local user collections dropped and recopied). Size capacity and oplog window accordingly.
- Prefer keeping `AUTO_REPAIR_ON_DIVERGENCE=false` unless you accept periodic full collection rebuilds when counts disagree (counts alone are a coarse signal).
- Prefer `./db-dump.sh` for portable backups (compressed archive). Copying raw `data/` WiredTiger files only works with the same Mongo major version and a clean stop.
- On one EC2 with multiple installs (`/var/www/db-retail`, `/var/www/db-pass`, …), use different `LOCAL_MONGO_PORT` and `SYNC_AGENT_PORT` in each `.env`.
