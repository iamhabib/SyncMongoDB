# MongoDB Remote → Local Change Stream Sync

Sync service that mirrors collection changes from a remote MongoDB (e.g. Atlas) into a local/self-hosted MongoDB instance in near real time, using **Change Streams** plus an automatic **initial sync**. Progress is tracked via Change Stream resume tokens and keyset pagination metadata in the `_sync_metadata` collection on the target database.

> **Requirement:** the remote deployment must support Change Streams (replica set or sharded cluster / Atlas). Standalone MongoDB is not supported as a source. Change Streams are backed by the replica-set oplog; you do not configure “oplog tailing” separately in this app.

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

1. Copy this project into an install directory on the host (one copy per database you sync).
2. Configure and start:
  ```bash
   cp .env.example .env
   # edit .env — set remote URI, DB name, passwords, and unique ports if multiple installs share one host
   docker compose up -d --build
  ```

### Multiple databases on one host

Use **separate install folders**. Compose project name follows the directory name, so containers do not clash. Only **host ports** must be unique per install:


|                    | Install A            | Install B            |
| ------------------ | -------------------- | -------------------- |
| Folder             | `/var/www/sync-db-a` | `/var/www/sync-db-b` |
| `LOCAL_MONGO_PORT` | `27018`              | `27019`              |
| `SYNC_AGENT_PORT`  | `3001`               | `3002`               |


```bash
cd /var/www/sync-db-a && docker compose up -d --build
cd /var/www/sync-db-b && docker compose up -d --build
```

Compass (from your PC, after Security Group allows your IP on that Mongo port):

```text
mongodb://admin:PASSWORD@HOST_PUBLIC_IP:LOCAL_MONGO_PORT/MONGO_DATABASE_NAME?authSource=admin
```

---

## Remote database access (Change Streams / oplog)

This service reads the remote DB with the official driver **Change Stream** API (`db.watch()`). On replica sets and Atlas, that is powered by the **oplog**. You configure a remote user and network access — you do not enable a separate “oplog sync” flag in this repo.

### 1. Cluster / topology

- **Atlas**: any cluster (replica set / sharded) supports Change Streams.
- **Self-hosted**: must be a **replica set** (or sharded). Standalone `mongod` cannot be the source.

Keep the oplog window large enough for your worst outage (if the resume token falls off the oplog, this service does a **full re-sync**).

### 2. Network access

- **Atlas → Network Access**: allow the **sync host’s public IP** (or VPC peering / private endpoint if you use those).
- Do not use `0.0.0.0/0` in production unless you accept the risk.

### 3. Database user (least privilege)

Create a dedicated sync user on the remote cluster. Minimum useful privileges for this app:


| Need                                                       | Why                           |
| ---------------------------------------------------------- | ----------------------------- |
| `find` / `changeStream` (or `read`) on the synced database | Initial copy + Change Streams |
| `listCollections` on that database                         | Discovery of collections      |
| `listIndexes` (included with typical read roles)           | Index replication             |


**Atlas UI:** Database Access → Add user → built-in role `**read`** on the target database (or `readAnyDatabase` only if you must sync many DBs with one user — this app syncs one `MONGO_DATABASE_NAME` per install).

**Self-hosted example** (run as admin on the remote cluster):

```javascript
use admin
db.createUser({
  user: "sync_reader",
  pwd: "STRONG_PASSWORD",
  roles: [
    { role: "read", db: "YOUR_DATABASE_NAME" }
  ]
})
```

If you also need to read from multiple databases with one user:

```javascript
db.createUser({
  user: "sync_reader",
  pwd: "STRONG_PASSWORD",
  roles: [ { role: "readAnyDatabase", db: "admin" } ]
})
```

URL-encode special characters in the password when putting it in `REMOTE_MONGODB_URL`.

### 4. Connection string

Set in `.env`:

```env
MONGO_DATABASE_NAME=YOUR_DATABASE_NAME
REMOTE_MONGODB_URL=mongodb+srv://sync_reader:ENCODED_PASSWORD@cluster.mongodb.net/{MONGO_DATABASE_NAME}?retryWrites=true&w=majority
```

For a self-hosted replica set (non-SRV):

```env
REMOTE_MONGODB_URL=mongodb://sync_reader:ENCODED_PASSWORD@host1:27017,host2:27017,host3:27017/{MONGO_DATABASE_NAME}?replicaSet=rs0&authSource=admin
```

### 5. Quick verify from the sync host

```bash
# optional: mongosh against Atlas/remote with the sync user
mongosh "$REMOTE_MONGODB_URL" --eval 'db.runCommand({ ping: 1 })'
```

Then start Compose and confirm `/health` is healthy and collections appear locally (`./monitor.sh` option 8).

---

## Environment Variables

**Required** (per install `.env`):


| Variable                    | Description                                                         |
| --------------------------- | ------------------------------------------------------------------- |
| `MONGO_DATABASE_NAME`       | Database name to sync                                               |
| `LOCAL_MONGO_PORT`          | Host Mongo port — **unique per install on the same host**           |
| `SYNC_AGENT_PORT`           | Sync `/health` & `/metrics` on `127.0.0.1` — **unique per install** |
| `LOCAL_MONGO_ROOT_USER`     | Local Mongo root user                                               |
| `LOCAL_MONGO_ROOT_PASSWORD` | Local Mongo root password                                           |
| `REMOTE_MONGODB_URL`        | Remote/Atlas URI (`{MONGO_DATABASE_NAME}` ok)                       |


`LOCAL_MONGO_URL` is set by Compose to `mongo:27017` inside that install’s network.

Optional vars (`LOG_LEVEL`, `LOG_REPLICATION_EVENTS`, retries, divergence repair, etc.) are listed with defaults in `[.env.example](./.env.example)`.

---

## Monitoring (`monitor.sh`)

Interactive helper for live sync watch, logs, health/metrics, and local DB queries. Uses `.env` (`SYNC_AGENT_PORT`, Mongo credentials).

```bash
chmod +x monitor.sh
./monitor.sh
```


| Option | What it does                                                                                   |
| ------ | ---------------------------------------------------------------------------------------------- |
| `1`    | Tail sync container stdout/stderr                                                              |
| `2`    | Tail today's log under `logs/combined/`                                                        |
| `3`    | Tail today's error log                                                                         |
| `4`    | `GET /health` JSON                                                                             |
| `5`    | `GET /metrics` once (Prometheus)                                                               |
| `6`    | **Watch live sync counters** — polls health; `totalSynced` rises when source changes replicate |
| `7`    | Watch replication log lines (set `LOG_REPLICATION_EVENTS=true` in `.env`, recreate sync)       |
| `8`    | **Query LOCAL DB only** — against this install’s `mongo` service. Never the remote source.     |
| `9`    | Exit                                                                                           |


To see each INSERT/UPDATE/DELETE in option `7`:

```bash
# in .env
LOG_REPLICATION_EVENTS=true
docker compose up -d sync
./monitor.sh   # choose 7
```

Equivalent manual checks (use your install’s `SYNC_AGENT_PORT`):

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

Creates a **gzip-compressed** `mongodump` archive of the **local synced** DB.

```bash
cd /path/to/this-install
chmod +x db-dump.sh
./db-dump.sh
```

Output example:

```text
dumps/<folder>-<db>-<timestamp>.archive.gz
```

Copy or SFTP that file to the machine where you will run `mongorestore` (your laptop, another EC2, etc.).

### Restore to MongoDB Atlas (or any remote host)

Works from **any terminal** that has [MongoDB Database Tools](https://www.mongodb.com/docs/database-tools/installation/) (`mongorestore`) installed — local PC or EC2.

**1. Allow this machine’s public IP** on the remote cluster  
- Atlas → **Network Access** → Add IP Address → IP of the PC/EC2 running `mongorestore`  
- Wait until the entry is Active  

**2. Use a remote user that can write** (e.g. `readWrite` on the target DB, or Atlas `atlasAdmin` / custom role). A sync **read-only** user cannot restore.

**3. Restore with a full URI** (host is inside the URI — no separate `--host` needed for `mongodb+srv`):

```bash
# Atlas (SRV) — replace USER, PASSWORD, CLUSTER, and archive path
mongorestore \
  --uri='mongodb+srv://USER:PASSWORD@CLUSTER.mongodb.net/?retryWrites=true&w=majority' \
  --gzip \
  --archive=./dumps/your-file.archive.gz \
  --nsInclude='SOURCE_DB.*' \
  --nsFrom='SOURCE_DB.*' \
  --nsTo='TARGET_DB.*'
```

- `SOURCE_DB` = database name inside the dump (usually your `MONGO_DATABASE_NAME` when dumped).  
- `TARGET_DB` = database name on Atlas (same as source, or a new name).  
- If restoring into the **same** DB name as in the dump, you can omit `--nsFrom` / `--nsTo` and use:

```bash
mongorestore \
  --uri='mongodb+srv://USER:PASSWORD@CLUSTER.mongodb.net/TARGET_DB?retryWrites=true&w=majority' \
  --gzip \
  --archive=./dumps/your-file.archive.gz
```

**Self-hosted / non-SRV remote** (explicit hosts + port):

```bash
mongorestore \
  --uri='mongodb://USER:PASSWORD@host1:27017,host2:27017,host3:27017/TARGET_DB?replicaSet=rs0&authSource=admin' \
  --gzip \
  --archive=./dumps/your-file.archive.gz
```

Or with separate flags:

```bash
mongorestore \
  --host='host1:27017,host2:27017,host3:27017' \
  --username='USER' \
  --password='PASSWORD' \
  --authenticationDatabase=admin \
  --db='TARGET_DB' \
  --gzip \
  --archive=./dumps/your-file.archive.gz
```

**Tips**

- URL-encode special characters in the password (`@` → `%40`, etc.).  
- Drop or clear `TARGET_DB` first if you need a clean replace (`mongosh` → `use TARGET_DB` → `db.dropDatabase()`), or use `--drop` to drop collections before restore (destructive).  
- To verify: `mongosh 'mongodb+srv://...' --eval 'db.getSiblingDB("TARGET_DB").stats()'`  
- `_sync_metadata` from the local sync agent is included in the dump; drop that collection on Atlas after restore if you do not want sync bookkeeping there.

### Restore to another local Mongo only

```bash
mongorestore \
  --uri='mongodb://admin:PASSWORD@127.0.0.1:27018/TARGET_DB?authSource=admin' \
  --gzip \
  --archive=./dumps/your-file.archive.gz
```

---

## Operational notes

- Collections whose names start with `_` (including `_sync_metadata`) and `system.*` are not synced from the remote.
- After a long outage, if the change stream resume token has fallen off the oplog, expect a **full re-sync** (local user collections dropped and recopied). Size capacity and oplog window accordingly.
- Prefer keeping `AUTO_REPAIR_ON_DIVERGENCE=false` unless you accept periodic full collection rebuilds when counts disagree (counts alone are a coarse signal).
- Prefer `./db-dump.sh` for portable backups (compressed archive). Copying raw `data/` WiredTiger files only works with the same Mongo major version and a clean stop.
- On one host with multiple installs, use different `LOCAL_MONGO_PORT` and `SYNC_AGENT_PORT` in each `.env`.

