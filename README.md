# MongoDB Remote → Local Change Stream Sync

Production-grade sync service that mirrors collection changes from a remote
MongoDB (e.g. Atlas) into a local/self-hosted MongoDB instance, in real time,
using **Change Streams**. No `updatedAt` field required — resumption is
tracked purely via Change Stream resume tokens stored in a `_sync_metadata`
collection on the target database.

## Structure

```
.
├── docker-compose.yml         # spins up local mongo + sync service
├── .env.example               # copy to .env and fill in your values
├── README.md
└── mongodb-sync/              # sync service source
    ├── app.js                   # entry point + graceful shutdown
    ├── oplog-sync-service.js    # Change Stream engine + retry logic
    ├── sync-manager.js          # per-collection resume-token state
    ├── health-server.js         # /health and /metrics endpoints
    ├── logger.js                # winston logger (console + rotating files)
    ├── Dockerfile               # production container (non-root, healthcheck)
    ├── package.json
    └── test/
        └── sync-service.test.js # unit tests (node:test)
```

## Quick Start

```bash
cp .env.example .env
# edit .env — set REMOTE_MONGODB_URL credentials and MONGO_DATABASE_NAME
docker compose up -d --build
```

The `mongo` container is the local sync target. The `sync` service connects
to it automatically over the Docker network.

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `MONGO_DATABASE_NAME` | Database name to sync (used by both remote and local URLs) | `sync_db` |
| `LOCAL_MONGO_PORT` | Host port for the local MongoDB container | `27017` |
| `REMOTE_MONGODB_URL` | Remote MongoDB connection string. Supports `{MONGO_DATABASE_NAME}` placeholder | *required* |
| `LOCAL_MONGO_URL` | Local MongoDB connection string. Supports `{LOCAL_MONGO_PORT}` and `{MONGO_DATABASE_NAME}` placeholders | *required* |
| `PORT` | HTTP port for the health/metrics server | `3000` |
| `LOG_LEVEL` | Winston log level (`debug`, `info`, `warn`, `error`) | `info` |
| `MAX_RETRIES` | Max retry attempts per collection on stream error | `10` |
| `RETRY_DELAY_MS` | Delay between retries in milliseconds | `5000` |

### Placeholder Interpolation

Connection string values support `{PLACEHOLDER}` syntax that is resolved at
runtime from other environment variables:

```env
MONGO_DATABASE_NAME=myapp
LOCAL_MONGO_PORT=27017
REMOTE_MONGODB_URL=mongodb+srv://user:pass@cluster.mongodb.net/{MONGO_DATABASE_NAME}?retryWrites=true&w=majority
LOCAL_MONGO_URL=mongodb://localhost:{LOCAL_MONGO_PORT}/{MONGO_DATABASE_NAME}
```

> **Note:** Inside Docker Compose, `LOCAL_MONGO_URL` is overridden to
> `mongodb://mongo:27017/${MONGO_DATABASE_NAME}` so the sync service
> connects to the `mongo` container, not `localhost`.

## Verify It's Working

```bash
curl http://localhost:8080/health
curl http://localhost:8080/metrics
docker compose logs -f sync
```

## Running Tests

```bash
cd mongodb-sync
npm install
npm test
```

## How It Works

1. **Startup**: Connects to both databases, discovers all user collections
   on the remote database (excluding `_`-prefixed and `system.profile`
   collections), and opens a Change Stream per collection.
2. **Processing**: Each change event is applied to the local target:
   - `insert` → `replaceOne` with upsert (idempotent)
   - `update`/`replace` → `replaceOne` with `fullDocument: 'updateLookup'`;
     if `fullDocument` is null (document deleted before lookup), treated as
     a delete
   - `delete` → `deleteOne` by `documentKey._id`
3. **Resume tokens**: After each successful write, the resume token is
   persisted to `_sync_metadata`. On restart, each stream resumes exactly
   where it left off.
4. **Error handling**: Streams auto-reconnect with capped retries and
   configurable delay. Expired resume tokens are detected and cleared
   automatically (stream restarts from the current time).
5. **Graceful shutdown**: `SIGTERM`/`SIGINT` close all Change Streams and
   database connections cleanly.

## Production Notes

- **Remote MongoDB tier**: Change Streams require a replica set. On Atlas,
  this works on M10+ dedicated tiers and serverless instances. It does
  **not** work on the M0 free shared tier.
- **Network access**: The remote cluster must allow inbound connections from
  wherever this container runs (Atlas IP Access List / VPC peering).
- **Initial data**: Change Streams only capture changes *from the moment
  they start*. If the local database needs existing data, run
  `mongodump`/`mongorestore` before the first start — otherwise only new
  writes will be mirrored.
- **New collections**: Streams are opened per collection at startup. If a
  new collection is added to the remote database later, restart the `sync`
  container to pick it up.
- **Deleted-before-lookup**: If a document is deleted between the change
  event and the `fullDocument` lookup, it is treated as a delete rather
  than erroring. Check `operations.log` if you see unexpected removals.
