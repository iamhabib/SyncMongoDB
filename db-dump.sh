#!/usr/bin/env bash
# db-dump.sh — compressed mongodump of the local synced database (for SFTP / mongorestore)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

DB_NAME="${MONGO_DATABASE_NAME:?Set MONGO_DATABASE_NAME in .env}"
MONGO_USER="${LOCAL_MONGO_ROOT_USER:?Set LOCAL_MONGO_ROOT_USER in .env}"
MONGO_PASS="${LOCAL_MONGO_ROOT_PASSWORD:?Set LOCAL_MONGO_ROOT_PASSWORD in .env}"
DUMP_DIR="${DUMP_DIR:-dumps}"
INSTALL_LABEL="$(basename "$ROOT_DIR")"
STAMP="$(date +%Y%m%d-%H%M%S)"
ARCHIVE_NAME="${INSTALL_LABEL}-${DB_NAME}-${STAMP}.archive.gz"
ARCHIVE_PATH="${DUMP_DIR}/${ARCHIVE_NAME}"
CONTAINER_TMP="/tmp/${ARCHIVE_NAME}"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}==>${NC} $*"; }
err()  { echo -e "${RED}Error:${NC} $*" >&2; }

if ! docker compose ps --status running 2>/dev/null | grep -q mongo; then
  err "Mongo service is not running. Start it first: docker compose up -d mongo"
  exit 1
fi

mkdir -p "$DUMP_DIR"

info "Dumping database '${DB_NAME}' (gzip archive)..."
info "Output: ${ARCHIVE_PATH}"

# Write archive inside the container, then copy out (safe for binary gzip)
docker compose exec -T mongo mongodump \
  -u "$MONGO_USER" \
  -p "$MONGO_PASS" \
  --authenticationDatabase admin \
  --db "$DB_NAME" \
  --gzip \
  --archive="$CONTAINER_TMP"

docker compose cp "mongo:${CONTAINER_TMP}" "$ARCHIVE_PATH"
docker compose exec -T mongo rm -f "$CONTAINER_TMP"

SIZE="$(du -h "$ARCHIVE_PATH" | awk '{print $1}')"
ok "Dump complete: ${ARCHIVE_PATH} (${SIZE})"
echo
echo "Next steps (your choice):"
echo "  1) SFTP/SCP the file from:"
echo "       ${ROOT_DIR}/${ARCHIVE_PATH}"
echo "  2) On any machine with mongorestore + this file, allow that machine's IP in Atlas"
echo "     Network Access, then restore, e.g.:"
echo "       mongorestore --uri='mongodb+srv://USER:PASS@CLUSTER.mongodb.net/?retryWrites=true&w=majority' \\"
echo "         --gzip --archive=${ARCHIVE_NAME}"
echo "     See README.md → Database dump for full Atlas / remote examples."
echo
