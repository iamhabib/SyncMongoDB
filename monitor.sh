#!/usr/bin/env bash
# monitor.sh — logs, live sync watch, health/metrics, and local DB queries
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

SERVICE_PORT=${SYNC_AGENT_PORT:-${PORT:-3000}}
DB_NAME=${MONGO_DATABASE_NAME:-sync_db}
MONGO_USER=${LOCAL_MONGO_ROOT_USER:-admin}
MONGO_PASS=${LOCAL_MONGO_ROOT_PASSWORD:-}
INSTALL_LABEL="$(basename "$ROOT_DIR")"

mongosh_eval() {
  local eval_js="$1"
  if [[ -z "$MONGO_PASS" ]]; then
    echo "LOCAL_MONGO_ROOT_PASSWORD is empty in .env"
    return 1
  fi
  docker compose exec -T mongo mongosh \
    -u "$MONGO_USER" \
    -p "$MONGO_PASS" \
    --authenticationDatabase admin \
    --quiet \
    "$DB_NAME" \
    --eval "$eval_js"
}

watch_sync_counters() {
  echo "Watching live sync counters from /health (Ctrl+C to stop)..."
  echo "When Atlas/source changes are applied, totalSynced should increase."
  echo "---------------------------------------------------------------"
  local prev=""
  while true; do
    local json
    json="$(curl -sf "http://127.0.0.1:${SERVICE_PORT}/health" 2>/dev/null || echo '')"
    if [[ -z "$json" ]]; then
      echo "[$(date '+%H:%M:%S')] health unavailable — is sync running on port ${SERVICE_PORT}?"
      sleep 3
      continue
    fi
    if command -v jq >/dev/null 2>&1; then
      local line
      line="$(echo "$json" | jq -r '
        "status=\(.status // "n/a") collections=\(.connectedCollections)//\(.expectedCollectionsCount)
         failed=\(.failedStreams|length)
         " + (
           (.syncMetrics // [])
           | map(select(.collection|startswith("_")|not))
           | map("\(.collection):\(.totalSynced // 0)")
           | join(" ")
         ) + " divergences=" + ((.divergences // {}) | tojson)
      ')"
      if [[ "$line" != "$prev" ]]; then
        echo "[$(date '+%H:%M:%S')] $line"
        prev="$line"
      else
        echo "[$(date '+%H:%M:%S')] (no change) $line"
      fi
    else
      echo "[$(date '+%H:%M:%S')] (install jq for a clearer view)"
      echo "$json" | head -c 400
      echo
    fi
    sleep 2
  done
}

watch_replication_logs() {
  echo "=============================================================="
  echo " Live replication log stream (INSERT / UPDATE / DELETE / …)"
  echo "=============================================================="
  echo "Per-document events need LOG_REPLICATION_EVENTS=true or LOG_LEVEL=debug"
  echo "in .env, then: docker compose up -d sync"
  echo "Press Ctrl+C to stop."
  echo "--------------------------------------------------------------"
  docker compose logs -f --tail=50 sync 2>/dev/null | grep -E --line-buffered \
    'INSERT|UPDATE|DELETE|DROPPED|RENAMED|Initial Sync|Copied|Change Stream|Divergence|re-sync|discovery|ERROR|WARN|Failed' \
    || docker compose logs -f --tail=50 sync
}

query_menu() {
  echo
  echo "========== LOCAL DB only (synced copy on this host) =========="
  echo "Install: ${INSTALL_LABEL}  |  service: mongo  |  DB: ${DB_NAME}"
  echo "This does NOT query Atlas/remote. Only data already replicated here."
  echo "--------------------------------------------------------------"
  echo "1) List collections + counts"
  echo "2) Show _sync_metadata"
  echo "3) Sample documents from a collection (find limit)"
  echo "4) Count documents in a collection"
  echo "5) Custom mongosh --eval (JavaScript)"
  echo "6) Back / exit"
  read -r -p "Query choice [1-6]: " qchoice

  case "$qchoice" in
    1)
      mongosh_eval '
        db.getCollectionNames()
          .filter(c => !c.startsWith("system."))
          .sort()
          .forEach(c => print(c + ": " + db.getCollection(c).countDocuments()))
      '
      ;;
    2)
      mongosh_eval 'printjson(db._sync_metadata.find().toArray())'
      ;;
    3)
      read -r -p "Collection name: " col
      read -r -p "Limit [5]: " lim
      lim="${lim:-5}"
      mongosh_eval "printjson(db.getCollection('${col}').find().limit(${lim}).toArray())"
      ;;
    4)
      read -r -p "Collection name: " col
      mongosh_eval "print('${col}: ' + db.getCollection('${col}').countDocuments())"
      ;;
    5)
      echo "Example: db.users.find().limit(2).toArray()"
      read -r -p "JS to eval: " js
      mongosh_eval "printjson((() => { const r = (${js}); return r; })())"
      ;;
    6)
      echo "Done."
      ;;
    *)
      echo "Invalid choice."
      exit 1
      ;;
  esac
}

clear
echo "=========================================================="
echo "          MongoDB Sync Service Monitor CLI               "
echo "=========================================================="
echo "INSTANCE: ${INSTALL_LABEL}   DB: ${DB_NAME}   health: :${SERVICE_PORT}"
echo "----------------------------------------------------------"
echo "1) Tail live container console logs (stdout/stderr)"
echo "2) Tail daily combined log file (LOGS/combined)"
echo "3) Tail daily error log file (LOGS/errors)"
echo "4) Check service health API (JSON)"
echo "5) View Prometheus metrics once"
echo "6) Watch live sync counters (recommended — see replication)"
echo "7) Watch replication log lines (needs LOG_REPLICATION_EVENTS=true)"
echo "8) Query LOCAL DB only (synced data — not Atlas/remote)"
echo "9) Exit"
echo "=========================================================="
echo "Note: Option 2 alone usually will NOT show each replicated"
echo "doc while LOG_LEVEL=info. Use 6 (or 7 with replication logs)."
echo "Option 8 always hits local mongo container, never remote."
echo "=========================================================="
read -r -p "Enter choice [1-9]: " choice

case $choice in
  1)
    echo "Tailing live container console logs (Press Ctrl+C to stop)..."
    docker compose logs -f sync
    ;;
  2)
    DATE_STR=$(date +%Y-%m-%d)
    echo "Tailing daily combined log for $DATE_STR (Ctrl+C to stop)..."
    echo "(Per-doc replication is debug-level — prefer option 6 or 7.)"
    docker compose exec sync sh -c "tail -f /app/logs/combined/$DATE_STR.log 2>/dev/null || echo 'No log file found yet for today ($DATE_STR).'"
    ;;
  3)
    DATE_STR=$(date +%Y-%m-%d)
    echo "Tailing daily error log for $DATE_STR (Ctrl+C to stop)..."
    docker compose exec sync sh -c "tail -f /app/logs/errors/$DATE_STR.log 2>/dev/null || echo 'No error log file found for today ($DATE_STR).'"
    ;;
  4)
    echo "Fetching http://127.0.0.1:$SERVICE_PORT/health ..."
    if command -v jq &> /dev/null; then
      curl -s "http://127.0.0.1:$SERVICE_PORT/health" | jq .
    else
      curl -s "http://127.0.0.1:$SERVICE_PORT/health"
      echo -e "\nNote: Install 'jq' for formatted JSON."
    fi
    ;;
  5)
    echo "Fetching http://127.0.0.1:$SERVICE_PORT/metrics ..."
    curl -s "http://127.0.0.1:$SERVICE_PORT/metrics"
    ;;
  6)
    watch_sync_counters
    ;;
  7)
    watch_replication_logs
    ;;
  8)
    query_menu
    ;;
  9)
    echo "Exiting."
    exit 0
    ;;
  *)
    echo "Invalid option. Exiting."
    exit 1
    ;;
esac
