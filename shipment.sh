#!/usr/bin/env bash
# shipment.sh — safely package DATA_SOURCE (local MongoDB data dir) and ship it over SSH.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DATA_DIR="${DATA_DIR:-DATA_SOURCE}"
SHIP_DIR="${SHIP_DIR:-shipments}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}==>${NC} $*"; }
ok()    { echo -e "${GREEN}==>${NC} $*"; }
warn()  { echo -e "${YELLOW}Warning:${NC} $*"; }
err()   { echo -e "${RED}Error:${NC} $*" >&2; }

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "Required command not found: $1"
    exit 1
  fi
}

prompt() {
  # usage: prompt VAR "Label" [default]
  local __var="$1"
  local __label="$2"
  local __default="${3-}"
  local __input=""
  if [[ -n "$__default" ]]; then
    read -r -p "$__label [$__default]: " __input || true
    __input="${__input:-$__default}"
  else
    read -r -p "$__label: " __input
  fi
  printf -v "$__var" '%s' "$__input"
}

prompt_secret() {
  local __var="$1"
  local __label="$2"
  local __input=""
  read -r -s -p "$__label: " __input
  echo
  printf -v "$__var" '%s' "$__input"
}

confirm() {
  local reply=""
  read -r -p "$1 [y/N]: " reply || true
  [[ "${reply,,}" == "y" || "${reply,,}" == "yes" ]]
}

build_ssh_base() {
  SSH_BASE=(ssh -p "$REMOTE_PORT" -o StrictHostKeyChecking=accept-new)
  SCP_BASE=(scp -P "$REMOTE_PORT" -o StrictHostKeyChecking=accept-new)
  RSYNC_SSH="ssh -p $REMOTE_PORT -o StrictHostKeyChecking=accept-new"

  if [[ "$AUTH_METHOD" == "key" && -n "${SSH_KEY_PATH:-}" ]]; then
    SSH_BASE+=(-i "$SSH_KEY_PATH")
    SCP_BASE+=(-i "$SSH_KEY_PATH")
    RSYNC_SSH+=" -i $SSH_KEY_PATH"
  fi
}

remote_run() {
  if [[ "$AUTH_METHOD" == "password" ]]; then
    SSHPASS="$REMOTE_PASS" sshpass -e "${SSH_BASE[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "$@"
  else
    "${SSH_BASE[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "$@"
  fi
}

remote_rsync() {
  local src="$1"
  local dest="$2"
  if [[ "$AUTH_METHOD" == "password" ]]; then
    SSHPASS="$REMOTE_PASS" rsync -a --info=progress2 -e "sshpass -e $RSYNC_SSH" "$src" "$dest"
  else
    rsync -a --info=progress2 -e "$RSYNC_SSH" "$src" "$dest"
  fi
}

compose() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" "$@"
  else
    err "docker compose is required to stop/start Mongo safely"
    exit 1
  fi
}

mongo_running() {
  docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'mongo-sync-target'
}

cleanup_archive() {
  if [[ "${KEEP_LOCAL_ARCHIVE:-n}" != "y" && -n "${ARCHIVE_PATH:-}" && -f "${ARCHIVE_PATH:-}" ]]; then
    info "Removing local archive $ARCHIVE_PATH"
    rm -f "$ARCHIVE_PATH"
  fi
}

# --- preflight ---
need_cmd tar
need_cmd ssh
need_cmd rsync
need_cmd docker

if [[ ! -f "$COMPOSE_FILE" ]]; then
  err "Run this from the project root (missing $COMPOSE_FILE)."
  exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
  err "Data directory not found: $SCRIPT_DIR/$DATA_DIR"
  err "Nothing to ship."
  exit 1
fi

echo
echo "=========================================================="
echo "  MongoDB DATA_SOURCE shipment (WiredTiger data directory)"
echo "=========================================================="
echo "  Source: $SCRIPT_DIR/$DATA_DIR"
echo "  This copies the FULL Mongo data dir (not individual .wt files)."
echo "=========================================================="
echo

# --- destination prompts ---
prompt REMOTE_HOST "Remote IP / hostname"
[[ -n "$REMOTE_HOST" ]] || { err "Host is required"; exit 1; }

prompt REMOTE_PORT "SSH port" "22"
prompt REMOTE_USER "SSH username" "ubuntu"

echo
echo "Authentication method:"
echo "  1) SSH private key (recommended)"
echo "  2) Password (requires sshpass)"
read -r -p "Choose [1/2] (default 1): " auth_choice || true
auth_choice="${auth_choice:-1}"

AUTH_METHOD="key"
REMOTE_PASS=""
SSH_KEY_PATH=""

case "$auth_choice" in
  2)
    AUTH_METHOD="password"
    need_cmd sshpass
    prompt_secret REMOTE_PASS "SSH password"
    [[ -n "$REMOTE_PASS" ]] || { err "Password is required for method 2"; exit 1; }
    ;;
  *)
    AUTH_METHOD="key"
    default_key="${HOME}/.ssh/id_ed25519"
    [[ -f "$default_key" ]] || default_key="${HOME}/.ssh/id_rsa"
    if [[ -f "$default_key" ]]; then
      prompt SSH_KEY_PATH "Path to private key" "$default_key"
    else
      prompt SSH_KEY_PATH "Path to private key (leave empty to use ssh-agent/default)"
    fi
    if [[ -n "$SSH_KEY_PATH" && ! -f "$SSH_KEY_PATH" ]]; then
      err "Key file not found: $SSH_KEY_PATH"
      exit 1
    fi
    ;;
esac

prompt REMOTE_PATH "Remote destination directory" "/var/www/SyncMongoDB"
prompt REMOTE_EXTRACT "Extract archive on remote into DATA_SOURCE? (y/n)" "y"

echo
info "Target: ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PORT} → ${REMOTE_PATH}"
if ! confirm "Proceed with shipment?"; then
  warn "Cancelled."
  exit 0
fi

build_ssh_base

# --- connectivity check ---
info "Checking SSH connectivity..."
if ! remote_run "echo ok" >/dev/null; then
  err "Cannot SSH to ${REMOTE_USER}@${REMOTE_HOST}. Fix access and retry."
  exit 1
fi
ok "SSH OK"

# --- stop mongo for a consistent copy ---
STOPPED_MONGO=0
STOPPED_SYNC=0
if mongo_running; then
  warn "Local mongo-sync-target is running. It should be stopped for a consistent copy."
  if confirm "Stop mongo (and sync if present) now?"; then
    info "Stopping sync service (if running)..."
    compose stop sync >/dev/null 2>&1 || true
    STOPPED_SYNC=1
    info "Stopping mongo..."
    compose stop mongo
    STOPPED_MONGO=1
    ok "Mongo stopped"
  else
    if ! confirm "Continue anyway (NOT recommended — risk of corrupt copy)?"; then
      exit 1
    fi
    warn "Continuing while Mongo may still be writing..."
  fi
else
  info "Local mongo container is not running — good for a clean copy."
fi

# --- package ---
mkdir -p "$SHIP_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
ARCHIVE_NAME="DATA_SOURCE-${STAMP}.tar.gz"
ARCHIVE_PATH="$SHIP_DIR/$ARCHIVE_NAME"

info "Creating archive (this can take a while on large DBs)..."
# Avoid packing mongod.lock contention issues; include full tree as-is after stop
tar -C "$SCRIPT_DIR" -czf "$ARCHIVE_PATH" "$DATA_DIR"
ARCHIVE_SIZE="$(du -h "$ARCHIVE_PATH" | awk '{print $1}')"
ok "Archive ready: $ARCHIVE_PATH ($ARCHIVE_SIZE)"

# --- ship ---
info "Ensuring remote directory exists..."
remote_run "mkdir -p $(printf '%q' "$REMOTE_PATH")"

info "Uploading archive via rsync..."
remote_rsync "$ARCHIVE_PATH" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/"
ok "Upload complete"

if [[ "${REMOTE_EXTRACT,,}" == "y" || "${REMOTE_EXTRACT,,}" == "yes" ]]; then
  info "Extracting on remote (existing DATA_SOURCE will be renamed aside if present)..."
  remote_run "bash -s" <<EOF
set -euo pipefail
cd $(printf '%q' "$REMOTE_PATH")
if [ -d DATA_SOURCE ]; then
  mv DATA_SOURCE "DATA_SOURCE.bak-${STAMP}"
  echo "Renamed existing DATA_SOURCE -> DATA_SOURCE.bak-${STAMP}"
fi
tar -xzf $(printf '%q' "$ARCHIVE_NAME")
echo "Extracted to $(printf '%q' "$REMOTE_PATH")/DATA_SOURCE"
EOF
  ok "Remote extract done"
  echo
  info "On the remote host, start Mongo with the same image major (mongo:7) and the same"
  info "LOCAL_MONGO_ROOT_USER / LOCAL_MONGO_ROOT_PASSWORD from your .env"
else
  info "Archive left on remote as: ${REMOTE_PATH}/${ARCHIVE_NAME}"
  info "Extract later with: tar -C ${REMOTE_PATH} -xzf ${REMOTE_PATH}/${ARCHIVE_NAME}"
fi

# --- optional restart local ---
if [[ "$STOPPED_MONGO" -eq 1 ]]; then
  if confirm "Restart local mongo now?"; then
    compose start mongo
    if [[ "$STOPPED_SYNC" -eq 1 ]] && confirm "Restart local sync service too?"; then
      compose start sync
    fi
    ok "Local services restart requested"
  else
    warn "Local mongo left stopped."
  fi
fi

if confirm "Keep local archive under ${SHIP_DIR}/?"; then
  KEEP_LOCAL_ARCHIVE=y
  ok "Kept $ARCHIVE_PATH"
else
  KEEP_LOCAL_ARCHIVE=n
  cleanup_archive
fi

echo
ok "Shipment finished."
echo "  Remote host : ${REMOTE_USER}@${REMOTE_HOST}"
echo "  Remote path : ${REMOTE_PATH}"
if [[ "${REMOTE_EXTRACT,,}" == "y" || "${REMOTE_EXTRACT,,}" == "yes" ]]; then
  echo "  Data dir    : ${REMOTE_PATH}/DATA_SOURCE"
fi
echo
echo "See SHIPMENT.md for restore / run steps on the destination machine."
