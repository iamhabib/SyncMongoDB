#!/bin/sh
set -e

# Bind-mounted ./LOGS is often root-owned on the host. Fix ownership when
# starting as root, then drop to appuser for the Node process.
mkdir -p /app/logs/combined /app/logs/errors

if [ "$(id -u)" = "0" ]; then
  chown -R appuser:appgroup /app/logs
  exec su-exec appuser "$@"
fi

exec "$@"
