#!/bin/bash
set -euo pipefail

/usr/local/bin/docker-entrypoint.sh cassandra -f &
CASS_PID=$!

echo "⏳ Waiting for Cassandra to accept connections on 9042..."

READY=false
for i in {1..30}; do
  if nc -z localhost 9042 >/dev/null 2>&1; then
    READY=true
    break
  fi
  sleep 2
done

if [ "$READY" != "true" ]; then
  echo "❌ Cassandra did not start in time"
  exit 1
fi

echo "▶ Initializing schema..."
/opt/script/init-cassandra.sh || true

wait "$CASS_PID"
