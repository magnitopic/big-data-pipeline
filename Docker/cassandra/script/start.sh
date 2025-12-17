#!/bin/bash
set -euo pipefail

# Use the official entrypoint to apply env-based config, then start Cassandra in foreground
/usr/local/bin/docker-entrypoint.sh cassandra -f &
CASS_PID=$!

# Wait for port
echo "Waiting for Cassandra to accept connections on 9042..."
for i in {1..30}; do
  if /bin/nc -z localhost 9042; then
    break
  fi
  sleep 2
done

# Initialize schema
/opt/script/init-cassandra.sh || true

# Wait for Cassandra
wait ${CASS_PID}
