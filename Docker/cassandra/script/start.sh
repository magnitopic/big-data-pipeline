#!/bin/sh
set -e

echo "🚀 Starting Cassandra..."
/usr/local/bin/docker-entrypoint.sh cassandra -f &

CASS_PID=$!

echo "⏳ Waiting Cassandra bootstrap..."
/opt/script/init-cassandra.sh

echo "✅ Cassandra ready, handing over control"
wait ${CASS_PID}
