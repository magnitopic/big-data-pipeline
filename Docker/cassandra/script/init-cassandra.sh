#!/bin/bash
set -e

echo "⏳ Waiting for Cassandra on port 9042..."

for i in {1..40}; do
  if nc -z localhost 9042 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! nc -z localhost 9042 >/dev/null 2>&1; then
  echo "❌ Cassandra not ready after timeout"
  exit 1
fi

echo "📦 Creating historic schema..."
cqlsh -f /opt/tools/AHI_CQL_BD1_Historico.cql

echo "📡 Creating streaming schema..."
cqlsh -f /opt/tools/AHI_CQL_BD2_Streaming.cql

echo "🎯 Cassandra schemas initialized successfully!"
