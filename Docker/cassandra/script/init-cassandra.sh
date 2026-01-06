#!/bin/sh
set -eu

echo "⏳ Waiting for Cassandra to accept CQL..."

for i in $(seq 1 60); do
  if cqlsh localhost 9042 -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! cqlsh localhost 9042 -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; then
  echo "❌ Cassandra not ready"
  exit 1
fi

echo "📦 Creating historic schema..."
cqlsh -f /opt/tools/AHI_CQL_BD1_Historico.cql

echo "📡 Creating streaming schema..."
cqlsh -f /opt/tools/AHI_CQL_BD2_Streaming.cql

echo "🎯 Cassandra schema initialized successfully!"
