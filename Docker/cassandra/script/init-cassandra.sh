#!/bin/sh
set -eu

echo "Waiting for Cassandra to be ready..."
sleep 30

echo "Creating keyspace and tables for historic flight/weather analysis..."
cqlsh -f /opt/tools/AHI_CQL_BD1_Historico.cql

echo "Creating keyspace and tables for streaming flight/weather data..."
cqlsh -f /opt/tools/AHI_CQL_BD2_Streaming.cql

echo "Cassandra schema initialized successfully!"
