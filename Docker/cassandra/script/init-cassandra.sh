#!/bin/bash

echo "Waiting for Cassandra to be ready..."
sleep 30

echo "Creating keyspace and tables for historic flight/weather analysis..."

# Apply historic schema
cqlsh -f /opt/tools/AHI_CQL_BD1_Historico.cql

echo "Cassandra schema initialized successfully!"
