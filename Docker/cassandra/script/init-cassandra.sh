#!/bin/bash

echo "Waiting for Cassandra to be ready..."
sleep 30

echo "Creating keyspace and tables for flight delay analysis..."

# Apply full schema from tools
cqlsh -f /opt/tools/schema_flight_delays.cql

echo "Cassandra schema initialized successfully!"
