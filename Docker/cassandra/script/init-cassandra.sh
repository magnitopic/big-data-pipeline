#!/bin/bash

echo "Waiting for Cassandra to be ready..."
sleep 30

echo "Creating keyspace and tables for flight delay analysis..."

cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS flight_delays
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
"

echo "Cassandra schema initialized successfully!"
