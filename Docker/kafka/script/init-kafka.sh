#!/bin/bash

echo "Starting Kafka broker..."

# Start Kafka in the background
/etc/confluent/docker/run &
KAFKA_PID=$!

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 15

# Create topics for the flight delay analysis
echo "Creating Kafka topics..."

kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic flight-events \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic weather-events \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic enriched-events \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic kpi-results \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo "Kafka topics created successfully!"

# List all topics
echo "Available topics:"
kafka-topics --list --bootstrap-server localhost:9092

# Keep the container running
wait $KAFKA_PID
