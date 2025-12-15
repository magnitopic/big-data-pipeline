#!/bin/bash
set -e

echo "Starting Kafka broker..."

# Arrancar Kafka en background
/etc/confluent/docker/run &
KAFKA_PID=$!

echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  sleep 3
done

echo "Creating Kafka topics..."

kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic flight-events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic weather-events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic enriched-events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic kpi-results \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Kafka topics created successfully!"
echo "Available topics:"
kafka-topics --list --bootstrap-server kafka:9092

wait $KAFKA_PID
