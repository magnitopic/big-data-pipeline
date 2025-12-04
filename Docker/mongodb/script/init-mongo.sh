#!/bin/bash
# MongoDB initialization script
# This runs automatically on first container start

set -e

echo "Initializing MongoDB database..."

# Create database and collections
mongosh <<EOF
use ${MONGO_INITDB_DATABASE}

// Create collections for the big data pipeline
db.createCollection("kpi_realtime")
db.createCollection("flight_events")
db.createCollection("weather_events")

print("MongoDB initialized successfully!")
EOF
