-- Cassandra Schema for Flight Delay Analysis
-- This file contains the CQL schema definitions

-- Keyspace creation
CREATE KEYSPACE IF NOT EXISTS flight_delays 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_delays;

-- Flight events table
CREATE TABLE IF NOT EXISTS flights (
    flight_id text,
    timestamp timestamp,
    airline text,
    origin text,
    destination text,
    departure_time timestamp,
    arrival_time timestamp,
    status text,
    delay_minutes int,
    PRIMARY KEY (flight_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Weather events table
CREATE TABLE IF NOT EXISTS weather (
    location text,
    timestamp timestamp,
    temperature double,
    humidity int,
    wind_speed double,
    conditions text,
    visibility double,
    PRIMARY KEY (location, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Enriched data table (flights + weather)
CREATE TABLE IF NOT EXISTS enriched_data (
    event_id uuid,
    flight_id text,
    timestamp timestamp,
    airline text,
    origin text,
    destination text,
    delay_minutes int,
    weather_conditions text,
    temperature double,
    wind_speed double,
    PRIMARY KEY (event_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- KPI metrics table
CREATE TABLE IF NOT EXISTS kpi_metrics (
    metric_id uuid,
    timestamp timestamp,
    metric_name text,
    metric_value double,
    location text,
    airline text,
    PRIMARY KEY (metric_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
