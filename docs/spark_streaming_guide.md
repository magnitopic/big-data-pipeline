# Spark Streaming ETL Pipeline

This document explains how to use the Spark Streaming pipeline to consume real-time flight and weather data from Kafka and write it to Cassandra.

## Overview

The pipeline consists of two scripts:

1. **`pysparkConnector.py`** - Batch ETL for historical data (CSV → Cassandra)
2. **`pysparkStreamingConnector.py`** - Streaming ETL for real-time data (Kafka → Cassandra)

## Architecture

```
NiFi → Kafka Topics → Spark Streaming → Cassandra
        ├── flights_raw  → flights_rt
        └── weather_raw  → weather_rt
```

## Cassandra Databases

### Historic Database (`flight_weather_historic`)
- `airline_delay_cause` - Historical delay causes by airline
- `delays_history_agg` - Aggregated delay statistics
- `delays_history_sample` - Sample flight delay records
- `weather_lcd` - Historical weather data

### Streaming Database (`flight_weather_streaming`)
- `flights_rt` - Real-time flight positions from OpenSky Network
- `weather_rt` - Real-time weather data from OpenWeather API

## Running the Streaming Pipeline

### Prerequisites

1. **Kafka topics must be created:**
   ```bash
   # Inside Kafka container
   kafka-topics --create --topic flights_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   kafka-topics --create --topic weather_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```

2. **Cassandra keyspace and tables must exist:**
   The tables are defined in `/Docker/cassandra/tools/schema_flight_delays.cql` and should be created automatically on container startup.

3. **NiFi flow must be running:**
   NiFi should be configured to fetch data from OpenSky and OpenWeather APIs and publish to Kafka.

### Starting the Streaming Pipeline

#### Option 1: Using spark-submit (Recommended for production)

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  /opt/spark-apps/pysparkStreamingConnector.py
```

#### Option 2: Using Python directly (for testing)

```bash
docker exec -it spark-master python3 /opt/spark-apps/pysparkStreamingConnector.py
```

**Note:** Option 2 requires Kafka and Cassandra connector JARs to be in the classpath.

### Environment Variables

You can customize the pipeline behavior with these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CASSANDRA_HOST` | `cassandra` | Cassandra host |
| `CASSANDRA_PORT` | `9042` | Cassandra CQL port |
| `CASSANDRA_KEYSPACE_STREAMING` | `flight_weather_streaming` | Target keyspace |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker addresses |
| `FLIGHTS_KAFKA_TOPIC` | `flights_raw` | Kafka topic for flight data |
| `WEATHER_KAFKA_TOPIC` | `weather_raw` | Kafka topic for weather data |
| `SPARK_MASTER_URL` | (auto-detect) | Spark master URL |

Example with custom settings:
```bash
docker exec -it spark-master \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e FLIGHTS_KAFKA_TOPIC=opensky_flights \
  spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  /opt/spark-apps/pysparkStreamingConnector.py
```

## Monitoring

### Check Streaming Status

1. **Spark UI:** http://localhost:8081 (Spark Master Web UI)
   - View active streaming jobs
   - Monitor batch processing times
   - Check executor logs

2. **Query Cassandra:**
   ```bash
   docker exec -it cassandra cqlsh
   ```
   ```sql
   USE flight_weather_streaming;
   
   -- Check flights data
   SELECT COUNT(*) FROM flights_rt;
   SELECT * FROM flights_rt LIMIT 10;
   
   -- Check weather data
   SELECT COUNT(*) FROM weather_rt;
   SELECT * FROM weather_rt LIMIT 10;
   ```

3. **Kafka Consumer Groups:**
   ```bash
   docker exec -it kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --describe --group spark-kafka-streaming
   ```

### Checkpoints

Streaming checkpoints are stored at:
- `/opt/spark-data/checkpoints/flights_rt`
- `/opt/spark-data/checkpoints/weather_rt`

These allow the pipeline to resume from the last processed offset if restarted.

## Data Schemas

### Input: Kafka JSON Messages

**Flights (OpenSky Network):**
```json
{
  "icao24": "a12345",
  "callsign": "UAL123",
  "origin_country": "United States",
  "time_position": 1640995200,
  "longitude": -74.0060,
  "latitude": 40.7128,
  "baro_altitude": 10000.0,
  "on_ground": false,
  "velocity": 250.5,
  "true_track": 180.0,
  "geo_altitude": 10050.0
}
```

**Weather (OpenWeather API):**
```json
{
  "lat": 40.7128,
  "lon": -74.0060,
  "dt": 1640995200,
  "temp": 15.5,
  "humidity": 65,
  "clouds": 20,
  "visibility": 10000,
  "wind_speed": 5.5,
  "wind_deg": 180,
  "wind_gust": 8.0,
  "rain_1h": 0.0,
  "snow_1h": 0.0,
  "weather_main": "Clear",
  "weather_description": "clear sky"
}
```

### Output: Cassandra Tables

See `/docs/AHI_CQL_BD2_Streaming.cql` for complete table definitions.

## Troubleshooting

### Pipeline won't start

1. **Check Kafka is running:**
   ```bash
   docker ps | grep kafka
   docker logs kafka
   ```

2. **Verify topics exist:**
   ```bash
   docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

3. **Check Cassandra is ready:**
   ```bash
   docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE flight_weather_streaming;"
   ```

### No data appearing in Cassandra

1. **Verify NiFi is publishing to Kafka:**
   ```bash
   # Consume from Kafka directly
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic weather_raw \
     --from-beginning
   ```

2. **Check Spark logs:**
   ```bash
   docker logs spark-master
   docker logs spark-worker
   ```

3. **Verify checkpoint directories are writable:**
   ```bash
   docker exec -it spark-master ls -la /opt/spark-data/checkpoints/
   ```

### High latency or lag

1. **Increase Spark resources:**
   Edit `docker-compose.yml` to allocate more memory/cores to Spark workers

2. **Tune batch intervals:**
   Modify the streaming query to use `.trigger(processingTime='30 seconds')`

3. **Scale Kafka partitions:**
   Increase partitions for better parallelism

## Stopping the Pipeline

Press `Ctrl+C` in the terminal running the streaming job, or:

```bash
# Find the process
docker exec -it spark-master ps aux | grep pysparkStreamingConnector

# Kill it
docker exec -it spark-master kill <PID>
```

## Integration with Power BI

Power BI can connect to both databases:

1. **Historical Analysis:** Connect to `flight_weather_historic` keyspace
2. **Real-Time Dashboard:** Connect to `flight_weather_streaming` keyspace

Use the Cassandra ODBC driver or Spark JDBC connector for Power BI connectivity.
