"""
Spark Structured Streaming Pipeline for Real-Time Flight and Weather Data
Consumes from Kafka topics, transforms data, and writes to Cassandra streaming tables.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType, ArrayType, DoubleType
)
import sys
import os

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv(
    "CASSANDRA_KEYSPACE_STREAMING", "flight_weather_streaming")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FLIGHTS_TOPIC = os.getenv("FLIGHTS_KAFKA_TOPIC", "flights_raw")
WEATHER_TOPIC = os.getenv("WEATHER_KAFKA_TOPIC", "weather_raw")


def create_spark_session():
    """Initialize Spark session with Cassandra and Kafka connectors"""
    builder = (
        SparkSession.builder
        .appName("FlightWeatherStreamingETL")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        # Kafka packages will be loaded via --packages when submitting
    )

    master = os.getenv("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)

    return builder.getOrCreate()


def get_flights_schema():
    """
    Define schema for OpenSky Network flight data
    Based on OpenSky API response structure
    """
    return StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", IntegerType(), True),  # Unix timestamp
        StructField("last_contact", IntegerType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("sensors", ArrayType(IntegerType()), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True)
    ])


def get_weather_schema():
    """
    Define schema for OpenWeather API data
    Based on actual OpenWeather API response structure (nested JSON)
    """
    return StructType([
        StructField("coord", StructType([
            StructField("lon", FloatType(), True),
            StructField("lat", FloatType(), True)
        ]), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ])), True),
        StructField("main", StructType([
            StructField("temp", FloatType(), True),
            StructField("feels_like", FloatType(), True),
            StructField("temp_min", FloatType(), True),
            StructField("temp_max", FloatType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("sea_level", IntegerType(), True),
            StructField("grnd_level", IntegerType(), True)
        ]), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind", StructType([
            StructField("speed", FloatType(), True),
            StructField("deg", IntegerType(), True),
            StructField("gust", FloatType(), True)
        ]), True),
        StructField("clouds", StructType([
            StructField("all", IntegerType(), True)
        ]), True),
        StructField("rain", StructType([
            StructField("1h", FloatType(), True)
        ]), True),
        StructField("snow", StructType([
            StructField("1h", FloatType(), True)
        ]), True),
        StructField("dt", IntegerType(), True),
        StructField("sys", StructType([
            StructField("country", StringType(), True),
            StructField("sunrise", IntegerType(), True),
            StructField("sunset", IntegerType(), True)
        ]), True),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True)
    ])


def process_flights_stream(spark):
    """
    Read flights data from Kafka, transform, and write to Cassandra
    """
    print("\n=== Starting Flights Stream Processing ===")

    # Read from Kafka
    flights_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", FLIGHTS_TOPIC)
        # Use "earliest" to replay all data
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON from Kafka value field
    flights_schema = get_flights_schema()

    flights_parsed = (
        flights_stream
        .selectExpr("CAST(value AS STRING) as json_data")
        .select(from_json(col("json_data"), flights_schema).alias("data"))
        .select("data.*")
    )

    # Transform to match Cassandra schema (flights_rt table)
    flights_transformed = flights_parsed.select(
        col("icao24").cast("string"),
        to_date(
            to_timestamp(col("time_position"))
        ).alias("time_position"),
        col("callsign").cast("string"),
        col("origin_country").cast("string"),
        col("longitude").cast("float"),
        col("latitude").cast("float"),
        col("baro_altitude").cast("float"),
        col("on_ground").cast("boolean"),
        col("velocity").cast("float"),
        col("true_track").cast("float"),
        col("geo_altitude").cast("float")
    ).filter(
        # Filter out null required fields
        col("icao24").isNotNull() &
        col("time_position").isNotNull()
    )

    # Write to Cassandra
    query = (
        flights_transformed.writeStream
        .outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", "flights_rt")
        .option("checkpointLocation", "/opt/spark-data/checkpoints/flights_rt")
        .start()
    )

    print(
        f"✓ Flights stream started - writing to {CASSANDRA_KEYSPACE}.flights_rt")
    return query


def process_weather_stream(spark):
    """
    Read weather data from Kafka, transform, and write to Cassandra
    """
    print("\n=== Starting Weather Stream Processing ===")

    # Read from Kafka
    weather_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", WEATHER_TOPIC)
        # Use "earliest" to replay all data
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON from Kafka value field
    weather_schema = get_weather_schema()

    weather_parsed = (
        weather_stream
        .selectExpr("CAST(value AS STRING) as json_data")
        .select(from_json(col("json_data"), weather_schema).alias("data"))
        .select("data.*")
    )

    # Transform to match Cassandra schema (weather_rt table)
    # Extract nested fields and flatten the structure
    weather_transformed = weather_parsed.select(
        col("coord.lat").cast("float").alias("lat"),
        col("coord.lon").cast("float").alias("lon"),
        to_date(
            to_timestamp(col("dt"))
        ).alias("dt"),
        col("main.temp").cast("float").alias("temp"),
        col("main.humidity").cast("int").alias("humidity"),
        col("clouds.all").cast("int").alias("clouds"),
        col("visibility").cast("int"),
        col("wind.speed").cast("float").alias("wind_speed"),
        col("wind.deg").cast("int").alias("wind_deg"),
        col("wind.gust").cast("float").alias("wind_gust"),
        col("rain.1h").cast("float").alias("rain_1h"),
        col("snow.1h").cast("float").alias("snow_1h"),
        col("weather").getItem(0).getField("main").cast(
            "string").alias("weather_main"),
        col("weather").getItem(0).getField("description").cast(
            "string").alias("weather_description")
    ).filter(
        # Filter out null required fields
        col("lat").isNotNull() & 
        col("lon").isNotNull() & 
        col("dt").isNotNull()
    )

    # Write to Cassandra
    query = (
        weather_transformed.writeStream
        .outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", "weather_rt")
        .option("checkpointLocation", "/opt/spark-data/checkpoints/weather_rt")
        .start()
    )

    print(
        f"✓ Weather stream started - writing to {CASSANDRA_KEYSPACE}.weather_rt")
    return query


def main():
    """Main streaming pipeline"""
    print("=" * 60)
    print("FLIGHT WEATHER STREAMING ETL PIPELINE")
    print("Kafka → Spark Streaming → Cassandra")
    print("=" * 60)

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Start both streaming queries
        flights_query = process_flights_stream(spark)
        weather_query = process_weather_stream(spark)

        print("\n" + "=" * 60)
        print("STREAMING PIPELINE STARTED SUCCESSFULLY")
        print("=" * 60)
        print(f"\nKafka Topics:")
        print(f"  - {FLIGHTS_TOPIC} → flights_rt")
        print(f"  - {WEATHER_TOPIC} → weather_rt")
        print(f"\nCassandra Keyspace: {CASSANDRA_KEYSPACE}")
        print(f"Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
        print("\nPress Ctrl+C to stop the streaming pipeline...")

        # Wait for termination of both queries
        flights_query.awaitTermination()
        weather_query.awaitTermination()

    except KeyboardInterrupt:
        print("\n\nStopping streaming pipeline...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error in streaming pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
