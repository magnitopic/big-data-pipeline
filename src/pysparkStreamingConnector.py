from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, current_timestamp, from_unixtime, to_timestamp
from pyspark.sql.types import *
import os

# ========================
# CONFIG
# ========================
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE_STREAMING", "flight_weather_streaming")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FLIGHTS_TOPIC = os.getenv("FLIGHTS_KAFKA_TOPIC", "opensky.usa.raw")

# ========================
# SPARK SESSION
# ========================
spark = (
    SparkSession.builder
    .appName("OpenSky-USA-Streaming")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", CASSANDRA_PORT)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ========================
# OPEN SKY SCHEMA
# ========================
schema = StructType([
    StructField("time", LongType()),
    StructField("states", ArrayType(ArrayType(StringType())))
])

# ========================
# KAFKA STREAM
# ========================
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", FLIGHTS_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 200)
    .option("failOnDataLoss", "false")
    .load()
)


json_df = raw.selectExpr("CAST(value AS STRING) as json")

parsed = json_df.select(from_json(col("json"), schema).alias("data"))

states = parsed.select(explode(col("data.states")).alias("s"))

# ========================
# TRANSFORM TO CASSANDRA MODEL
# ========================
clean = states.select(
    col("s")[0].alias("icao24"),
    to_timestamp(from_unixtime(col("s")[3].cast("long"))).alias("time_position"),
    col("s")[1].alias("callsign"),
    col("s")[2].alias("origin_country"),
    col("s")[5].cast("double").alias("longitude"),
    col("s")[6].cast("double").alias("latitude"),
    col("s")[7].cast("double").alias("baro_altitude"),
    col("s")[8].cast("boolean").alias("on_ground"),
    col("s")[9].cast("double").alias("velocity"),
    col("s")[10].cast("double").alias("true_track"),
    col("s")[13].cast("double").alias("geo_altitude")
).filter(
    col("icao24").isNotNull() &
    col("time_position").isNotNull()
)

# ========================
# WRITE TO CASSANDRA
# ========================
query = (
    clean.writeStream
    .trigger(processingTime="15 seconds")
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", CASSANDRA_KEYSPACE)
    .option("table", "flights_rt")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/opensky")
    .outputMode("append")
    .start()
    )

query.awaitTermination()
