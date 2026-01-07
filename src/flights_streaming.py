from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, from_unixtime, to_timestamp
from pyspark.sql.types import *
import os, time

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE_STREAMING", "flight_weather_streaming")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FLIGHTS_TOPIC = os.getenv("FLIGHTS_KAFKA_TOPIC", "opensky.usa.raw")

spark = (
    SparkSession.builder
    .appName("Flights-USA-Streaming")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.connection.timeoutMS", "20000")
    .config("spark.cassandra.read.timeoutMS", "20000")
    .config("spark.cassandra.connection.keepAliveMS", "20000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("time", LongType()),
    StructField("states", ArrayType(ArrayType(StringType())))
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", FLIGHTS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data"))

states = parsed.select(explode(col("data.states")).alias("s"))

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

def write_to_cassandra(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    for i in range(5):
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", CASSANDRA_KEYSPACE) \
                .option("table", "flights_rt") \
                .mode("append") \
                .save()
            return
        except Exception as e:
            print(f"Cassandra write failed (attempt {i+1}/5): {e}")
            time.sleep(5)

    raise Exception("Cassandra unreachable after retries")

query = (
    clean.writeStream
    .trigger(processingTime="15 seconds")
    .foreachBatch(write_to_cassandra)
    .option("checkpointLocation", "/opt/spark-data/checkpoints/opensky")
    .start()
)

query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, from_unixtime, to_timestamp
from pyspark.sql.types import *
import os, time

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE_STREAMING", "flight_weather_streaming")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FLIGHTS_TOPIC = os.getenv("FLIGHTS_KAFKA_TOPIC", "opensky.usa.raw")

spark = (
    SparkSession.builder
    .appName("Flights-USA-Streaming")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.connection.timeout_ms", "20000")
    .config("spark.cassandra.read.timeout_ms", "20000")
    .config("spark.cassandra.connection.keep_alive_ms", "20000")
    .config("spark.cassandra.connection.retries", "10")
    .config("spark.cassandra.connection.retry_delay_ms", "3000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("time", LongType()),
    StructField("states", ArrayType(ArrayType(StringType())))
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", FLIGHTS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data"))

states = parsed.select(explode(col("data.states")).alias("s"))

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

def write_to_cassandra(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    for i in range(5):
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", CASSANDRA_KEYSPACE) \
                .option("table", "flights_rt") \
                .mode("append") \
                .save()
            return
        except Exception as e:
            print(f"Cassandra write failed (attempt {i+1}/5): {e}")
            time.sleep(5)

    raise Exception("Cassandra unreachable after retries")

query = (
    clean.writeStream
    .trigger(processingTime="15 seconds")
    .foreachBatch(write_to_cassandra)
    .option("checkpointLocation", "/opt/spark-data/checkpoints/opensky")
    .start()
)

query.awaitTermination()
