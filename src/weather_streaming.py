from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, from_unixtime, to_timestamp
from pyspark.sql.types import *
import os

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE_STREAMING", "flight_weather_streaming")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WEATHER_TOPIC = os.getenv("WEATHER_KAFKA_TOPIC", "openweather.usa.raw")

spark = (
    SparkSession.builder
    .appName("Weather-USA-Streaming")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("list", ArrayType(StructType([
        StructField("coord", StructType([
            StructField("lat", FloatType()),
            StructField("lon", FloatType())
        ])),
        StructField("dt", LongType()),
        StructField("main", StructType([
            StructField("temp", FloatType()),
            StructField("humidity", IntegerType())
        ])),
        StructField("clouds", StructType([
            StructField("all", IntegerType())
        ])),
        StructField("visibility", IntegerType()),
        StructField("wind", StructType([
            StructField("speed", FloatType()),
            StructField("deg", IntegerType()),
            StructField("gust", FloatType())
        ])),
        StructField("rain", StructType([
            StructField("1h", FloatType())
        ])),
        StructField("snow", StructType([
            StructField("1h", FloatType())
        ])),
        StructField("weather", ArrayType(StructType([
            StructField("main", StringType()),
            StructField("description", StringType())
        ])))
    ])))
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", WEATHER_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

json_df = raw.selectExpr("CAST(value AS STRING) as json")
parsed = json_df.select(from_json(col("json"), schema).alias("data"))
exploded = parsed.select(explode(col("data.list")).alias("w"))

clean = exploded.select(
    col("w.coord.lat").alias("lat"),
    col("w.coord.lon").alias("lon"),
    to_timestamp(from_unixtime(col("w.dt"))).alias("dt"),
    col("w.main.temp").alias("temp"),
    col("w.main.humidity").alias("humidity"),
    col("w.clouds.all").alias("clouds"),
    col("w.visibility").alias("visibility"),
    col("w.wind.speed").alias("wind_speed"),
    col("w.wind.deg").alias("wind_deg"),
    col("w.wind.gust").alias("wind_gust"),
    col("w.rain.1h").alias("rain_1h"),
    col("w.snow.1h").alias("snow_1h"),
    col("w.weather")[0]["main"].alias("weather_main"),
    col("w.weather")[0]["description"].alias("weather_description")
).filter(
    col("lat").isNotNull() &
    col("lon").isNotNull() &
    col("dt").isNotNull()
)

query = (
    clean.writeStream
    .trigger(processingTime="30 seconds")
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", CASSANDRA_KEYSPACE)
    .option("table", "weather_rt")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/weather")
    .outputMode("append")
    .start()
)

query.awaitTermination()
