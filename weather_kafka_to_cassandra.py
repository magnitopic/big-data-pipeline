from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, ArrayType, LongType
)

# ---------- Spark Session ----------
spark = (
    SparkSession.builder
    .appName("WeatherKafkaToCassandra")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------- OpenWeather JSON Schema ----------
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", FloatType()),
        StructField("lat", FloatType())
    ])),
    StructField("weather", ArrayType(
        StructType([
            StructField("main", StringType()),
            StructField("description", StringType())
        ])
    )),
    StructField("main", StructType([
        StructField("temp", FloatType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("visibility", IntegerType()),
    StructField("wind", StructType([
        StructField("speed", FloatType()),
        StructField("deg", IntegerType()),
        StructField("gust", FloatType())
    ])),
    StructField("clouds", StructType([
        StructField("all", IntegerType())
    ])),
    StructField("rain", StructType([
        StructField("1h", FloatType())
    ])),
    StructField("snow", StructType([
        StructField("1h", FloatType())
    ])),
    StructField("dt", LongType())
])

# ---------- Read from Kafka ----------
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "weather_raw")
    .option("startingOffsets", "latest")
    .load()
)

# ---------- Parse JSON ----------
json_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# ---------- Transform to Cassandra Schema ----------
weather_df = (
    json_df
    .select(
        col("coord.lat").alias("lat"),
        col("coord.lon").alias("lon"),
        to_timestamp(expr("dt")).alias("dt"),  # epoch -> timestamp
        col("main.temp").alias("temp"),
        col("main.humidity").alias("humidity"),
        col("clouds.all").alias("clouds"),
        col("visibility"),
        col("wind.speed").alias("wind_speed"),
        col("wind.deg").alias("wind_deg"),
        col("wind.gust").alias("wind_gust"),
        col("rain.`1h`").alias("rain_1h"),
        col("snow.`1h`").alias("snow_1h"),
        col("weather")[0]["main"].alias("weather_main"),
        col("weather")[0]["description"].alias("weather_description")
    )
)

# ---------- Write to Cassandra ----------
query = (
    weather_df
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "flight_weather_streaming")
    .option("table", "weather_rt")
    .option("checkpointLocation", "/tmp/checkpoints/weather_rt")
    .outputMode("append")
    .start()
)

query.awaitTermination()
