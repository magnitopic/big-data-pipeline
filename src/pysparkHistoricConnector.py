"""
Spark ETL Pipeline for Flight Delays Analysis
Reads CSVs from ./data, transforms them, and writes to Cassandra.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, month, year,
    round, concat_ws, lit, coalesce, isnull,
    to_date, quarter, to_timestamp, from_unixtime, unix_timestamp
)
from pyspark.sql.types import DoubleType, IntegerType, FloatType
import sys
import os

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flight_weather_historic")

# Data paths
# Default to container path
DATA_DIR = os.getenv("DATA_DIR", "/data")
AIRLINE_DELAY_FILE = f"{DATA_DIR}/flights_delay.csv"
DELAYS_AGG_FILE = f"{DATA_DIR}/delays_history_agg.csv"
DELAYS_SAMPLE_FILE = f"{DATA_DIR}/delays_history_sample.csv"


def create_spark_session():
    """Initialize Spark session with Cassandra connector and optional master override"""
    builder = (
        SparkSession.builder
        .appName("FlightDelaysETL")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
    )
    master = os.getenv("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def write_to_cassandra(df, table_name, mode="append"):
    """Write DataFrame to Cassandra table in the configured keyspace"""
    (df.write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace=CASSANDRA_KEYSPACE, table=table_name)
        .mode(mode)
        .save())
    print(f"✓ Written {df.count()} rows to Cassandra table: {table_name}")


def parse_time_to_minutes(time_col):
    """Parse 'HH:MM' string column to total minutes as float"""
    from pyspark.sql.functions import split, coalesce
    parts = split(time_col, ':')
    hours = parts.getItem(0).cast(FloatType())
    minutes = parts.getItem(1).cast(FloatType())
    return coalesce(hours * 60 + minutes, lit(0.0))


def load_flights_delay_historico(spark):
    """
    Load flights_delay.csv and align to delays_history_sample table.
    This file contains individual flight records with delay information.
    """
    print("\n=== Loading flights_delay.csv (Individual Flights) ===")

    df = spark.read.csv(AIRLINE_DELAY_FILE, header=True, inferSchema=True)

    # Generate unique flight IDs using row number
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, monotonically_increasing_id

    # Add a unique ID column
    df = df.withColumn("flight_id",
                       concat_ws("_",
                                 col("Flight_Date"),
                                 col("Airline_Name"),
                                 monotonically_increasing_id().cast("string")))

    # Transform to match delays_history_sample schema
    df_transformed = df.select(
        col("flight_id").cast("string"),
        col("Origin_City").cast("string").alias("origin_airport"),
        col("Destination_City").cast("string").alias("dest_airport"),
        col("Airline_Name").cast("string").alias("airline"),
        to_date(col("Flight_Date")).alias("flight_date"),
        to_timestamp(concat_ws(" ", col("Flight_Date"), col(
            "Scheduled_Departure_Time")), "yyyy-MM-dd HH:mm").alias("scheduled_departure"),
        to_timestamp(concat_ws(" ", col("Flight_Date"), col(
            "Actual_Departure_Time")), "yyyy-MM-dd HH:mm").alias("actual_departure"),
        to_timestamp(concat_ws(" ", col("Flight_Date"), col(
            "Scheduled_Arrival_Time")), "yyyy-MM-dd HH:mm").alias("scheduled_arrival"),
        to_timestamp(concat_ws(" ", col("Flight_Date"), col(
            "Actual_Arrival_Time")), "yyyy-MM-dd HH:mm").alias("actual_arrival"),
        col("Departure_Delay_Minutes").cast(
            IntegerType()).alias("departure_delay_min"),
        col("Arrival_Delay_Minutes").cast(
            IntegerType()).alias("arrival_delay_min"),
        # Determine primary delay cause - simplified for now
        when(col("Cancelled_Flag") != "Not Cancelled", lit("CANCELLED"))
        .when(col("Arrival_Delay_Minutes").cast(IntegerType()) > 15, lit("DELAYED"))
        .otherwise(lit("ON_TIME")).alias("delay_cause")
    ).filter(
        # Filter out records with missing required primary key
        col("flight_id").isNotNull()
    )

    print(
        f"Loaded {df_transformed.count()} flight records for delays_history_sample")
    return df_transformed


def aggregate_airline_delay_cause(spark):
    """
    Aggregate flights_delay.csv data to create airline_delay_cause table.
    Groups by year, month, origin city (airport), and airline (carrier).
    """
    print("\n=== Aggregating flights_delay.csv for airline_delay_cause ===")

    df = spark.read.csv(AIRLINE_DELAY_FILE, header=True, inferSchema=True)

    # Extract year and month from Flight_Date
    df = df.withColumn("year", year(to_date(col("Flight_Date"))))
    df = df.withColumn("month", month(to_date(col("Flight_Date"))))

    # Parse delay times from HH:MM format to minutes
    df = df.withColumn("carrier_delay_min",
                       parse_time_to_minutes(col("Carrier_Delay_HH_MM")))
    df = df.withColumn("weather_delay_min",
                       parse_time_to_minutes(col("Weather_Delay_HH_MM")))
    df = df.withColumn(
        "nas_delay_min", parse_time_to_minutes(col("NAS_Delay_HH_MM")))
    df = df.withColumn("security_delay_min",
                       parse_time_to_minutes(col("Security_Delay_HH_MM")))
    df = df.withColumn("late_aircraft_delay_min",
                       parse_time_to_minutes(col("Late_Aircraft_Delay_HH_MM")))

    # Create flags for different conditions
    df = df.withColumn("is_delayed_15",
                       when(col("Arrival_Delay_Minutes").cast(IntegerType()) >= 15, 1).otherwise(0))
    df = df.withColumn("is_cancelled",
                       when(col("Cancelled_Flag") != "Not Cancelled", 1).otherwise(0))
    df = df.withColumn("is_diverted",
                       when(col("Diverted_Flag") == "Diverted", 1).otherwise(0))

    # Count delay causes (when delay > 0 for each type)
    df = df.withColumn("carrier_ct_flag",
                       when(col("carrier_delay_min") > 0, 1).otherwise(0))
    df = df.withColumn("weather_ct_flag",
                       when(col("weather_delay_min") > 0, 1).otherwise(0))
    df = df.withColumn("nas_ct_flag",
                       when(col("nas_delay_min") > 0, 1).otherwise(0))
    df = df.withColumn("security_ct_flag",
                       when(col("security_delay_min") > 0, 1).otherwise(0))
    df = df.withColumn("late_aircraft_ct_flag",
                       when(col("late_aircraft_delay_min") > 0, 1).otherwise(0))

    # Aggregate by year, month, airport (origin_city), and carrier (airline)
    agg_df = df.groupBy("year", "month", "Origin_City", "Airline_Name").agg(
        # Counts of delay causes
        sum("carrier_ct_flag").cast(FloatType()).alias("carrier_ct"),
        sum("weather_ct_flag").cast(FloatType()).alias("weather_ct"),
        sum("nas_ct_flag").cast(FloatType()).alias("nas_ct"),
        sum("security_ct_flag").cast(FloatType()).alias("security_ct"),
        sum("late_aircraft_ct_flag").cast(
            FloatType()).alias("late_aircraft_ct"),

        # Flight counts
        count("*").cast(IntegerType()).alias("arr_flights"),
        sum("is_delayed_15").cast(IntegerType()).alias("arr_del15"),
        sum("is_cancelled").cast(IntegerType()).alias("arr_cancelled"),
        sum("is_diverted").cast(IntegerType()).alias("arr_diverted"),

        # Total delay minutes by type
        sum("Arrival_Delay_Minutes").cast(FloatType()).alias("arr_delay"),
        sum("carrier_delay_min").cast(FloatType()).alias("carrier_delay"),
        sum("weather_delay_min").cast(FloatType()).alias("weather_delay"),
        sum("nas_delay_min").cast(FloatType()).alias("nas_delay"),
        sum("security_delay_min").cast(FloatType()).alias("security_delay"),
        sum("late_aircraft_delay_min").cast(
            FloatType()).alias("late_aircraft_delay")
    )

    # Rename columns to match schema
    final_df = agg_df.select(
        col("year"),
        col("month"),
        col("Origin_City").alias("airport"),
        col("Airline_Name").alias("carrier"),
        lit(None).alias("airport_name"),  # Not available in flights_delay.csv
        lit(None).alias("carrier_name"),  # Not available in flights_delay.csv
        col("carrier_ct"),
        col("weather_ct"),
        col("nas_ct"),
        col("security_ct"),
        col("late_aircraft_ct"),
        col("arr_flights"),
        col("arr_del15"),
        col("arr_cancelled"),
        col("arr_diverted"),
        col("arr_delay"),
        col("carrier_delay"),
        col("weather_delay"),
        col("nas_delay"),
        col("security_delay"),
        col("late_aircraft_delay")
    )

    print(
        f"Aggregated {final_df.count()} records for airline_delay_cause table")
    return final_df


def load_delays_history_sample_historico(spark):
    """Load delays_history_sample.csv and align to Historico schema."""
    print("\n=== Loading delays_history_sample.csv (Historico) ===")

    df = spark.read.csv(DELAYS_SAMPLE_FILE, header=True, inferSchema=True)

    # Validate required columns
    required = [
        "flight_id", "origin_airport", "dest_airport", "airline", "flight_date",
        "scheduled_departure", "actual_departure", "scheduled_arrival", "actual_arrival",
        "departure_delay_min", "arrival_delay_min", "delay_cause"
    ]
    for k in required:
        if k not in df.columns:
            # Allow partial datasets: fill missing with nulls where not key
            if k == "flight_id":
                raise ValueError(
                    "delays_history_sample.csv missing required primary key 'flight_id'")
            df = df.withColumn(k, lit(None))

    # Cast types
    df_typed = (
        df.withColumn("flight_id", col("flight_id").cast("string"))
          .withColumn("origin_airport", col("origin_airport").cast("string"))
          .withColumn("dest_airport", col("dest_airport").cast("string"))
          .withColumn("airline", col("airline").cast("string"))
          .withColumn("flight_date", to_date(col("flight_date")))
          .withColumn("scheduled_departure", to_timestamp(col("scheduled_departure")))
          .withColumn("actual_departure", to_timestamp(col("actual_departure")))
          .withColumn("scheduled_arrival", to_timestamp(col("scheduled_arrival")))
          .withColumn("actual_arrival", to_timestamp(col("actual_arrival")))
          .withColumn("departure_delay_min", col("departure_delay_min").cast(IntegerType()))
          .withColumn("arrival_delay_min", col("arrival_delay_min").cast(IntegerType()))
          .withColumn("delay_cause", col("delay_cause").cast("string"))
    )

    # Keep only table columns
    selected = df_typed.select(
        "flight_id", "origin_airport", "dest_airport", "airline", "flight_date",
        "scheduled_departure", "actual_departure", "scheduled_arrival", "actual_arrival",
        "departure_delay_min", "arrival_delay_min", "delay_cause"
    )

    print(f"Loaded {selected.count()} records for delays_history_sample")
    return selected


def load_delays_history_agg_historico(spark):
    """Load delays_history_agg.csv and align to Historico schema."""
    print("\n=== Loading delays_history_agg.csv (Historico) ===")

    df = spark.read.csv(DELAYS_AGG_FILE, header=True, inferSchema=True)

    required_keys = ["year", "month", "airport", "carrier"]
    for k in required_keys:
        if k not in df.columns:
            raise ValueError(
                f"Missing required column '{k}' in delays_history_agg.csv")

    float_cols = [
        "avg_arr_delay_min", "pct_arr_del15",
        "arr_delay", "carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"
    ]
    int_cols = ["arr_flights", "arr_del15", "arr_cancelled", "arr_diverted"]
    text_cols = ["airport_name", "carrier_name"]

    for c in float_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(FloatType()))
        else:
            df = df.withColumn(c, col(c).cast(FloatType()))

    for c in int_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(IntegerType()))
        else:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    for c in text_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))

    selected = df.select(
        col("year").cast(IntegerType()),
        col("month").cast(IntegerType()),
        col("airport").cast("string"),
        col("carrier").cast("string"),
        col("airport_name").cast("string"),
        col("carrier_name").cast("string"),
        col("avg_arr_delay_min"),
        col("pct_arr_del15"),
        col("arr_flights"),
        col("arr_del15"),
        col("arr_cancelled"),
        col("arr_diverted"),
        col("arr_delay"),
        col("carrier_delay"),
        col("weather_delay"),
        col("nas_delay"),
        col("security_delay"),
        col("late_aircraft_delay")
    )

    print(f"Loaded {selected.count()} records for delays_history_agg")
    return selected


def create_fact_delays(df_airline_delays):
    """Create fact table: flight delays by airport, airline, time"""
    print("\n=== Creating fact_flight_delays ===")

    fact_delays = df_airline_delays.select(
        col("year"),
        col("month"),
        col("quarter"),
        col("airport"),
        col("airport_name"),
        col("carrier"),
        col("carrier_name"),
        col("arr_flights").alias("total_flights"),
        col("arr_del15").alias("delayed_flights"),
        col("arr_cancelled").alias("cancelled_flights"),
        col("arr_diverted").alias("diverted_flights"),
        col("arr_delay").alias("total_delay_minutes"),
        col("carrier_delay"),
        col("weather_delay"),
        col("nas_delay"),
        col("security_delay"),
        col("late_aircraft_delay"),
        col("delay_rate"),
        col("cancellation_rate"),
        col("avg_delay_per_flight"),
        col("weather_delay_pct"),
        col("carrier_delay_pct")
    )

    return fact_delays


def create_dim_airports(df_airline_delays):
    """Create dimension table: airports"""
    print("\n=== Creating dim_airports ===")

    dim_airports = df_airline_delays.select(
        col("airport").alias("airport_code"),
        col("airport_name")
    ).distinct()

    return dim_airports


def create_dim_airlines(df_airline_delays):
    """Create dimension table: airlines"""
    print("\n=== Creating dim_airlines ===")

    dim_airlines = df_airline_delays.select(
        col("carrier").alias("carrier_code"),
        col("carrier_name")
    ).distinct()

    return dim_airlines


def create_agg_airport_performance(df_airline_delays):
    """Aggregate: Airport performance summary"""
    print("\n=== Creating agg_airport_performance ===")

    agg_airport = df_airline_delays.groupBy("airport", "airport_name").agg(
        sum("arr_flights").alias("total_flights"),
        sum("arr_del15").alias("total_delayed_flights"),
        sum("arr_cancelled").alias("total_cancelled"),
        sum("arr_delay").alias("total_delay_minutes"),
        sum("weather_delay").alias("total_weather_delay"),
        sum("carrier_delay").alias("total_carrier_delay"),
        avg("delay_rate").alias("avg_delay_rate"),
        avg("avg_delay_per_flight").alias("avg_delay_per_flight")
    ).withColumn(
        "weather_impact_score",
        round((col("total_weather_delay") / col("total_delay_minutes") * 100), 2)
    ).orderBy(col("total_flights").desc())

    return agg_airport


def create_agg_airline_performance(df_airline_delays):
    """Aggregate: Airline performance summary"""
    print("\n=== Creating agg_airline_performance ===")

    agg_airline = df_airline_delays.groupBy("carrier", "carrier_name").agg(
        sum("arr_flights").alias("total_flights"),
        sum("arr_del15").alias("total_delayed_flights"),
        sum("arr_cancelled").alias("total_cancelled"),
        sum("arr_delay").alias("total_delay_minutes"),
        avg("delay_rate").alias("avg_delay_rate"),
        avg("avg_delay_per_flight").alias("avg_delay_per_flight"),
        sum("weather_delay").alias("total_weather_delay"),
        sum("carrier_delay").alias("total_carrier_delay")
    ).withColumn(
        "on_time_rate",
        round(100 - col("avg_delay_rate"), 2)
    ).withColumn(
        "reliability_score",
        round((col("total_flights") - col("total_delayed_flights") -
              col("total_cancelled")) / col("total_flights") * 100, 2)
    ).orderBy(col("reliability_score").desc())

    return agg_airline


def create_agg_monthly_trends(df_airline_delays):
    """Aggregate: Monthly delay trends"""
    print("\n=== Creating agg_monthly_trends ===")

    agg_monthly = df_airline_delays.groupBy("year", "month").agg(
        sum("arr_flights").alias("total_flights"),
        sum("arr_del15").alias("total_delayed_flights"),
        sum("arr_delay").alias("total_delay_minutes"),
        sum("weather_delay").alias("weather_delay_minutes"),
        sum("carrier_delay").alias("carrier_delay_minutes"),
        sum("nas_delay").alias("nas_delay_minutes"),
        sum("late_aircraft_delay").alias("late_aircraft_delay_minutes"),
        avg("delay_rate").alias("avg_delay_rate")
    ).withColumn(
        "date",
        to_date(concat_ws("-", col("year"), col("month"), lit("01")))
    ).orderBy("year", "month")

    return agg_monthly


def create_agg_delay_causes(df_airline_delays):
    """Aggregate: Delay causes breakdown"""
    print("\n=== Creating agg_delay_causes ===")

    # Sum all delays by type
    delay_totals = df_airline_delays.agg(
        sum("weather_delay").alias("weather_delay"),
        sum("carrier_delay").alias("carrier_delay"),
        sum("nas_delay").alias("nas_delay"),
        sum("security_delay").alias("security_delay"),
        sum("late_aircraft_delay").alias("late_aircraft_delay")
    )

    # Unpivot to long format
    from pyspark.sql.functions import array, explode, struct

    delay_causes = delay_totals.select(
        explode(array(
            struct(lit("Weather").alias("delay_cause"), col(
                "weather_delay").alias("total_minutes")),
            struct(lit("Carrier").alias("delay_cause"), col(
                "carrier_delay").alias("total_minutes")),
            struct(lit("NAS").alias("delay_cause"), col(
                "nas_delay").alias("total_minutes")),
            struct(lit("Security").alias("delay_cause"), col(
                "security_delay").alias("total_minutes")),
            struct(lit("Late Aircraft").alias("delay_cause"), col(
                "late_aircraft_delay").alias("total_minutes"))
        )).alias("cause_data")
    ).select(
        col("cause_data.delay_cause"),
        col("cause_data.total_minutes")
    )

    # Calculate percentages
    total_delay = delay_causes.agg(sum("total_minutes")).collect()[0][0]

    delay_causes = delay_causes.withColumn(
        "percentage",
        round((col("total_minutes") / lit(total_delay) * 100), 2)
    ).orderBy(col("total_minutes").desc())

    return delay_causes


def create_agg_route_analysis(df_sample):
    """Aggregate: Route performance"""
    print("\n=== Creating agg_route_analysis ===")

    agg_routes = df_sample.groupBy("origin_airport", "dest_airport", "airline").agg(
        count("*").alias("total_flights"),
        sum("is_delayed").alias("delayed_flights"),
        avg("arrival_delay_min").alias("avg_delay_minutes"),
        count(when(col("delay_cause") == "WEATHER", 1)).alias("weather_delays"),
        count(when(col("delay_cause") == "CARRIER", 1)).alias("carrier_delays")
    ).withColumn(
        "delay_rate",
        round((col("delayed_flights") / col("total_flights") * 100), 2)
    ).filter(
        col("total_flights") >= 10  # Only routes with meaningful data
    ).orderBy(col("delay_rate").desc())

    return agg_routes


def create_data_quality_report(df_original, df_clean, dataset_name):
    """Create data quality metrics"""
    original_count = df_original.count()
    clean_count = df_clean.count()
    removed = original_count - clean_count
    quality_rate = round((clean_count / original_count * 100),
                         2) if original_count > 0 else 0

    return {
        "dataset": dataset_name,
        "original_records": original_count,
        "clean_records": clean_count,
        "removed_records": removed,
        "quality_rate": quality_rate
    }


def main():
    """Main ETL pipeline targeting Historico schema"""
    print("=" * 60)
    print("FLIGHT HISTORICO ETL PIPELINE → Cassandra")
    print("=" * 60)

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load CSVs aligned to Historico tables
        df_flights_delay = load_flights_delay_historico(spark)
        df_airline_delay_cause = aggregate_airline_delay_cause(spark)
        df_agg = load_delays_history_agg_historico(spark)
        # Note: delays_history_sample CSV is replaced by flights_delay.csv

        # Write to Cassandra (Historico keyspace)
        write_to_cassandra(df_flights_delay, "delays_history_sample")
        write_to_cassandra(df_airline_delay_cause, "airline_delay_cause")
        write_to_cassandra(df_agg, "delays_history_agg")

        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY (Historico)")
        print("=" * 60)
        print("\nTables written in Cassandra (flight_weather_historic):")
        print("  - delays_history_sample (individual flights from flights_delay.csv)")
        print("  - airline_delay_cause (aggregated from flights_delay.csv)")
        print("  - delays_history_agg")
        print("\nReady for Power BI connection!")

    except Exception as e:
        print(f"\n❌ Error in ETL pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
