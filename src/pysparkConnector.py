"""
Spark ETL Pipeline for Flight Delays Analysis
Reads CSVs from ./data, transforms them, and writes to Cassandra.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, month, year, 
    round, concat_ws, lit, coalesce, isnull, 
    to_date, quarter, to_timestamp
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


def load_airline_delay_cause_historico(spark):
    """Load Airline_Delay_Cause.csv and align to Historico schema."""
    print("\n=== Loading Airline_Delay_Cause.csv (Historico) ===")

    df = spark.read.csv(AIRLINE_DELAY_FILE, header=True, inferSchema=True)

    # Ensure required keys exist
    required_keys = ["year", "month", "airport", "carrier"]
    for k in required_keys:
        if k not in df.columns:
            raise ValueError(f"Missing required column '{k}' in Airline_Delay_Cause.csv")

    # Add optional columns if missing and cast types to match Historico table
    float_cols = [
        "carrier_ct", "weather_ct", "nas_ct", "security_ct", "late_aircraft_ct",
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

    # Select and order columns as in table
    selected = df.select(
        col("year").cast(IntegerType()),
        col("month").cast(IntegerType()),
        col("airport").cast("string"),
        col("carrier").cast("string"),
        col("airport_name").cast("string"),
        col("carrier_name").cast("string"),
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

    print(f"Loaded {selected.count()} records for airline_delay_cause")
    return selected


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
                raise ValueError("delays_history_sample.csv missing required primary key 'flight_id'")
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
            raise ValueError(f"Missing required column '{k}' in delays_history_agg.csv")

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
        round((col("total_flights") - col("total_delayed_flights") - col("total_cancelled")) / col("total_flights") * 100, 2)
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
            struct(lit("Weather").alias("delay_cause"), col("weather_delay").alias("total_minutes")),
            struct(lit("Carrier").alias("delay_cause"), col("carrier_delay").alias("total_minutes")),
            struct(lit("NAS").alias("delay_cause"), col("nas_delay").alias("total_minutes")),
            struct(lit("Security").alias("delay_cause"), col("security_delay").alias("total_minutes")),
            struct(lit("Late Aircraft").alias("delay_cause"), col("late_aircraft_delay").alias("total_minutes"))
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
    quality_rate = round((clean_count / original_count * 100), 2) if original_count > 0 else 0
    
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
        df_airline_cause = load_airline_delay_cause_historico(spark)
        df_agg = load_delays_history_agg_historico(spark)
        df_sample = load_delays_history_sample_historico(spark)

        # Write to Cassandra (Historico keyspace)
        write_to_cassandra(df_airline_cause, "airline_delay_cause")
        write_to_cassandra(df_agg, "delays_history_agg")
        write_to_cassandra(df_sample, "delays_history_sample")

        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY (Historico)")
        print("=" * 60)
        print("\nTables written in Cassandra (flight_weather_historic):")
        print("  - airline_delay_cause")
        print("  - delays_history_agg")
        print("  - delays_history_sample")
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