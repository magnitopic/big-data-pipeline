"""
Spark ETL Pipeline for Flight Delays Analysis
Reads CSVs from ./data, transforms them, and writes to Cassandra.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, month, year, 
    round, concat_ws, lit, coalesce, isnull, 
    to_date, quarter
)
from pyspark.sql.types import DoubleType, IntegerType
import sys
import os

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flight_delays")

# Data paths
# Default to container path "/data"; allow override for local WSL runs via env.
DATA_DIR = os.getenv("DATA_DIR", "/data")
AIRLINE_DELAY_FILE = f"{DATA_DIR}/Airline_Delay_Cause.csv"
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


def load_and_clean_airline_delays(spark):
    """Load and clean main airline delay data"""
    print("\n=== Loading Airline_Delay_Cause.csv ===")
    
    df = spark.read.csv(AIRLINE_DELAY_FILE, header=True, inferSchema=True)
    
    # Data quality: remove nulls and invalid data
    df_clean = df.filter(
        (col("arr_flights").isNotNull()) & 
        (col("arr_flights") > 0) &
        (col("year").isNotNull()) &
        (col("month").isNotNull())
    )
    
    # Cast numeric columns
    numeric_cols = [
        "arr_flights", "arr_del15", "arr_cancelled", "arr_diverted", 
        "arr_delay", "carrier_delay", "weather_delay", "nas_delay", 
        "security_delay", "late_aircraft_delay"
    ]
    
    for col_name in numeric_cols:
        df_clean = df_clean.withColumn(col_name, coalesce(col(col_name), lit(0)).cast(DoubleType()))
    
    # Calculate derived metrics
    df_transformed = df_clean.withColumn(
        "delay_rate", 
        round((col("arr_del15") / col("arr_flights") * 100), 2)
    ).withColumn(
        "cancellation_rate", 
        round((col("arr_cancelled") / col("arr_flights") * 100), 2)
    ).withColumn(
        "avg_delay_per_flight", 
        round((col("arr_delay") / col("arr_flights")), 2)
    ).withColumn(
        "weather_delay_pct",
        round((col("weather_delay") / col("arr_delay") * 100), 2)
    ).withColumn(
        "carrier_delay_pct",
        round((col("carrier_delay") / col("arr_delay") * 100), 2)
    ).withColumn(
        "date_key",
        concat_ws("-", col("year"), col("month"), lit("01"))
    ).withColumn(
        "quarter",
        quarter(to_date(col("date_key")))
    )
    
    print(f"Loaded {df_transformed.count()} records from Airline_Delay_Cause")
    return df_transformed


def load_delays_sample(spark):
    """Load individual flight delay samples"""
    print("\n=== Loading delays_history_sample.csv ===")
    
    df = spark.read.csv(DELAYS_SAMPLE_FILE, header=True, inferSchema=True)
    
    # Clean and transform
    df_clean = df.filter(
        (col("flight_id").isNotNull()) &
        (col("flight_date").isNotNull())
    ).withColumn(
        "departure_delay_min", coalesce(col("departure_delay_min"), lit(0))
    ).withColumn(
        "arrival_delay_min", coalesce(col("arrival_delay_min"), lit(0))
    ).withColumn(
        "is_delayed", when(col("arrival_delay_min") > 15, 1).otherwise(0)
    ).withColumn(
        "delay_severity",
        when(col("arrival_delay_min") <= 0, "On Time")
        .when(col("arrival_delay_min") <= 15, "Minor")
        .when(col("arrival_delay_min") <= 30, "Moderate")
        .when(col("arrival_delay_min") <= 60, "Significant")
        .otherwise("Severe")
    )
    
    print(f"Loaded {df_clean.count()} flight samples")
    return df_clean


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
    """Main ETL pipeline"""
    print("=" * 60)
    print("FLIGHT DELAYS ETL PIPELINE → Cassandra")
    print("=" * 60)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load and transform data
        df_airline_delays = load_and_clean_airline_delays(spark)
        df_sample = load_delays_sample(spark)
        
        # Create fact table
        fact_delays = create_fact_delays(df_airline_delays)
        write_to_cassandra(fact_delays, "fact_flight_delays")

        # Create dimension tables
        dim_airports = create_dim_airports(df_airline_delays)
        write_to_cassandra(dim_airports, "dim_airports")
        
        dim_airlines = create_dim_airlines(df_airline_delays)
        write_to_cassandra(dim_airlines, "dim_airlines")
        
        # Create aggregated tables for analytics
        agg_airport = create_agg_airport_performance(df_airline_delays)
        write_to_cassandra(agg_airport, "agg_airport_performance")
        
        agg_airline = create_agg_airline_performance(df_airline_delays)
        write_to_cassandra(agg_airline, "agg_airline_performance")
        
        agg_monthly = create_agg_monthly_trends(df_airline_delays)
        write_to_cassandra(agg_monthly, "agg_monthly_trends")
        
        agg_causes = create_agg_delay_causes(df_airline_delays)
        write_to_cassandra(agg_causes, "agg_delay_causes")
        
        agg_routes = create_agg_route_analysis(df_sample)
        write_to_cassandra(agg_routes, "agg_route_analysis")
        
        # Write sample data
        write_to_cassandra(df_sample, "fact_flight_samples")
        
        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY (Cassandra)")
        print("=" * 60)
        print("\nTables created in Cassandra:")
        print("  - fact_flight_delays (main fact table)")
        print("  - fact_flight_samples (individual flights)")
        print("  - dim_airports (airport dimension)")
        print("  - dim_airlines (airline dimension)")
        print("  - agg_airport_performance (for airport analysis)")
        print("  - agg_airline_performance (for airline comparison)")
        print("  - agg_monthly_trends (for time series)")
        print("  - agg_delay_causes (for cause breakdown)")
        print("  - agg_route_analysis (for route performance)")
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