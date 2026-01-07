"""
Spark ETL Pipeline for Flight Delays Analysis - Historic Database
Reads CSVs from ./data, transforms them, and writes to Cassandra.
Target schema: flight_weather_historic keyspace with Spanish table names.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, month, year,
    round, concat_ws, lit, coalesce, isnull,
    to_date, quarter, to_timestamp, from_unixtime, unix_timestamp,
    monotonically_increasing_id, row_number, max as spark_max
)
from pyspark.sql.types import DoubleType, IntegerType, FloatType, BooleanType
from pyspark.sql.window import Window
import sys
import os

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flight_weather_historic")

# Data paths
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




def load_flights_delay_historico(spark):
    """
    Load flights_delay.csv and transform to hist_retraso_vuelos_muestra table.
    This table contains individual flight records with delay information.
    """
    print("\n=== Loading flights_delay.csv → hist_retraso_vuelos_muestra ===")

    # Read CSV with explicit schema to ensure time columns are strings
    df = spark.read.csv(AIRLINE_DELAY_FILE, header=True, inferSchema=False)
    
    # Cast specific columns to proper types
    df = df.withColumn("Flight_Date", col("Flight_Date").cast("string"))
    df = df.withColumn("Scheduled_Departure_Time", col("Scheduled_Departure_Time").cast("string"))
    df = df.withColumn("Actual_Departure_Time", col("Actual_Departure_Time").cast("string"))
    df = df.withColumn("Scheduled_Arrival_Time", col("Scheduled_Arrival_Time").cast("string"))
    df = df.withColumn("Actual_Arrival_Time", col("Actual_Arrival_Time").cast("string"))
    df = df.withColumn("Departure_Delay_Minutes", col("Departure_Delay_Minutes").cast(IntegerType()))
    df = df.withColumn("Arrival_Delay_Minutes", col("Arrival_Delay_Minutes").cast(IntegerType()))
    df = df.withColumn("Airline_Name", col("Airline_Name").cast("string"))
    df = df.withColumn("Origin_City", col("Origin_City").cast("string"))
    df = df.withColumn("Destination_City", col("Destination_City").cast("string"))
    df = df.withColumn("Cancelled_Flag", col("Cancelled_Flag").cast("string"))

    # Trim all time columns
    from pyspark.sql.functions import trim
    df = df.withColumn("Scheduled_Departure_Time", trim(col("Scheduled_Departure_Time")))
    df = df.withColumn("Actual_Departure_Time", trim(col("Actual_Departure_Time")))
    df = df.withColumn("Scheduled_Arrival_Time", trim(col("Scheduled_Arrival_Time")))
    df = df.withColumn("Actual_Arrival_Time", trim(col("Actual_Arrival_Time")))

    # Add a unique flight ID
    df = df.withColumn("id_vuelo",
                       concat_ws("_",
                                 col("Flight_Date"),
                                 col("Airline_Name"),
                                 monotonically_increasing_id().cast("string")))

    # Create timestamps by properly concatenating date and time with single space
    df_transformed = df.select(
        col("id_vuelo").cast("string"),
        trim(col("Origin_City")).cast("string").alias("codigo_aeropuerto_origen"),
        trim(col("Destination_City")).cast("string").alias("codigo_aeropuerto_destino"),
        trim(col("Airline_Name")).cast("string").alias("aerolinea"),
        to_date(col("Flight_Date"), "yyyy-MM-dd").alias("fecha_vuelo"),
        # Create timestamps using concat with single space
        when((col("Scheduled_Departure_Time").isNotNull()) & (col("Scheduled_Departure_Time") != "") & (col("Scheduled_Departure_Time") != "-"),
             to_timestamp(concat_ws(" ", col("Flight_Date"), col("Scheduled_Departure_Time")), "yyyy-MM-dd HH:mm"))
        .otherwise(lit(None)).alias("salida_programada"),
        when((col("Actual_Departure_Time").isNotNull()) & (col("Actual_Departure_Time") != "") & (col("Actual_Departure_Time") != "-"),
             to_timestamp(concat_ws(" ", col("Flight_Date"), col("Actual_Departure_Time")), "yyyy-MM-dd HH:mm"))
        .otherwise(lit(None)).alias("salida_real"),
        when((col("Scheduled_Arrival_Time").isNotNull()) & (col("Scheduled_Arrival_Time") != "") & (col("Scheduled_Arrival_Time") != "-"),
             to_timestamp(concat_ws(" ", col("Flight_Date"), col("Scheduled_Arrival_Time")), "yyyy-MM-dd HH:mm"))
        .otherwise(lit(None)).alias("llegada_programada"),
        when((col("Actual_Arrival_Time").isNotNull()) & (col("Actual_Arrival_Time") != "") & (col("Actual_Arrival_Time") != "-"),
             to_timestamp(concat_ws(" ", col("Flight_Date"), col("Actual_Arrival_Time")), "yyyy-MM-dd HH:mm"))
        .otherwise(lit(None)).alias("llegada_real"),
        col("Departure_Delay_Minutes").alias("retraso_salida"),
        col("Arrival_Delay_Minutes").alias("retraso_llegada"),
        # Determine primary delay cause
        when(col("Cancelled_Flag") != "Not Cancelled", lit("CANCELADO"))
        .when(col("Arrival_Delay_Minutes") > 15, lit("RETRASADO"))
        .otherwise(lit("A_TIEMPO")).alias("causa_retraso")
    ).filter(
        col("id_vuelo").isNotNull()
    )

    print(f"✓ Loaded {df_transformed.count()} flight records for hist_retraso_vuelos_muestra")
    return df_transformed


def aggregate_airline_delay_cause(spark):
    """
    Aggregate flights_delay.csv data to create hist_retraso_mes table.
    Groups by year, month, origin city (airport), and airline (carrier).
    """
    print("\n=== Aggregating flights_delay.csv → hist_retraso_mes ===")

    df = spark.read.csv(AIRLINE_DELAY_FILE, header=True, inferSchema=True)

    # Extract year and month from Flight_Date, create date column
    df = df.withColumn("flight_date", to_date(col("Flight_Date")))
    df = df.withColumn("year", year(col("flight_date")))
    df = df.withColumn("month", month(col("flight_date")))
    df = df.withColumn("fecha", 
                       to_date(concat_ws("-", col("year"), col("month"), lit("01"))))

    # Parse delay times from HH:MM format to minutes
    def parse_time_to_minutes(time_col):
        from pyspark.sql.functions import split
        parts = split(time_col, ':')
        hours = parts.getItem(0).cast(FloatType())
        minutes = parts.getItem(1).cast(FloatType())
        return coalesce(hours * 60 + minutes, lit(0.0))

    df = df.withColumn("retrasos_aerolinea",
                       parse_time_to_minutes(col("Carrier_Delay_HH_MM")))
    df = df.withColumn("retrasos_clima",
                       parse_time_to_minutes(col("Weather_Delay_HH_MM")))
    df = df.withColumn("retrasos_nas",
                       parse_time_to_minutes(col("NAS_Delay_HH_MM")))
    df = df.withColumn("retrasos_seguridad",
                       parse_time_to_minutes(col("Security_Delay_HH_MM")))
    df = df.withColumn("retrasos_aeronave_tardia",
                       parse_time_to_minutes(col("Late_Aircraft_Delay_HH_MM")))

    # Create flags for different conditions
    df = df.withColumn("retraso_15min",
                       when(col("Arrival_Delay_Minutes").cast(IntegerType()) >= 15, 1).otherwise(0))
    df = df.withColumn("cancelado_flag",
                       when(col("Cancelled_Flag") != "Not Cancelled", 1).otherwise(0))
    df = df.withColumn("desviado_flag",
                       when(col("Diverted_Flag") == "Diverted", 1).otherwise(0))

    # Aggregate by fecha, airport, and carrier
    agg_df = df.groupBy("fecha", "Origin_City", "Airline_Name").agg(
        # Flight counts
        count("*").cast(IntegerType()).alias("vuelos_llegados"),
        sum("retraso_15min").cast(IntegerType()).alias("vuelos_retraso_15min"),
        sum("cancelado_flag").cast(IntegerType()).alias("vuelos_cancelados"),
        sum("desviado_flag").cast(IntegerType()).alias("vuelos_desviados"),

        # Total delay minutes by type
        sum("Arrival_Delay_Minutes").cast(FloatType()).alias("total_retrasos"),
        sum("retrasos_aerolinea").cast(FloatType()).alias("retrasos_aerolinea"),
        sum("retrasos_clima").cast(FloatType()).alias("retrasos_clima"),
        sum("retrasos_nas").cast(FloatType()).alias("retrasos_nas"),
        sum("retrasos_seguridad").cast(FloatType()).alias("retrasos_seguridad"),
        sum("retrasos_aeronave_tardia").cast(FloatType()).alias("retrasos_aeronave_tardia")
    )

    # Calculate additional metrics
    final_df = agg_df.select(
        col("fecha"),
        col("Origin_City").alias("codigo_aeropuerto"),
        lit(None).alias("aeropuerto"),  # Airport name would need a lookup table
        col("Airline_Name").alias("codigo_aerolinea"),
        lit(None).alias("aerolinea"),  # Airline name would need a lookup table
        col("vuelos_llegados"),
        col("vuelos_retraso_15min"),
        col("vuelos_desviados"),
        col("vuelos_cancelados"),
        col("retrasos_clima"),
        col("retrasos_nas"),
        col("retrasos_aerolinea"),
        col("retrasos_aeronave_tardia"),
        col("retrasos_seguridad"),
        col("total_retrasos"),
        # Calculate average delay per flight
        when(col("vuelos_llegados") > 0,
             round(col("total_retrasos") / col("vuelos_llegados"), 2)).otherwise(lit(0.0))
        .alias("retraso_promedio_llegada"),
        # Calculate percentage of delayed flights
        when(col("vuelos_llegados") > 0,
             round(col("vuelos_retraso_15min") / col("vuelos_llegados") * 100, 2)).otherwise(lit(0.0))
        .alias("pct_vuelos_retraso_15min")
    )

    print(f"✓ Aggregated {final_df.count()} records for hist_retraso_mes")
    return final_df


def load_delays_history_sample_historico(spark):
    """Load delays_history_sample.csv and align to hist_retraso_vuelos schema."""
    print("\n=== Loading delays_history_sample.csv (hist_retraso_vuelos) ===")

    df = spark.read.csv(DELAYS_SAMPLE_FILE, header=True, inferSchema=True)

    # Transform to match hist_retraso_vuelos schema
    df_typed = (
        df.withColumn("aerolinea", col("airline").cast("string"))
          .withColumn("ciudad_origen", col("origin_airport").cast("string"))
          .withColumn("ciudad_destino", col("dest_airport").cast("string"))
          .withColumn("fecha_vuelo", to_date(col("flight_date")))
          .withColumn("salida_programada", to_timestamp(col("scheduled_departure")))
          .withColumn("salida_real", to_timestamp(col("actual_departure")))
          .withColumn("llegada_programada", to_timestamp(col("scheduled_arrival")))
          .withColumn("llegada_real", to_timestamp(col("actual_arrival")))
          .withColumn("retraso_salida", col("departure_delay_min").cast(IntegerType()))
          .withColumn("retraso_llegada", col("arrival_delay_min").cast(IntegerType()))
          .withColumn("desviado", when(col("delay_cause") == "DIVERTED", True).otherwise(False).cast(BooleanType()))
          .withColumn("cancelado", when(col("delay_cause") == "CANCELADO", True).otherwise(False).cast(BooleanType()))
          .withColumn("codigo_cancelacion", lit(None).cast("string"))
          .withColumn("tiempo_programado_vuelo", lit(None).cast(IntegerType()))
          .withColumn("tiempo_real_vuelo", lit(None).cast(IntegerType()))
          .withColumn("retraso_clima", lit(0.0).cast(FloatType()))
          .withColumn("retraso_nas", lit(0.0).cast(FloatType()))
          .withColumn("retraso_aerolinea", lit(0.0).cast(FloatType()))
          .withColumn("retraso_aeronave_tardia", lit(0.0).cast(FloatType()))
          .withColumn("retraso_seguridad", lit(0.0).cast(FloatType()))
    )

    selected = df_typed.select(
        "aerolinea", "ciudad_origen", "ciudad_destino", "desviado", "cancelado",
        "codigo_cancelacion", "fecha_vuelo", "salida_programada", "salida_real",
        "llegada_programada", "llegada_real", "retraso_salida", "retraso_llegada",
        "tiempo_programado_vuelo", "tiempo_real_vuelo", "retraso_clima",
        "retraso_nas", "retraso_aerolinea", "retraso_aeronave_tardia", "retraso_seguridad"
    )

    print(f"✓ Loaded {selected.count()} records for hist_retraso_vuelos")
    return selected


def main():
    """Main ETL pipeline targeting Historic Cassandra schema"""
    print("=" * 60)
    print("FLIGHT HISTORIC ETL PIPELINE → Cassandra")
    print("=" * 60)

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load and transform data for historic tables
        df_flights_sample = load_flights_delay_historico(spark)
        df_monthly_agg = aggregate_airline_delay_cause(spark)

        # Write to Cassandra (flight_weather_historic keyspace)
        write_to_cassandra(df_flights_sample, "hist_retraso_vuelos_muestra")
        write_to_cassandra(df_monthly_agg, "hist_retraso_mes")

        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nTables written in Cassandra (flight_weather_historic):")
        print("  - hist_retraso_vuelos_muestra (individual flights)")
        print("  - hist_retraso_mes (monthly aggregations)")
        print("\nReady for analysis and Power BI connection!")

    except Exception as e:
        print(f"\n❌ Error in ETL pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
