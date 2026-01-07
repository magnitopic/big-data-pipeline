"""
Spark ETL Pipeline for Flight Delays Analysis - Historic Database
Reads CSVs from ./data, transforms them, and writes to Cassandra.
Target schema: flight_weather_historic keyspace with Spanish table names.

CORRECT MAPPINGS:
- flights_delay.csv → hist_retraso_vuelos
- delays_history_agg.csv → hist_retraso_mes
- delays_history_sample.csv → hist_retraso_vuelos_muestra
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, month, year,
    round, concat_ws, lit, coalesce, isnull,
    to_date, quarter, to_timestamp, from_unixtime, unix_timestamp,
    monotonically_increasing_id, row_number, max as spark_max, split, trim
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
FLIGHTS_DELAY_FILE = f"{DATA_DIR}/flights_delay.csv"
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
    """Parse time from HH:MM format to minutes (float)"""
    parts = split(time_col, ':')
    hours = parts.getItem(0).cast(FloatType())
    minutes = parts.getItem(1).cast(FloatType())
    return coalesce(hours * 60 + minutes, lit(0.0))


def load_flights_delay_to_hist_retraso_vuelos(spark):
    """
    Load flights_delay.csv and transform to hist_retraso_vuelos table.
    This table contains detailed flight records with delay breakdown by cause.
    
    Schema:
    - aerolinea, ciudad_origen, ciudad_destino
    - desviado (boolean), cancelado (boolean), codigo_cancelacion
    - fecha_vuelo, salida_programada, salida_real, llegada_programada, llegada_real
    - retraso_salida, retraso_llegada
    - tiempo_programado_vuelo, tiempo_real_vuelo
    - retraso_clima, retraso_nas, retraso_aerolinea, retraso_aeronave_tardia, retraso_seguridad
    """
    print("\n=== Loading flights_delay.csv → hist_retraso_vuelos ===")

    df = spark.read.csv(FLIGHTS_DELAY_FILE, header=True, inferSchema=False)
    
    # Cast and clean columns
    df = df.withColumn("Flight_Date", col("Flight_Date").cast("string"))
    df = df.withColumn("Scheduled_Departure_Time", trim(col("Scheduled_Departure_Time")))
    df = df.withColumn("Actual_Departure_Time", trim(col("Actual_Departure_Time")))
    df = df.withColumn("Scheduled_Arrival_Time", trim(col("Scheduled_Arrival_Time")))
    df = df.withColumn("Actual_Arrival_Time", trim(col("Actual_Arrival_Time")))
    df = df.withColumn("Departure_Delay_Minutes", col("Departure_Delay_Minutes").cast(IntegerType()))
    df = df.withColumn("Arrival_Delay_Minutes", col("Arrival_Delay_Minutes").cast(IntegerType()))
    df = df.withColumn("Scheduled_Elapsed_Time_Minutes", col("Scheduled_Elapsed_Time_Minutes").cast(IntegerType()))
    df = df.withColumn("Actual_Elapsed_Time_Minutes", col("Actual_Elapsed_Time_Minutes").cast(IntegerType()))

    # Parse delay times from HH:MM format to minutes
    df = df.withColumn("retraso_aerolinea", parse_time_to_minutes(col("Carrier_Delay_HH_MM")))
    df = df.withColumn("retraso_clima", parse_time_to_minutes(col("Weather_Delay_HH_MM")))
    df = df.withColumn("retraso_nas", parse_time_to_minutes(col("NAS_Delay_HH_MM")))
    df = df.withColumn("retraso_seguridad", parse_time_to_minutes(col("Security_Delay_HH_MM")))
    df = df.withColumn("retraso_aeronave_tardia", parse_time_to_minutes(col("Late_Aircraft_Delay_HH_MM")))

    # Transform to match hist_retraso_vuelos schema
    df_transformed = df.select(
        trim(col("Airline_Name")).cast("string").alias("aerolinea"),
        trim(col("Origin_City")).cast("string").alias("ciudad_origen"),
        trim(col("Destination_City")).cast("string").alias("ciudad_destino"),
        when(col("Diverted_Flag") == "Diverted", True).otherwise(False).cast(BooleanType()).alias("desviado"),
        when(col("Cancelled_Flag") != "Not Cancelled", True).otherwise(False).cast(BooleanType()).alias("cancelado"),
        when(col("Cancelled_Flag") != "Not Cancelled", col("Cancellation_Code")).otherwise(lit(None)).cast("string").alias("codigo_cancelacion"),
        to_date(col("Flight_Date"), "yyyy-MM-dd").alias("fecha_vuelo"),
        # Create timestamps by concatenating date and time
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
        col("Scheduled_Elapsed_Time_Minutes").alias("tiempo_programado_vuelo"),
        col("Actual_Elapsed_Time_Minutes").alias("tiempo_real_vuelo"),
        col("retraso_clima"),
        col("retraso_nas"),
        col("retraso_aerolinea"),
        col("retraso_aeronave_tardia"),
        col("retraso_seguridad")
    ).filter(
        col("fecha_vuelo").isNotNull() & col("ciudad_origen").isNotNull()
    )

    print(f"✓ Loaded {df_transformed.count()} flight records for hist_retraso_vuelos")
    return df_transformed


def load_delays_agg_to_hist_retraso_mes(spark):
    """
    Load delays_history_agg.csv and transform to hist_retraso_mes table.
    This table contains monthly aggregations by airport and carrier.
    
    Schema:
    - fecha, codigo_aeropuerto, aeropuerto, codigo_aerolinea, aerolinea
    - vuelos_llegados, vuelos_retraso_15min, vuelos_desviados, vuelos_cancelados
    - retrasos_clima, retrasos_nas, retrasos_aerolinea, retrasos_aeronave_tardia, retrasos_seguridad
    - total_retrasos, retraso_promedio_llegada, pct_vuelos_retraso_15min
    """
    print("\n=== Loading delays_history_agg.csv → hist_retraso_mes ===")

    df = spark.read.csv(DELAYS_AGG_FILE, header=True, inferSchema=True)

    # Create fecha from year and month
    df = df.withColumn("fecha", 
                       to_date(concat_ws("-", col("year"), col("month"), lit("01"))))

    # Transform to match hist_retraso_mes schema
    df_transformed = df.select(
        col("fecha"),
        col("airport").alias("codigo_aeropuerto"),
        col("airport_name").alias("aeropuerto"),
        col("carrier").alias("codigo_aerolinea"),
        col("carrier_name").alias("aerolinea"),
        col("arr_flights").cast(IntegerType()).alias("vuelos_llegados"),
        col("arr_del15").cast(IntegerType()).alias("vuelos_retraso_15min"),
        col("arr_diverted").cast(IntegerType()).alias("vuelos_desviados"),
        col("arr_cancelled").cast(IntegerType()).alias("vuelos_cancelados"),
        col("weather_delay").cast(FloatType()).alias("retrasos_clima"),
        col("nas_delay").cast(FloatType()).alias("retrasos_nas"),
        col("carrier_delay").cast(FloatType()).alias("retrasos_aerolinea"),
        col("late_aircraft_delay").cast(FloatType()).alias("retrasos_aeronave_tardia"),
        col("security_delay").cast(FloatType()).alias("retrasos_seguridad"),
        col("arr_delay").cast(FloatType()).alias("total_retrasos"),
        col("avg_arr_delay_min").cast(FloatType()).alias("retraso_promedio_llegada"),
        (col("pct_arr_del15") * 100).cast(FloatType()).alias("pct_vuelos_retraso_15min")
    ).filter(
        col("fecha").isNotNull() & col("codigo_aeropuerto").isNotNull()
    )

    print(f"✓ Loaded {df_transformed.count()} monthly aggregation records for hist_retraso_mes")
    return df_transformed


def load_delays_sample_to_hist_retraso_vuelos_muestra(spark):
    """
    Load delays_history_sample.csv and transform to hist_retraso_vuelos_muestra table.
    This table contains a sample of individual flights with basic delay information.
    
    Schema:
    - id_vuelo (PK), codigo_aeropuerto_origen, codigo_aeropuerto_destino, aerolinea
    - fecha_vuelo, salida_programada, salida_real, llegada_programada, llegada_real
    - retraso_salida, retraso_llegada, causa_retraso
    """
    print("\n=== Loading delays_history_sample.csv → hist_retraso_vuelos_muestra ===")

    df = spark.read.csv(DELAYS_SAMPLE_FILE, header=True, inferSchema=True)

    # Transform to match hist_retraso_vuelos_muestra schema
    df_transformed = df.select(
        col("flight_id").cast("string").alias("id_vuelo"),
        col("origin_airport").cast("string").alias("codigo_aeropuerto_origen"),
        col("dest_airport").cast("string").alias("codigo_aeropuerto_destino"),
        col("airline").cast("string").alias("aerolinea"),
        to_date(col("flight_date")).alias("fecha_vuelo"),
        to_timestamp(col("scheduled_departure")).alias("salida_programada"),
        to_timestamp(col("actual_departure")).alias("salida_real"),
        to_timestamp(col("scheduled_arrival")).alias("llegada_programada"),
        to_timestamp(col("actual_arrival")).alias("llegada_real"),
        col("departure_delay_min").cast(IntegerType()).alias("retraso_salida"),
        col("arrival_delay_min").cast(IntegerType()).alias("retraso_llegada"),
        col("delay_cause").cast("string").alias("causa_retraso")
    ).filter(
        col("id_vuelo").isNotNull()
    )

    print(f"✓ Loaded {df_transformed.count()} sample flight records for hist_retraso_vuelos_muestra")
    return df_transformed


def main():
    """Main ETL pipeline targeting Historic Cassandra schema"""
    print("=" * 80)
    print("FLIGHT HISTORIC ETL PIPELINE → Cassandra")
    print("=" * 80)
    print("\nCORRECT MAPPINGS:")
    print("  flights_delay.csv → hist_retraso_vuelos")
    print("  delays_history_agg.csv → hist_retraso_mes")
    print("  delays_history_sample.csv → hist_retraso_vuelos_muestra")
    print("=" * 80)

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load and transform data with CORRECT mappings
        df_hist_retraso_vuelos = load_flights_delay_to_hist_retraso_vuelos(spark)
        df_hist_retraso_mes = load_delays_agg_to_hist_retraso_mes(spark)
        df_hist_retraso_vuelos_muestra = load_delays_sample_to_hist_retraso_vuelos_muestra(spark)

        # Write to Cassandra (flight_weather_historic keyspace)
        write_to_cassandra(df_hist_retraso_vuelos, "hist_retraso_vuelos")
        write_to_cassandra(df_hist_retraso_mes, "hist_retraso_mes")
        write_to_cassandra(df_hist_retraso_vuelos_muestra, "hist_retraso_vuelos_muestra")

        print("\n" + "=" * 80)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\nTables written in Cassandra (flight_weather_historic):")
        print("  - hist_retraso_vuelos (detailed flights with delay breakdown)")
        print("  - hist_retraso_mes (monthly aggregations)")
        print("  - hist_retraso_vuelos_muestra (sample flights)")
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