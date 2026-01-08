"""
Spark ETL Pipeline for Weather Station Data - Historic Database
Reads 33,000+ weather station CSV files from a directory and writes to Cassandra.
Target table: hist_estaciones_m in flight_weather_historic keyspace.

Optimized for:
- Large-scale data processing (146GB)
- High write throughput to Cassandra
- Parallel processing of thousands of files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, to_timestamp, when, regexp_extract,
    concat_ws, substring, length
)
from pyspark.sql.types import (
    FloatType, IntegerType, StringType, TimestampType
)
import sys
import os

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flight_weather_historic")

# Data paths
DATA_DIR = os.getenv("WEATHER_DATA_DIR", "/data/bigData")
# Expects CSV files like: LCD_*.csv in the directory

# Performance tuning
BATCH_SIZE = int(os.getenv("CASSANDRA_BATCH_SIZE", "1000"))
PARALLELISM = int(os.getenv("SPARK_PARALLELISM", "200"))


def create_spark_session():
    """
    Initialize Spark session with optimized settings for large-scale ETL.
    Configured for high-throughput Cassandra writes.
    """
    builder = (
        SparkSession.builder
        .appName("WeatherStationsETL")
        # Memory configuration - adjust based on your cluster
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "4")
        # Cassandra connector configuration
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        # Optimizations for large datasets
        .config("spark.sql.shuffle.partitions", str(PARALLELISM))
        .config("spark.default.parallelism", str(PARALLELISM))
        # Cassandra write optimizations
        .config("spark.cassandra.output.batch.size.rows", str(BATCH_SIZE))
        .config("spark.cassandra.output.concurrent.writes", "10")
        .config("spark.cassandra.output.batch.grouping.buffer.size", "1000")
        .config("spark.cassandra.output.batch.grouping.key", "partition")
        # Enable adaptive query execution for better performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    
    master = os.getenv("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)
    
    return builder.getOrCreate()


def parse_weather_type(weather_str):
    """
    Parse weather type codes from HourlyPresentWeatherType field.
    Maps common weather codes to integers for the database.
    Returns INT representing weather condition.
    """
    # This is a simplified mapping - adjust based on your needs
    # Weather codes: RA=rain, SN=snow, FG=fog, etc.
    return (
        when(col(weather_str).contains("RA"), 1)  # Rain
        .when(col(weather_str).contains("SN"), 2)  # Snow
        .when(col(weather_str).contains("FG"), 3)  # Fog
        .when(col(weather_str).contains("BR"), 4)  # Mist
        .when(col(weather_str).contains("TS"), 5)  # Thunderstorm
        .otherwise(0)  # Clear/Unknown
    )


def clean_numeric_field(field_col, target_type=FloatType()):
    """
    Clean numeric fields that may contain 's', 'T', or other non-numeric values.
    's' typically means "suspect data"
    'T' typically means "trace amount"
    """
    cleaned = (
        when(col(field_col).rlike("^[0-9.-]+$"), col(field_col).cast(target_type))
        .when(col(field_col) == "T", lit(0.001).cast(target_type))  # Trace
        .otherwise(lit(None).cast(target_type))
    )
    return cleaned


def load_weather_stations_data(spark, input_path):
    """
    Load all weather station CSV files from the input directory.
    
    The files follow the format: LCD_STATIONID_YEAR.csv
    Each file contains hourly weather observations for one station for one year.
    
    CSV Schema (relevant columns):
    - STATION: Station ID
    - DATE: Timestamp in ISO format
    - LATITUDE, LONGITUDE, ELEVATION: Station location
    - NAME: Station name
    - HourlyAltimeterSetting: Barometric pressure
    - HourlyDewPointTemperature: Dew point
    - HourlyDryBulbTemperature: Air temperature
    - HourlyPrecipitation: Precipitation amount
    - HourlyPresentWeatherType: Weather condition codes
    - HourlyPressureChange: Change in pressure
    - HourlyPressureTendency: Pressure trend
    - HourlyRelativeHumidity: Humidity %
    - HourlySkyConditions: Cloud conditions
    - HourlySeaLevelPressure: Sea level pressure
    - HourlyVisibility: Visibility distance
    - HourlyWetBulbTemperature: Wet bulb temperature
    - HourlyWindDirection: Wind direction in degrees
    - HourlyWindGustSpeed: Wind gust speed
    - HourlyWindSpeed: Wind speed
    """
    print(f"\n=== Loading weather station data from: {input_path} ===")
    print("This may take several minutes...")
    
    # Read all CSV files in the directory
    # Spark will automatically parallelize reading across files
    df = spark.read.csv(
        f"{input_path}/LCD_*.csv",
        header=True,
        inferSchema=False,  # We'll handle types manually for better control
        multiLine=False,
        escape='"'
    )
    
    print(f"✓ Loaded data from CSV files")
    print(f"  Total records (before filtering): {df.count()}")
    
    # Transform to match hist_estaciones_m schema
    df_transformed = df.select(
        # Primary key: station ID and measurement timestamp
        trim(col("STATION")).cast(StringType()).alias("estacion_m"),
        to_timestamp(col("DATE"), "yyyy-MM-dd'T'HH:mm:ss").alias("fecha_medicion"),
        
        # Station metadata
        trim(col("NAME")).cast(StringType()).alias("nombre_estacion_m"),
        clean_numeric_field("LATITUDE").alias("lat"),
        clean_numeric_field("LONGITUDE").alias("lon"),
        clean_numeric_field("ELEVATION").alias("elev"),
        
        # Weather measurements
        clean_numeric_field("HourlyAltimeterSetting").alias("ajuste_barometrico"),
        clean_numeric_field("HourlyDewPointTemperature").alias("temp_punto_rocio"),
        clean_numeric_field("HourlyDryBulbTemperature").alias("temp_ambiente"),
        clean_numeric_field("HourlyPrecipitation").alias("precipitacion"),
        
        # Weather condition (parsed from text codes)
        parse_weather_type("HourlyPresentWeatherType").cast(IntegerType()).alias("tipo_condicion_climatica"),
        
        # Pressure data
        trim(col("HourlyPressureChange")).cast(StringType()).alias("cambio_presion_atmosferica"),
        clean_numeric_field("HourlyPressureTendency").alias("tendencia_presion"),
        clean_numeric_field("HourlyRelativeHumidity").alias("humedad_relativa"),
        
        # Sky and visibility
        trim(col("HourlySkyConditions")).cast(StringType()).alias("condiciones_cielo"),
        clean_numeric_field("HourlySeaLevelPressure").alias("presion_nivel_mar"),
        clean_numeric_field("HourlyVisibility").alias("visibilidad"),
        clean_numeric_field("HourlyWetBulbTemperature").alias("temperatura_bulbo_humedo"),
        
        # Wind data
        clean_numeric_field("HourlyWindDirection").alias("direccion_viento"),
        clean_numeric_field("HourlyWindGustSpeed").alias("velocidad_rafaga_viento"),
        clean_numeric_field("HourlyWindSpeed").alias("velocidad_viento")
    ).filter(
        # Filter out records without required primary key fields
        col("estacion_m").isNotNull() & 
        col("fecha_medicion").isNotNull()
    )
    
    print(f"✓ Transformed data")
    print(f"  Valid records (after transformation): {df_transformed.count()}")
    
    return df_transformed


def write_to_cassandra(df, table_name, mode="append"):
    """
    Write DataFrame to Cassandra table with optimized settings.
    Uses batch writes and partition grouping for high throughput.
    """
    print(f"\n=== Writing to Cassandra table: {table_name} ===")
    print("This will take some time for large datasets...")
    
    (df.write
        .format("org.apache.spark.sql.cassandra")
        .options(
            keyspace=CASSANDRA_KEYSPACE,
            table=table_name
        )
        .mode(mode)
        .save())
    
    print(f"✓ Successfully written to Cassandra table: {table_name}")


def main():
    """Main ETL pipeline for weather station data"""
    print("=" * 80)
    print("WEATHER STATIONS ETL PIPELINE → Cassandra")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  Data Directory: {DATA_DIR}")
    print(f"  Cassandra Host: {CASSANDRA_HOST}")
    print(f"  Cassandra Keyspace: {CASSANDRA_KEYSPACE}")
    print(f"  Target Table: hist_estaciones_m")
    print(f"  Spark Parallelism: {PARALLELISM}")
    print(f"  Cassandra Batch Size: {BATCH_SIZE}")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Check if input directory exists
        if not os.path.exists(DATA_DIR):
            raise FileNotFoundError(
                f"Weather data directory not found: {DATA_DIR}\n"
                f"Please set WEATHER_DATA_DIR environment variable to the correct path."
            )
        
        # Load and transform weather station data
        df_weather = load_weather_stations_data(spark, DATA_DIR)
        
        # Show sample of data for verification
        print("\n=== Sample of transformed data ===")
        df_weather.show(5, truncate=True)
        
        # Write to Cassandra
        write_to_cassandra(df_weather, "hist_estaciones_m", mode="append")
        
        print("\n" + "=" * 80)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\nData written to Cassandra:")
        print("  - Table: hist_estaciones_m")
        print("  - Keyspace: flight_weather_historic")
        print("\nPerformance Tips:")
        print("  - For faster writes, increase CASSANDRA_BATCH_SIZE (default: 1000)")
        print("  - For better parallelism, increase SPARK_PARALLELISM (default: 200)")
        print("  - Monitor Cassandra write throughput and adjust accordingly")
        
    except FileNotFoundError as e:
        print(f"\n❌ File Error: {str(e)}")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Error in ETL pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()