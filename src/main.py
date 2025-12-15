from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

def main():
    # ==============================
    # 1. Crear Spark Session
    # ==============================
    spark = SparkSession.builder \
        .appName("FlightDelays-Batch-BI") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ==============================
    # 2. Leer CSV histórico agregado
    # ==============================
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/delays_history_agg.csv")

    print("Schema original:")
    df.printSchema()

    # ==============================
    # 3. Limpieza básica de tipos
    # ==============================
    df_clean = df \
        .withColumn("arr_flights", col("arr_flights").cast("int")) \
        .withColumn("arr_del15", col("arr_del15").cast("int")) \
        .withColumn("arr_cancelled", col("arr_cancelled").cast("int")) \
        .withColumn("arr_diverted", col("arr_diverted").cast("int")) \
        .withColumn("arr_delay", col("arr_delay").cast("double")) \
        .withColumn("avg_arr_delay_min", col("avg_arr_delay_min").cast("double")) \
        .withColumn("carrier_delay", col("carrier_delay").cast("double")) \
        .withColumn("weather_delay", col("weather_delay").cast("double")) \
        .withColumn("nas_delay", col("nas_delay").cast("double")) \
        .withColumn("security_delay", col("security_delay").cast("double")) \
        .withColumn("late_aircraft_delay", col("late_aircraft_delay").cast("double"))

    print("Schema limpio:")
    df_clean.printSchema()

    # ==============================
    # 4. KPI por AEROPUERTO (Página 1 BI)
    # ==============================
    airport_kpi = df_clean.groupBy(
        "airport", "airport_name", "year", "month"
    ).agg(
        sum("arr_flights").alias("total_flights"),
        sum("arr_del15").alias("delayed_flights"),
        avg("avg_arr_delay_min").alias("avg_delay_min"),
        sum("arr_delay").alias("total_delay_min"),
        sum("weather_delay").alias("weather_delay_min")
    )

    # ==============================
    # 5. KPI por AEROLÍNEA (Página 3 BI)
    # ==============================
    carrier_kpi = df_clean.groupBy(
        "carrier", "carrier_name"
    ).agg(
        sum("arr_flights").alias("total_flights"),
        sum("arr_del15").alias("delayed_flights"),
        avg("avg_arr_delay_min").alias("avg_delay_min"),
        sum("arr_delay").alias("total_delay_min")
    )

    # ==============================
    # 6. KPI CLIMA vs RETRASOS (Página 2 BI)
    # ==============================
    weather_kpi = df_clean.groupBy(
        "airport", "airport_name"
    ).agg(
        sum("weather_delay").alias("weather_delay_min"),
        sum("arr_delay").alias("total_delay_min")
    )

    # ==============================
    # 7. Guardar resultados para Power BI
    # ==============================
    airport_kpi.coalesce(1) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("output/airport_kpi")

    carrier_kpi.coalesce(1) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("output/carrier_kpi")

    weather_kpi.coalesce(1) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv("output/weather_kpi")

    print("✔ Job Spark finalizado correctamente")

    spark.stop()


if __name__ == "__main__":
    main()
