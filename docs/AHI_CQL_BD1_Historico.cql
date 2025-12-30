CREATE KEYSPACE IF NOT EXISTS flight_weather_historic
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_weather_historic;

CREATE TABLE IF NOT EXISTS hist_retraso_vuelos (
    aerolinea TEXT,
    ciudad_origen TEXT,
    ciudad_destino TEXT,
    desviado BOOLEAN,
    cancelado BOOLEAN,
    codigo_cancelacion TEXT,
    fecha_vuelo DATE,
    salida_programada TIMESTAMP,
    salida_real TIMESTAMP,
    llegada_programada TIMESTAMP,
    llegada_real TIMESTAMP,
    retraso_salida INT,
    retraso_llegada INT,
    tiempo_programado_vuelo INT,
    tiempo_real_vuelo INT,
    retraso_clima FLOAT,
    retraso_nas FLOAT,
    retraso_aerolinea FLOAT,
    retraso_aeronave_tardia FLOAT,
    retraso_seguridad FLOAT,
    PRIMARY KEY ((fecha_vuelo, ciudad_origen), aerolinea)
);

CREATE TABLE IF NOT EXISTS hist_retraso_vuelos_muestra (
    id_vuelo TEXT PRIMARY KEY,
    codigo_aeropuerto_origen TEXT,
    codigo_aeropuerto_destino TEXT,
    aerolinea TEXT,
    fecha_vuelo DATE,
    salida_programada TIMESTAMP,
    salida_real TIMESTAMP,
    llegada_programada TIMESTAMP,
    llegada_real TIMESTAMP,
    retraso_salida INT,
    retraso_llegada INT,
    causa_retraso TEXT
);

CREATE TABLE IF NOT EXISTS hist_retraso_mes (
    fecha DATE,
    codigo_aeropuerto TEXT,
    aeropuerto TEXT,
    codigo_aerolinea TEXT,
    aerolinea TEXT,
    vuelos_llegados INT,
    vuelos_retraso_15min INT,
    vuelos_desviados INT,
    vuelos_cancelados INT,
    retrasos_clima FLOAT,
    retrasos_nas FLOAT,
    retrasos_aerolinea FLOAT,
    retrasos_aeronave_tardia FLOAT,
    retrasos_seguridad FLOAT,
    total_retrasos FLOAT,
    retraso_promedio_llegada FLOAT,
    pct_vuelos_retraso_15min FLOAT,
    PRIMARY KEY ((fecha, codigo_aeropuerto), codigo_aerolinea)
);

CREATE TABLE IF NOT EXISTS hist_estaciones_m (
    estacion_m TEXT,
    fecha_medicion TIMESTAMP,
    nombre_estacion_m TEXT,
    lat FLOAT,
    lon FLOAT,
    elev FLOAT,
    ajuste_barometrico FLOAT,
    temp_punto_rocio FLOAT,
    temp_ambiente FLOAT,
    precipitacion FLOAT,
    tipo_condicion_climatica INT,
    cambio_presion_atmosferica TEXT,
    tendencia_presion FLOAT,
    humedad_relativa FLOAT,
    condiciones_cielo TEXT,
    presion_nivel_mar FLOAT,
    visibilidad FLOAT,
    temperatura_bulbo_humedo FLOAT,
    direccion_viento FLOAT,
    velocidad_rafaga_viento FLOAT,
    velocidad_viento FLOAT,
    PRIMARY KEY (estacion_m, fecha_medicion)
) WITH CLUSTERING ORDER BY (fecha_medicion DESC);
