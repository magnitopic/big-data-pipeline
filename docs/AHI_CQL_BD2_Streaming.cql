CREATE KEYSPACE IF NOT EXISTS flight_weather_streaming
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_weather_streaming;

CREATE TABLE IF NOT EXISTS flights_rt (
    icao24 text,
    time_position date,
    callsign text,
    origin_country text,
    longitude float,
    latitude float,
    baro_altitude float,
    on_ground boolean,
    velocity float,
    true_track float,
    geo_altitude float,
    PRIMARY KEY ((icao24), time_position)
) WITH CLUSTERING ORDER BY (time_position DESC);

CREATE TABLE IF NOT EXISTS weather_rt (
    lat float,
    lon float,
    dt date,
    temp float,
    humidity int,
    clouds int,
    visibility int,
    wind_speed float,
    wind_deg int,
    wind_gust float,
    rain_1h float,
    snow_1h float,
    weather_main text,
    weather_description text,
    PRIMARY KEY ((lat, lon), dt)
) WITH CLUSTERING ORDER BY (dt DESC);