CREATE KEYSPACE IF NOT EXISTS flight_weather_historic
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_weather_historic;

CREATE TABLE IF NOT EXISTS airline_delay_cause (
    year int,
    month int,
    airport text,
    carrier text,
    airport_name text,
    carrier_name text,
    carrier_ct float,
    weather_ct float,
    nas_ct float,
    security_ct float,
    late_aircraft_ct float,
    arr_flights int,
    arr_del15 int,
    arr_cancelled int,
    arr_diverted int,
    arr_delay float,
    carrier_delay float,
    weather_delay float,
    nas_delay float,
    security_delay float,
    late_aircraft_delay float,
    PRIMARY KEY ((year, month, airport), carrier)
);

CREATE TABLE IF NOT EXISTS delays_history_agg (
    year int,
    month int,
    airport text,
    carrier text,
    airport_name text,
    carrier_name text,
    avg_arr_delay_min float,
    pct_arr_del15 float,
    arr_flights int,
    arr_del15 int,
    arr_cancelled int,
    arr_diverted int,
    arr_delay float,
    carrier_delay float,
    weather_delay float,
    nas_delay float,
    security_delay float,
    late_aircraft_delay float,
    PRIMARY KEY ((year, month, airport), carrier)
);

CREATE TABLE IF NOT EXISTS delays_history_sample (
    flight_id text PRIMARY KEY,
    origin_airport text,
    dest_airport text,
    airline text,
    flight_date date,
    scheduled_departure timestamp,
    actual_departure timestamp,
    scheduled_arrival timestamp,
    actual_arrival timestamp,
    departure_delay_min int,
    arrival_delay_min int,
    delay_cause text
);

CREATE TABLE IF NOT EXISTS weather_lcd (
    station text,
    date timestamp,
    name text,
    latitude float,
    longitude float,
    elevation float,
    hourly_altimeter_setting float,
    hourly_dew_point_temperature float,
    hourly_dry_bulb_temperature float,
    hourly_precipitation float,
    hourly_present_weather_type int,
    hourly_pressure_change text,
    hourly_pressure_tendency float,
    hourly_relative_humidity float,
    hourly_sky_conditions text,
    hourly_sea_level_pressure float,
    hourly_visibility float,
    hourly_wet_bulb_temperature float,
    hourly_wind_direction float,
    hourly_wind_gust_speed float,
    hourly_wind_speed float,
    PRIMARY KEY (station, date)
) WITH CLUSTERING ORDER BY (date DESC);
