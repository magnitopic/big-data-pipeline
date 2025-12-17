-- Cassandra schema tailored to CSVs used by the ETL

CREATE KEYSPACE IF NOT EXISTS flight_delays 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_delays;

-- Fact table: flight delays aggregated per airport/carrier/month
CREATE TABLE IF NOT EXISTS fact_flight_delays (
    airport text,
    carrier text,
    year int,
    month int,
    quarter int,
    airport_name text,
    carrier_name text,
    total_flights double,
    delayed_flights double,
    cancelled_flights double,
    diverted_flights double,
    total_delay_minutes double,
    carrier_delay double,
    weather_delay double,
    nas_delay double,
    security_delay double,
    late_aircraft_delay double,
    delay_rate double,
    cancellation_rate double,
    avg_delay_per_flight double,
    weather_delay_pct double,
    carrier_delay_pct double,
    PRIMARY KEY ((airport, carrier), year, month)
) WITH CLUSTERING ORDER BY (year ASC, month ASC);

-- Dimension: airports
CREATE TABLE IF NOT EXISTS dim_airports (
    airport_code text PRIMARY KEY,
    airport_name text
);

-- Dimension: airlines
CREATE TABLE IF NOT EXISTS dim_airlines (
    carrier_code text PRIMARY KEY,
    carrier_name text
);

-- Aggregation: airport performance
CREATE TABLE IF NOT EXISTS agg_airport_performance (
    airport text,
    airport_name text,
    total_flights double,
    total_delayed_flights double,
    total_cancelled double,
    total_delay_minutes double,
    total_weather_delay double,
    total_carrier_delay double,
    avg_delay_rate double,
    avg_delay_per_flight double,
    weather_impact_score double,
    PRIMARY KEY (airport)
);

-- Aggregation: airline performance
CREATE TABLE IF NOT EXISTS agg_airline_performance (
    carrier text,
    carrier_name text,
    total_flights double,
    total_delayed_flights double,
    total_cancelled double,
    total_delay_minutes double,
    avg_delay_rate double,
    avg_delay_per_flight double,
    total_weather_delay double,
    total_carrier_delay double,
    on_time_rate double,
    reliability_score double,
    PRIMARY KEY (carrier)
);

-- Aggregation: monthly trends
CREATE TABLE IF NOT EXISTS agg_monthly_trends (
    year int,
    month int,
    total_flights double,
    total_delayed_flights double,
    total_delay_minutes double,
    weather_delay_minutes double,
    carrier_delay_minutes double,
    nas_delay_minutes double,
    late_aircraft_delay_minutes double,
    avg_delay_rate double,
    date date,
    PRIMARY KEY (year, month)
) WITH CLUSTERING ORDER BY (month ASC);

-- Aggregation: delay causes breakdown (overall)
CREATE TABLE IF NOT EXISTS agg_delay_causes (
    delay_cause text PRIMARY KEY,
    total_minutes double,
    percentage double
);

-- Aggregation: route performance
CREATE TABLE IF NOT EXISTS agg_route_analysis (
    origin_airport text,
    dest_airport text,
    airline text,
    total_flights int,
    delayed_flights int,
    avg_delay_minutes double,
    weather_delays int,
    carrier_delays int,
    delay_rate double,
    PRIMARY KEY ((origin_airport, dest_airport, airline))
);

-- Fact: flight samples
CREATE TABLE IF NOT EXISTS fact_flight_samples (
    flight_id text,
    flight_date date,
    origin_airport text,
    dest_airport text,
    airline text,
    departure_delay_min double,
    arrival_delay_min double,
    is_delayed int,
    delay_severity text,
    PRIMARY KEY ((flight_id), flight_date)
) WITH CLUSTERING ORDER BY (flight_date DESC);
