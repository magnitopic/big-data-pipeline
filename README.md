# Big Data Pipeline: Flight Delays & Weather Analysis

A big data pipeline that processes flight delay data combined with weather information using both historical and real-time streaming data.

## Overview

This project analyzes the relationship between meteorological conditions and flight delays using a modern big data architecture. We process data from multiple sources and combine historical records with live streaming data to generate insights.

## What We Do

- **Ingest** historical flight delays and weather data from CSV files
- **Stream** real-time flight and weather data via APIs
- **Process** data using Apache Spark for batch jobs and Flink for stream processing
- **Store** results in Cassandra, MySQL, and other databases
- **Analyze** patterns between weather events and flight delays

## Key Technologies

- **Docker** - Containerized deployment of all services
- **Python** - Data processing and pipeline orchestration
- **Apache Spark** - Batch data processing and transformations
- **NiFi** - Data ingestion and workflow automation
- **Apache Cassandra** - Distributed database for time-series data

## Data Sources

### Historical Data
- CSV files with flight delay history
- Historical weather records
- Stored in `/data/` directory

### Streaming Data
- Real-time flight information from OpenSky API
- Live weather updates from OpenWeather API
- Continuously ingested via NiFi

## Quick Start

### Using Make Commands

| Command | Description |
|---------|-------------|
| `make build` | Build and start all containers |
| `make restart` | Restart containers |
| `make stop` | Stop running containers |
| `make down` | Stop and remove containers |
| `make re` | Full reset (destroy and rebuild) |
| `make historic` | Run historical data ETL job |
| `make streaming` | Start real-time streaming pipeline |

### Manual Docker

Start all services with Docker directly:

```bash
docker compose -f docker-compose.yml up -d
```

## Service Endpoints

| Service | URL |
|---------|-----|
| NiFi | https://localhost:8443/nifi |
| Spark Master | http://localhost:8081 |
| Spark Worker | http://localhost:8082 |
| Cassandra | localhost:9042 |
| MySQL | localhost:3306 |
| MongoDB | localhost:27017 |

## Project Structure

- `/src/` - Python processing scripts
- `/config/` - Configuration and API keys
- `/data/` - Input and output data
- `/Docker/` - Docker and container configurations
- `/tests/` - Test suite
- `/docs/` - Documentation and schemas
