# Estructura final del proyecto (tipo)

```
/BIG-DATA-PIPELINE/
│
├── README.md
├── .env.example
├── .gitignore
│
├── /docs/
│   ├── arquitectura_kappa.png
│   ├── arquitectura_kappa.drawio
│   ├── api_endpoints.md
│   ├── data_dictionary.md
│   ├── data_governance.md
│   ├── quality_rules.md
│   ├── monitoring_plan.md
│   ├── report_final.pdf   (Hito 3)
│   └── presentacion.pptx  (Hito 3)
│
├── /docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.spark
│   ├── Dockerfile.flink
│   ├── nifi/
│   │   ├── nifi.conf
│   │   └── templates/
│   │       ├── nifi_ingest_csv.xml
│   │       └── nifi_ingest_apis.xml
│   └── env/
│       └── config.env
│
├── /config/
│   ├── __init__.py
│   ├── api_keys.py
│   └── settings.py
│
├── /nifi/
│   ├── ingest_csv/
│   │   ├── flow_data_ingest_csv.xml
│   │   └── readme_ingest_csv.md
│   ├── ingest_api_opensky/
│   │   ├── flow_opensky.xml
│   │   └── readme_opensky.md
│   └── ingest_api_openweather/
│       ├── flow_openweather.xml
│       └── readme_openweather.md
│
├── /kafka/
│   ├── create_topics.sh
│   ├── topic_schemas/
│   │   ├── flights_all.json
│   │   └── weather_all.json
│   └── sample_messages/
│       ├── flights_sample.json
│       └── weather_sample.json
│
├── /spark/
│   ├── batch_base.py
│   ├── batch_cleaning.py
│   ├── batch_enrichment.py
│   ├── batch_join_delays_weather.py
│   ├── batch_to_mysql.py
│   └── utils/
│       ├── schema_definitions.py
│       └── validations.py
│
├── /flink/
│   ├── flink_base_job.py
│   ├── flink_join_weather_flights.py
│   ├── flink_kpis_realtime.py
│   ├── flink_reprocess_from_offset.py
│   └── utils/
│       ├── serializers.py
│       └── windowing_strategies.py
│
├── /sql/
│   ├── create_mysql_schema.sql
│   ├── create_mongo_collections.md
│   ├── create_cassandra_schema.cql
│   └── sample_queries.sql
│
├── /powerbi/
│   ├── dashboard.pbix
│   ├── screenshots/
│   │   ├── page1_airports.png
│   │   ├── page2_weather_vs_delays.png
│   │   └── page3_airlines.png
│   └── documentation.md
│
├── /tests/
│   ├── test_weather_api.py
│   ├── test_opensky_api.py
│   ├── test_kafka_connection.py
│   ├── test_pyspark.py
│   └── test_flink_job.py
│
└── /data/
    ├── input/
    │   ├── delays_history_agg.csv
    │   ├── delays_history_sample.csv
    │   └── weather_history.csv
    └── output/
        ├── curated/
        └── analytics/

```

# RETO BIG DATA: METEOROLOGÍA Y RETRASOS DE VUELOS

## Docker containers

How to run?

```bash
docker compose -f docker-compose.yml up -d
```

## Services

### Nifi

```
https://localhost:8443/nifi
```

### Spark

```
spark://localhost:7077
Master: http://localhost:8081/
Worker: http://localhost:8082/
```

### MySQL

```
mysql://localhost:3306
```

### Mongodb

```
mongodb://localhost:27017
```

### HDFS

```
hdfs://localhost:8020
```

### Flink

```
http://localhost:8083
```