1. **Reset** 

1.1 Cassandra

docker compose stop cassandra

docker volume rm big-data-pipeline_cassandra-data

docker compose up -d cassandra

1.2 Total

docker compose down -v

docker system prune -af --volumes

docker compose up -d

**2. Kafka – Crear / verificar topic**

docker exec -it kafka bash

kafka-topics --bootstrap-server kafka:9092 --list

Si no existe:

kafka-topics --bootstrap-server kafka:9092 --create --topic opensky.usa.raw --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server kafka:9092 --create --topic openweather.usa.raw --partitions 1 --replication-factor 1

**3. Proton – Activar VPN**

**3. NiFi – Arrancar flujos**

**4. Spark – Arrancar .py**

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-cores 6 --total-executor-cores 6 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/spark-apps/flights_streaming.py

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-cores 6 --total-executor-cores 6 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/spark-apps/weather_streaming.py

**5. Cassandra – Comprobar base de datos**

docker exec -it cassandra cqlsh

USE flight_weather_streaming;

SELECT COUNT(*) FROM flights_rt;  

SELECT * FROM flights_rt LIMIT 5;

SELECT COUNT(*) FROM weather_rt;

SELECT * FROM weather_rt LIMIT 5;

Debe incrementar cada minuto

(Si no funciona decírselo a Hugo)
