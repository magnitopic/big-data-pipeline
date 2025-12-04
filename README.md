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
