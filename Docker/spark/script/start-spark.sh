#!/bin/bash

echo "Starting Spark ${SPARK_MODE}..."

if [ "$SPARK_MODE" == "master" ]; then
    echo "Starting Spark Master on port ${SPARK_MASTER_PORT:-7077}"
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$SPARK_MODE" == "worker" ]; then
    echo "Connecting Spark Worker to master at ${SPARK_MASTER_URL}"
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker ${SPARK_MASTER_URL}
else
    echo "Unknown SPARK_MODE: ${SPARK_MODE}"
    exit 1
fi
