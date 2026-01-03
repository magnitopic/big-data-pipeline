#!/bin/bash
set -eo pipefail

echo "Starting Spark ${SPARK_MODE:-<unset>}..."

if [ "${SPARK_MODE:-}" = "master" ]; then
    echo "Launching Spark Master..."
    exec /opt/spark/bin/spark-class \
        org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 \
        --port 7077 \
        --webui-port 8080

elif [ "${SPARK_MODE:-}" = "worker" ]; then
    if [ -z "${SPARK_MASTER_URL:-}" ]; then
        echo "ERROR: SPARK_MASTER_URL is not set"
        exit 1
    fi

    echo "Launching Spark Worker connecting to ${SPARK_MASTER_URL}"
    exec /opt/spark/bin/spark-class \
        org.apache.spark.deploy.worker.Worker \
        "${SPARK_MASTER_URL}"

else
    echo "ERROR: Unknown SPARK_MODE='${SPARK_MODE:-<unset>}'"
    exit 1
fi
