#!/bin/bash

echo "Starting Flink ${FLINK_MODE}..."

if [ "$FLINK_MODE" == "jobmanager" ]; then
    echo "Starting Flink JobManager on port ${FLINK_JOBMANAGER_RPC_PORT:-6123}"
    exec /docker-entrypoint.sh jobmanager
elif [ "$FLINK_MODE" == "taskmanager" ]; then
    echo "Connecting Flink TaskManager to JobManager at ${FLINK_JOBMANAGER_HOST:-flink-jobmanager}:${FLINK_JOBMANAGER_RPC_PORT:-6123}"
    exec /docker-entrypoint.sh taskmanager
else
    echo "Unknown FLINK_MODE: ${FLINK_MODE}"
    exit 1
fi
