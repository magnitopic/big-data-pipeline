#!/bin/bash

set -euo pipefail

echo "Starting Flink ${FLINK_MODE}..."

ENTRYPOINT_BIN="/usr/local/bin/docker-entrypoint.sh"
if [ ! -x "$ENTRYPOINT_BIN" ] && [ -x "/docker-entrypoint.sh" ]; then
  ENTRYPOINT_BIN="/docker-entrypoint.sh"
fi

if [ "${FLINK_MODE:-}" = "jobmanager" ]; then
    echo "Starting Flink JobManager on port ${FLINK_JOBMANAGER_RPC_PORT:-6123}"
    exec "$ENTRYPOINT_BIN" jobmanager
elif [ "${FLINK_MODE:-}" = "taskmanager" ]; then
    echo "Connecting Flink TaskManager to JobManager at ${FLINK_JOBMANAGER_HOST:-flink-jobmanager}:${FLINK_JOBMANAGER_RPC_PORT:-6123}"
    exec "$ENTRYPOINT_BIN" taskmanager
else
    echo "Unknown FLINK_MODE: ${FLINK_MODE:-<unset>}"
    exit 1
fi
