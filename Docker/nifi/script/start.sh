#!/bin/bash
# NiFi startup script

set -e

echo "Starting NiFi..."
echo "NiFi will be available at: https://localhost:8443/nifi"
echo "Username: ${SINGLE_USER_CREDENTIALS_USERNAME:-admin}"
echo "Password: ${SINGLE_USER_CREDENTIALS_PASSWORD}"

# Start NiFi
/opt/nifi/scripts/start.sh
