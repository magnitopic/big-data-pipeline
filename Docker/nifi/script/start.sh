#!/bin/bash
# NiFi startup script with auto-import

set -e

echo "Starting NiFi..."
echo "NiFi will be available at: https://localhost:8443/nifi"
echo "Username: ${SINGLE_USER_CREDENTIALS_USERNAME:-admin}"
echo "Password: ${SINGLE_USER_CREDENTIALS_PASSWORD}"

# Start NiFi in background
/opt/nifi/scripts/start.sh &

# Wait for NiFi to initialize
sleep 5

# Import flow configuration in background
if [ -f "/opt/nifi/nifi-current/conf/scripts/import-flow.sh" ]; then
    echo "Flow import will start automatically once NiFi is ready..."
    /opt/nifi/nifi-current/conf/scripts/import-flow.sh &
fi

# Keep container running and follow NiFi logs
tail -f /opt/nifi/nifi-current/logs/nifi-app.log
