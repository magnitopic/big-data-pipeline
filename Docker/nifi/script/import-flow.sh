#!/bin/bash
# Script to import NiFi flow configuration from JSON

set -e

NIFI_API_URL="https://localhost:8443/nifi-api"
FLOW_FILE="/opt/nifi/nifi-current/templates/flow.json"
MAX_RETRIES=30
RETRY_INTERVAL=10

echo "===================================="
echo "NiFi Flow Auto-Import Script"
echo "===================================="

# Function to wait for NiFi to be ready
wait_for_nifi() {
    echo "Waiting for NiFi to start..."
    local count=0
    while [ $count -lt $MAX_RETRIES ]; do
        if curl -fk --connect-timeout 5 "${NIFI_API_URL}/flow/about" > /dev/null 2>&1; then
            echo "✓ NiFi is ready!"
            return 0
        fi
        count=$((count + 1))
        echo "Attempt $count/$MAX_RETRIES - NiFi not ready yet, waiting ${RETRY_INTERVAL}s..."
        sleep $RETRY_INTERVAL
    done
    echo "✗ NiFi failed to start within expected time"
    return 1
}

# Function to get root process group ID
get_root_pg_id() {
    curl -fk "${NIFI_API_URL}/flow/process-groups/root" 2>/dev/null | jq -r '.processGroupFlow.id'
}

# Function to create controller services
create_controller_services() {
    local root_pg_id=$1
    echo "Creating controller services..."
    
    # Create CSVReader service
    echo "Creating CSVReader service..."
    local csv_reader_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/controller-services" \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.csv.CSVReader",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-record-serialization-services-nar",
                    "version": "2.5.0"
                },
                "name": "CSVReader",
                "properties": {
                    "schema-access-strategy": "csv-header-derived",
                    "csv-format": "RFC4180",
                    "Skip Header Line": "true",
                    "csvutils-character-set": "UTF-8",
                    "Trim Fields": "true"
                }
            }
        }' 2>/dev/null)
    
    local csv_reader_id=$(echo "$csv_reader_response" | jq -r '.id')
    echo "✓ CSVReader created with ID: $csv_reader_id"
    
    # Create JsonRecordSetWriter service
    echo "Creating JsonRecordSetWriter service..."
    local json_writer_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/controller-services" \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.json.JsonRecordSetWriter",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-record-serialization-services-nar",
                    "version": "2.5.0"
                },
                "name": "JsonRecordSetWriter",
                "properties": {
                    "schema-access-strategy": "inherit-record-schema",
                    "Schema Write Strategy": "no-schema",
                    "output-grouping": "output-oneline",
                    "Pretty Print JSON": "false"
                }
            }
        }' 2>/dev/null)
    
    local json_writer_id=$(echo "$json_writer_response" | jq -r '.id')
    echo "✓ JsonRecordSetWriter created with ID: $json_writer_id"
    
    # Enable CSVReader
    sleep 2
    curl -fk -X PUT "${NIFI_API_URL}/controller-services/${csv_reader_id}/run-status" \
        -H "Content-Type: application/json" \
        -d "{\"revision\": {\"version\": 0}, \"state\": \"ENABLED\"}" > /dev/null 2>&1
    echo "✓ CSVReader enabled"
    
    # Enable JsonRecordSetWriter
    sleep 2
    curl -fk -X PUT "${NIFI_API_URL}/controller-services/${json_writer_id}/run-status" \
        -H "Content-Type: application/json" \
        -d "{\"revision\": {\"version\": 0}, \"state\": \"ENABLED\"}" > /dev/null 2>&1
    echo "✓ JsonRecordSetWriter enabled"
    
    # Return the IDs
    echo "${csv_reader_id}:${json_writer_id}"
}

# Function to create processors
create_processors() {
    local root_pg_id=$1
    local csv_reader_id=$2
    local json_writer_id=$3
    
    echo "Creating processors..."
    
    # Create GetFile processor
    echo "Creating GetFile processor..."
    local getfile_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/processors" \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.processors.standard.GetFile",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-standard-nar",
                    "version": "2.5.0"
                },
                "name": "GetFile - Read CSV",
                "position": {"x": 200, "y": 200},
                "config": {
                    "properties": {
                        "Input Directory": "/opt/nifi/data",
                        "File Filter": ".*\\\\.csv",
                        "Keep Source File": "true",
                        "Recurse Subdirectories": "false",
                        "Polling Interval": "30 sec",
                        "Batch Size": "10",
                        "Ignore Hidden Files": "true"
                    },
                    "schedulingPeriod": "30 sec",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "autoTerminatedRelationships": []
                }
            }
        }' 2>/dev/null)
    local getfile_id=$(echo "$getfile_response" | jq -r '.id')
    echo "✓ GetFile created with ID: $getfile_id"
    
    # Create ConvertRecord processor
    echo "Creating ConvertRecord processor..."
    local convert_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/processors" \
        -H "Content-Type: application/json" \
        -d "{
            \"revision\": {\"version\": 0},
            \"component\": {
                \"type\": \"org.apache.nifi.processors.standard.ConvertRecord\",
                \"bundle\": {
                    \"group\": \"org.apache.nifi\",
                    \"artifact\": \"nifi-standard-nar\",
                    \"version\": \"2.5.0\"
                },
                \"name\": \"ConvertRecord - CSV to JSON\",
                \"position\": {\"x\": 200, \"y\": 400},
                \"config\": {
                    \"properties\": {
                        \"record-reader\": \"${csv_reader_id}\",
                        \"record-writer\": \"${json_writer_id}\"
                    },
                    \"autoTerminatedRelationships\": []
                }
            }
        }" 2>/dev/null)
    local convert_id=$(echo "$convert_response" | jq -r '.id')
    echo "✓ ConvertRecord created with ID: $convert_id"
    
    # Create PublishKafka processor
    echo "Creating PublishKafka processor..."
    local kafka_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/processors" \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.kafka.processors.PublishKafka",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-kafka-2-6-nar",
                    "version": "2.5.0"
                },
                "name": "PublishKafka - Send to Kafka",
                "position": {"x": 200, "y": 600},
                "config": {
                    "properties": {
                        "bootstrap.servers": "kafka:29092",
                        "topic": "csv-data",
                        "acks": "1",
                        "compression.type": "none",
                        "security.protocol": "PLAINTEXT"
                    },
                    "autoTerminatedRelationships": []
                }
            }
        }' 2>/dev/null)
    local kafka_id=$(echo "$kafka_response" | jq -r '.id')
    echo "✓ PublishKafka created with ID: $kafka_id"
    
    # Create LogAttribute for success
    echo "Creating LogAttribute for success..."
    local logsuccess_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/processors" \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.processors.standard.LogAttribute",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-standard-nar",
                    "version": "2.5.0"
                },
                "name": "LogAttribute - Success",
                "position": {"x": 200, "y": 800},
                "config": {
                    "properties": {
                        "Log Level": "info",
                        "Log Payload": "false",
                        "Log prefix": "SUCCESS: "
                    },
                    "autoTerminatedRelationships": ["success"]
                }
            }
        }' 2>/dev/null)
    local logsuccess_id=$(echo "$logsuccess_response" | jq -r '.id')
    echo "✓ LogAttribute (success) created with ID: $logsuccess_id"
    
    # Create LogAttribute for failures
    echo "Creating LogAttribute for failures..."
    local logfail_response=$(curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/processors" \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.processors.standard.LogAttribute",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-standard-nar",
                    "version": "2.5.0"
                },
                "name": "LogAttribute - Failure",
                "position": {"x": 600, "y": 600},
                "config": {
                    "properties": {
                        "Log Level": "error",
                        "Log Payload": "true",
                        "Log prefix": "FAILURE: "
                    },
                    "autoTerminatedRelationships": ["success"]
                }
            }
        }' 2>/dev/null)
    local logfail_id=$(echo "$logfail_response" | jq -r '.id')
    echo "✓ LogAttribute (failure) created with ID: $logfail_id"
    
    echo "${getfile_id}:${convert_id}:${kafka_id}:${logsuccess_id}:${logfail_id}"
}

# Function to create connections
create_connections() {
    local root_pg_id=$1
    local getfile_id=$2
    local convert_id=$3
    local kafka_id=$4
    local logsuccess_id=$5
    local logfail_id=$6
    
    echo "Creating connections..."
    
    # GetFile -> ConvertRecord
    curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/connections" \
        -H "Content-Type: application/json" \
        -d "{
            \"revision\": {\"version\": 0},
            \"component\": {
                \"source\": {\"id\": \"${getfile_id}\", \"type\": \"PROCESSOR\"},
                \"destination\": {\"id\": \"${convert_id}\", \"type\": \"PROCESSOR\"},
                \"selectedRelationships\": [\"success\"]
            }
        }" > /dev/null 2>&1
    echo "✓ Connection: GetFile -> ConvertRecord"
    
    # ConvertRecord -> PublishKafka (success)
    curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/connections" \
        -H "Content-Type: application/json" \
        -d "{
            \"revision\": {\"version\": 0},
            \"component\": {
                \"source\": {\"id\": \"${convert_id}\", \"type\": \"PROCESSOR\"},
                \"destination\": {\"id\": \"${kafka_id}\", \"type\": \"PROCESSOR\"},
                \"selectedRelationships\": [\"success\"]
            }
        }" > /dev/null 2>&1
    echo "✓ Connection: ConvertRecord -> PublishKafka"
    
    # ConvertRecord -> LogFailure (failure)
    curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/connections" \
        -H "Content-Type: application/json" \
        -d "{
            \"revision\": {\"version\": 0},
            \"component\": {
                \"source\": {\"id\": \"${convert_id}\", \"type\": \"PROCESSOR\"},
                \"destination\": {\"id\": \"${logfail_id}\", \"type\": \"PROCESSOR\"},
                \"selectedRelationships\": [\"failure\"]
            }
        }" > /dev/null 2>&1
    echo "✓ Connection: ConvertRecord -> LogFailure"
    
    # PublishKafka -> LogSuccess (success)
    curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/connections" \
        -H "Content-Type: application/json" \
        -d "{
            \"revision\": {\"version\": 0},
            \"component\": {
                \"source\": {\"id\": \"${kafka_id}\", \"type\": \"PROCESSOR\"},
                \"destination\": {\"id\": \"${logsuccess_id}\", \"type\": \"PROCESSOR\"},
                \"selectedRelationships\": [\"success\"]
            }
        }" > /dev/null 2>&1
    echo "✓ Connection: PublishKafka -> LogSuccess"
    
    # PublishKafka -> LogFailure (failure)
    curl -fk -X POST "${NIFI_API_URL}/process-groups/${root_pg_id}/connections" \
        -H "Content-Type: application/json" \
        -d "{
            \"revision\": {\"version\": 0},
            \"component\": {
                \"source\": {\"id\": \"${kafka_id}\", \"type\": \"PROCESSOR\"},
                \"destination\": {\"id\": \"${logfail_id}\", \"type\": \"PROCESSOR\"},
                \"selectedRelationships\": [\"failure\"]
            }
        }" > /dev/null 2>&1
    echo "✓ Connection: PublishKafka -> LogFailure"
}

# Main execution
main() {
    if ! wait_for_nifi; then
        echo "✗ NiFi startup failed"
        exit 1
    fi
    
    echo ""
    local root_pg_id=$(get_root_pg_id)
    if [ -z "$root_pg_id" ] || [ "$root_pg_id" == "null" ]; then
        echo "✗ Failed to get root process group ID"
        exit 1
    fi
    
    echo "Root Process Group ID: $root_pg_id"
    echo ""
    
    # Check if flow already exists
    local existing=$(curl -fk "${NIFI_API_URL}/flow/process-groups/root" 2>/dev/null | jq -r '.processGroupFlow.flow.processors | length')
    if [ "$existing" != "0" ] && [ "$existing" != "null" ]; then
        echo "⚠ Flow already exists. Skipping import."
        exit 0
    fi
    
    # Create controller services
    local service_ids=$(create_controller_services "$root_pg_id")
    local csv_reader_id=$(echo "$service_ids" | cut -d: -f1)
    local json_writer_id=$(echo "$service_ids" | cut -d: -f2)
    
    echo ""
    sleep 3
    
    # Create processors
    local processor_ids=$(create_processors "$root_pg_id" "$csv_reader_id" "$json_writer_id")
    IFS=':' read -r getfile_id convert_id kafka_id logsuccess_id logfail_id <<< "$processor_ids"
    
    echo ""
    sleep 2
    
    # Create connections
    create_connections "$root_pg_id" "$getfile_id" "$convert_id" "$kafka_id" "$logsuccess_id" "$logfail_id"
    
    echo ""
    echo "===================================="
    echo "✓ Flow created successfully!"
    echo "===================================="
    echo ""
    echo "Flow Details:"
    echo "  - Reads CSV files from: /opt/nifi/data"
    echo "  - Converts to JSON format"
    echo "  - Publishes to Kafka topic: csv-data"
    echo ""
    echo "Access NiFi at: https://localhost:8443/nifi"
    echo "Note: Processors are stopped. Start them when ready."
}

main
