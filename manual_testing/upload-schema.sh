#!/usr/bin/env bash
set -e

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SUBJECT="${1:-orders-value}"
SCHEMA_FILE="${2:-order-schema.avsc}"

if [ ! -f "$SCHEMA_FILE" ]; then
    echo "Error: Schema file '$SCHEMA_FILE' not found"
    exit 1
fi

# Read and escape the schema for JSON
SCHEMA=$(cat "$SCHEMA_FILE" | jq -c '.')

echo "Uploading schema to subject: $SUBJECT"
echo "Schema Registry URL: $SCHEMA_REGISTRY_URL"

# Register the schema
RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schemaType\": \"AVRO\", \"schema\": $(echo "$SCHEMA" | jq -Rs '.')}" \
    "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions")

echo "Response: $RESPONSE"

# Check if successful
if echo "$RESPONSE" | jq -e '.id' > /dev/null 2>&1; then
    SCHEMA_ID=$(echo "$RESPONSE" | jq '.id')
    echo "Schema registered successfully with ID: $SCHEMA_ID"
else
    echo "Error registering schema"
    exit 1
fi
