# Manual Testing with Schema Registry

This folder helps you test the Schema Registry integration manually.

## Quick Start

### 1. Start Kafka and Schema Registry

```bash
# From project root
uv run khaos cluster-up

# Start Schema Registry
docker-compose -f docker/docker-compose.schema-registry.kraft.yml up -d

# Verify Schema Registry is running
curl http://localhost:8081/subjects
```

### 2. Upload Schema

```bash
cd manual_testing
chmod +x upload-schema.sh

# Upload the order schema
./upload-schema.sh orders-value order-schema.avsc
```

### 3. Verify Schema

```bash
# List subjects
curl http://localhost:8081/subjects

# Get schema details
curl http://localhost:8081/subjects/orders-value/versions/latest | jq
```

### 4. Run Scenario

```bash
# Use the built-in registry-provider scenario
uv run khaos run serialization/registry-provider --duration 30
```

## Files

| File | Description |
|------|-------------|
| `order-schema.avsc` | Sample Avro schema |
| `upload-schema.sh` | Script to upload schema to Schema Registry |

## Upload Custom Schema

```bash
./upload-schema.sh <subject-name> <schema-file>

# Examples:
./upload-schema.sh users-value user-schema.avsc
./upload-schema.sh payments-value payment-schema.avsc
```

## Cleanup

```bash
docker-compose -f docker/docker-compose.schema-registry.kraft.yml down
uv run khaos cluster-down
```
