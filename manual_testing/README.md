# Manual testing: Schema Registry

The automated tests cover the Schema Registry paths against `srfake`, an in-memory registry,
and the Kafka side against kfake. Neither is a real Confluent Schema Registry, so this folder
is how you check the combination by hand: a schema registered out of band, fetched by subject,
used to generate and encode records, and its id written into the Confluent wire header.

## Prerequisites

Docker, `curl` and `jq`, plus a binary built at the repo root:

```bash
go build -o khaos ./cmd/khaos
```

## 1. Start Kafka and Schema Registry

```bash
# From the project root
./khaos cluster-up --schema-registry

# Verify Schema Registry is running
curl http://localhost:8081/subjects
```

`--schema-registry` is only on `cluster-up`. Starting the cluster this way first is what makes
the rest work: `khaos run` would otherwise start the cluster itself, without a registry.

## 2. Upload a schema

```bash
cd manual_testing
./upload-schema.sh orders-value order-schema.avsc
```

Both arguments are optional and default to exactly those values. The registry URL comes from
`$SCHEMA_REGISTRY_URL`, defaulting to `http://localhost:8081`.

## 3. Verify the schema landed

```bash
# List subjects
curl http://localhost:8081/subjects

# Get schema details
curl http://localhost:8081/subjects/orders-value/versions/latest | jq
```

## 4. Run a scenario against the registered schema

```bash
./khaos run serialization/registry-provider -d 30 -k
```

`-k` matters here. Without it `khaos run` stops the cluster when the scenario ends, including
the one you started by hand, and the registry goes with it.

`scenarios/serialization/registry-provider.yaml` sets `schema_provider: registry` and
`subject_name: orders-value` for its `orders` topic, so the schema you uploaded is what decides
both the fields khaos generates and how the records are encoded. The subject is read, never
re-registered, so a run cannot mutate it. Watch for Avro-specific breakage: the sample schema
carries a `uuid` logical type on a string, an enum, and a `timestamp-millis` long.

## Files

| File | Description |
|------|-------------|
| `order-schema.avsc` | Sample Avro record: uuid, enum and timestamp-millis fields. |
| `upload-schema.sh` | Registers a schema file under a subject via the registry's REST API. |

## Upload a custom schema

```bash
./upload-schema.sh <subject-name> <schema-file>

# Examples:
./upload-schema.sh users-value user-schema.avsc
./upload-schema.sh payments-value payment-schema.avsc
```

Whatever subject you pick has to match the `subject_name` of the topic in your scenario file.
When khaos registers an inline schema itself it uses the Confluent TopicNameStrategy -- topic
`orders` becomes subject `orders-value` -- so following that convention here keeps the two
directions consistent.

## Cleanup

```bash
./khaos cluster-down
```
