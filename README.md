<div align="center">
  <img src="assets/logo.png" alt="khaos logo" width="200">
  <h1>khaos</h1>
  <p>Kafka traffic generator - realistic workloads for testing, learning, and chaos engineering</p>
</div>

## Use Cases

- **Learning Stream Processing**: Generate Kafka traffic, connect your Spark/Flink/Kafka Streams app to consume
- **Testing Monitoring & Alerts**: Validate your Grafana dashboards and alerting rules with real traffic
- **Load Testing**: Benchmark Kafka cluster performance at different throughput levels
- **Development & Testing**: Get realistic Kafka data for local development without production access
- **Chaos Engineering**: Simulate failures, rebalances, and backpressure scenarios

## Features

- **One-Command Setup**: Spin up a 3-broker Kafka cluster with traffic in seconds
- **YAML-Based Scenarios**: Define traffic patterns declaratively, no code required
- **Producer-Only Mode**: Generate data without built-in consumers (`--no-consumers`)
- **External Cluster Support**: Connect to any Kafka cluster (Confluent Cloud, MSK, self-hosted)
- **Chaos Engineering**: Built-in incident primitives (backpressure, rebalances, broker failures)
- **Full Authentication**: SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, SSL/TLS, mTLS
- **Live Stats Display**: Real-time producer/consumer metrics
- **Web UI**: Redpanda Console at localhost:8080 for cluster inspection

## Requirements

- Python 3.11+
- Docker and Docker Compose (for local cluster)
- [uv](https://docs.astral.sh/uv/) package manager

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/khaos.git
cd khaos

# Install dependencies
uv sync
```

### Development Setup

```bash
# Install dependencies
uv sync

# Install pre-commit hooks (required for contributors)
uv run pre-commit install

# Run linting/formatting manually
uv run ruff check --fix .
uv run ruff format .
```

## Quick Start

```bash
# Run a scenario (auto-starts local Kafka cluster)
uv run khaos run high-throughput

# Press Ctrl+C to stop
```

---

## CLI Reference

### Commands Overview

| Command | Description |
|---------|-------------|
| `khaos cluster-up` | Start the 3-broker Kafka cluster |
| `khaos cluster-down` | Stop the Kafka cluster |
| `khaos cluster-status` | Show Kafka cluster status |
| `khaos list` | List available traffic scenarios |
| `khaos validate` | Validate scenario YAML definitions |
| `khaos run` | Run scenarios on local Docker cluster |
| `khaos simulate` | Run scenarios on external Kafka cluster |

### `run` vs `simulate`

Both commands execute the **same YAML scenarios** with the same traffic patterns and incident triggers. The difference is where they run:

| | `run` | `simulate` |
|---|---|---|
| **Target cluster** | Local Docker (auto-managed) | Any external Kafka cluster |
| **Docker management** | Auto starts/stops cluster | No Docker interaction |
| **Authentication** | None needed | Full support (SASL, SSL, mTLS) |
| **Broker incidents** | Full support (`stop_broker`, `start_broker`) | Skipped (cannot control external brokers) |
| **All other incidents** | Full support | Full support |

**When to use `run`:**
- Local development and testing
- Full chaos engineering with broker failure simulation
- Quick experiments without external dependencies

**When to use `simulate`:**
- Load testing external clusters (Confluent Cloud, AWS MSK, self-hosted)
- Testing authentication configurations
- Running chaos scenarios on staging/production environments
- When you need broker incidents skipped (they're automatically skipped with a warning)

---

### Cluster Modes

khaos supports two Kafka deployment modes:

| Mode | Description |
|------|-------------|
| `kraft` | **Default.** Modern KRaft mode (no ZooKeeper) - Kafka 3.x+ |
| `zookeeper` | Legacy ZooKeeper mode - for testing older deployments |

Both modes run the same 3-broker cluster with identical ports and capabilities.

---

### `khaos cluster-up`

Start the 3-broker Kafka cluster in Docker.

```bash
# Start with KRaft mode (default)
uv run khaos cluster-up

# Start with ZooKeeper mode
uv run khaos cluster-up --mode zookeeper
uv run khaos cluster-up -m zookeeper
```

**Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--mode` | `-m` | `kraft` | Cluster mode: `kraft` or `zookeeper` |

This starts:
- 3 Kafka brokers (kafka-1, kafka-2, kafka-3)
- ZooKeeper (only in zookeeper mode)
- Redpanda Console at http://localhost:8080

---

### `khaos cluster-down`

Stop the Kafka cluster.

```bash
# Stop cluster (keep data)
uv run khaos cluster-down

# Stop cluster and remove all data volumes
uv run khaos cluster-down --volumes
uv run khaos cluster-down -v
```

**Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--volumes` | `-v` | Remove data volumes |

---

### `khaos cluster-status`

Show the status of Kafka containers.

```bash
uv run khaos cluster-status
```

---

### `khaos list`

List all available traffic scenarios.

```bash
uv run khaos list
```

---

### `khaos validate`

Validate scenario YAML files for errors.

```bash
# Validate all scenarios
uv run khaos validate

# Validate specific scenario(s)
uv run khaos validate high-throughput
uv run khaos validate consumer-lag hot-partition
```

---

### `khaos run`

Run one or more traffic simulation scenarios on the local Docker Kafka cluster.

**Auto-starts the cluster if not running.** After the scenario completes, the cluster is stopped (unless `--keep-cluster` is specified).

```bash
uv run khaos run SCENARIO [SCENARIO...] [OPTIONS]
```

**Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--duration` | `-d` | `0` | Duration in seconds (0 = run until Ctrl+C) |
| `--keep-cluster` | `-k` | `false` | Keep Kafka cluster running after scenario ends |
| `--bootstrap-servers` | `-b` | `127.0.0.1:9092,...` | Kafka bootstrap servers |
| `--mode` | `-m` | `kraft` | Cluster mode: `kraft` or `zookeeper` |
| `--no-consumers` | - | `false` | Disable built-in consumers (producer-only mode) |

**Examples:**

```bash
# Run single scenario until Ctrl+C
uv run khaos run high-throughput

# Run for 60 seconds
uv run khaos run high-throughput --duration 60
uv run khaos run high-throughput -d 60

# Run multiple scenarios together
uv run khaos run partition-skew rebalance-storm

# Run multiple scenarios for 2 minutes
uv run khaos run consumer-lag throughput-drop --duration 120

# Keep cluster running after scenario (for manual inspection)
uv run khaos run high-throughput --keep-cluster
uv run khaos run high-throughput -k

# Keep cluster running with duration
uv run khaos run high-throughput -d 60 -k

# Use custom bootstrap servers (still uses local Docker cluster)
uv run khaos run high-throughput --bootstrap-servers localhost:9092

# Run with ZooKeeper mode (instead of KRaft)
uv run khaos run high-throughput --mode zookeeper
uv run khaos run high-throughput -m zookeeper

# Producer-only mode (no built-in consumers)
# Useful for learning stream processing with Spark/Flink
uv run khaos run high-throughput --no-consumers -k
```

---

### `khaos simulate`

Run traffic simulation against an **external** Kafka cluster (Confluent Cloud, AWS MSK, self-hosted, etc.).

Unlike `run`, this command:
- Does NOT start/stop Docker infrastructure
- Automatically skips broker incidents (`stop_broker`, `start_broker`)
- Supports full authentication (SASL, SSL/TLS, mTLS)

```bash
uv run khaos simulate SCENARIO [SCENARIO...] [OPTIONS]
```

**Options:**

| Option | Short | Required | Default | Description |
|--------|-------|----------|---------|-------------|
| `--bootstrap-servers` | `-b` | Yes | - | Kafka bootstrap servers |
| `--duration` | `-d` | No | `0` | Duration in seconds (0 = run until Ctrl+C) |
| `--security-protocol` | - | No | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `--sasl-mechanism` | - | No | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `--sasl-username` | - | No | - | SASL username |
| `--sasl-password` | - | No | - | SASL password |
| `--ssl-ca-location` | - | No | - | Path to CA certificate file |
| `--ssl-cert-location` | - | No | - | Path to client certificate (mTLS) |
| `--ssl-key-location` | - | No | - | Path to client private key (mTLS) |
| `--ssl-key-password` | - | No | - | Password for encrypted private key |
| `--skip-topic-creation` | - | No | `false` | Skip topic creation (topics already exist) |
| `--no-consumers` | - | No | `false` | Disable built-in consumers (producer-only mode) |

**Examples:**

```bash
# Plain connection (no auth)
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9092

# With duration
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --duration 120

# Multiple scenarios
uv run khaos simulate consumer-lag throughput-drop \
    --bootstrap-servers kafka.example.com:9092

# Skip topic creation (topics already exist)
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --skip-topic-creation
```

#### Confluent Cloud

```bash
uv run khaos simulate high-throughput \
    --bootstrap-servers pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
    --security-protocol SASL_SSL \
    --sasl-mechanism PLAIN \
    --sasl-username <API_KEY> \
    --sasl-password <API_SECRET>
```

#### AWS MSK (IAM auth not supported, use SCRAM)

```bash
uv run khaos simulate high-throughput \
    --bootstrap-servers b-1.mycluster.kafka.us-east-1.amazonaws.com:9096 \
    --security-protocol SASL_SSL \
    --sasl-mechanism SCRAM-SHA-512 \
    --sasl-username alice \
    --sasl-password secret
```

#### Self-hosted with SASL/PLAIN

```bash
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --security-protocol SASL_PLAINTEXT \
    --sasl-mechanism PLAIN \
    --sasl-username admin \
    --sasl-password admin-secret
```

#### Self-hosted with SASL/SCRAM + SSL

```bash
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SASL_SSL \
    --sasl-mechanism SCRAM-SHA-256 \
    --sasl-username myuser \
    --sasl-password mypassword \
    --ssl-ca-location /path/to/ca.pem
```

#### Self-hosted with SSL (server auth only)

```bash
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SSL \
    --ssl-ca-location /path/to/ca.pem
```

#### Self-hosted with mTLS (mutual TLS)

```bash
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SSL \
    --ssl-ca-location /path/to/ca.pem \
    --ssl-cert-location /path/to/client.pem \
    --ssl-key-location /path/to/client.key

# With encrypted private key
uv run khaos simulate high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SSL \
    --ssl-ca-location /path/to/ca.pem \
    --ssl-cert-location /path/to/client.pem \
    --ssl-key-location /path/to/client.key \
    --ssl-key-password keypassword
```

---

## Learning Stream Processing

khaos is perfect for learning Apache Spark, Flink, or Kafka Streams. Use `--no-consumers` mode to generate traffic while you write your own stream processing application.

### Quick Start for Learners

```bash
# 1. Start generating traffic (keep cluster running)
uv run khaos run high-throughput --no-consumers --keep-cluster

# 2. Access Redpanda Console to inspect topics
open http://localhost:8080

# 3. Connect your own consumer application to:
#    - Bootstrap servers: 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
#    - Topics: orders, events (or check the scenario YAML)

# 4. When done, stop the cluster
uv run khaos cluster-down
```

---

## Available Scenarios

### Traffic Scenarios

| Scenario | Description |
|----------|-------------|
| `high-throughput` | High-throughput scenario (2 topics, 4 producers, 4 consumers) |
| `consumer-lag` | Consumer lag scenario (slow consumers, growing lag) |
| `hot-partition` | Hot partition scenario (skewed key distribution) |

### Incident Scenarios (Chaos Engineering)

| Scenario | Description | Recommended Duration |
|----------|-------------|---------------------|
| `uneven-assignment` | 12 partitions / 5 consumers = uneven distribution | 60s+ |
| `throughput-drop` | Downstream backpressure at T+30s slows consumers | 60s+ |
| `rebalance-storm` | Consumer join/leave every 20s triggers rebalances | 60s+ |
| `leadership-churn` | Broker stop/restart at T+45s causes leader elections | 90s+ |

**Note:** `leadership-churn` only works with local Docker cluster (uses `stop_broker`/`start_broker` incidents).

---

## Creating Custom Scenarios

Scenarios are defined in YAML files in the `scenarios/` directory.

### Basic Structure

```yaml
name: my-scenario
description: "My custom traffic pattern"

topics:
  - name: my-topic
    partitions: 12
    replication_factor: 3
    num_producers: 2
    num_consumer_groups: 1
    consumers_per_group: 3
    producer_rate: 1000          # messages/second
    consumer_delay_ms: 0         # processing delay per message

    message_schema:
      key_distribution: uniform  # uniform, zipfian, single_key, round_robin
      key_cardinality: 50        # number of unique keys
      min_size_bytes: 200
      max_size_bytes: 500

    producer_config:
      batch_size: 16384
      linger_ms: 5
      acks: "all"                # "0", "1", "all"
      compression_type: lz4      # none, gzip, snappy, lz4, zstd

# Optional: incident triggers
incidents:
  - type: increase_consumer_delay
    at_seconds: 30
    delay_ms: 100
```

### Topic Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `name` | required | Topic name |
| `partitions` | `6` | Number of partitions |
| `replication_factor` | `3` | Replication factor (max 3 for local cluster) |
| `num_producers` | `1` | Number of producer instances |
| `num_consumer_groups` | `1` | Number of consumer groups |
| `consumers_per_group` | `1` | Consumers per group |
| `producer_rate` | `1000` | Messages per second per producer |
| `consumer_delay_ms` | `0` | Processing delay per message (ms) |

### Message Schema

| Field | Default | Description |
|-------|---------|-------------|
| `key_distribution` | `uniform` | Key distribution: `uniform`, `zipfian`, `single_key`, `round_robin` |
| `key_cardinality` | `100` | Number of unique keys |
| `min_size_bytes` | `100` | Minimum message size |
| `max_size_bytes` | `1000` | Maximum message size |

### Producer Config

| Field | Default | Description |
|-------|---------|-------------|
| `batch_size` | `16384` | Batch size in bytes |
| `linger_ms` | `5` | Linger time in milliseconds |
| `acks` | `all` | Acknowledgment mode: `0`, `1`, `all` |
| `compression_type` | `none` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |

### Incident Primitives

| Primitive | Parameters | Description | External Cluster |
|-----------|------------|-------------|------------------|
| `increase_consumer_delay` | `at_seconds`, `delay_ms` | Simulate backpressure | Yes |
| `rebalance_consumer` | `every_seconds`, `initial_delay_seconds` | Trigger consumer rebalances | Yes |
| `stop_broker` | `at_seconds`, `broker` | Stop a Kafka broker | No (skipped) |
| `start_broker` | `at_seconds`, `broker` | Start a Kafka broker | No (skipped) |
| `change_producer_rate` | `at_seconds`, `rate` | Traffic spike/drop | Yes |
| `pause_consumer` | `at_seconds`, `duration_seconds` | Simulate GC pause | Yes |

### Incident Examples

```yaml
incidents:
  # Increase consumer delay at 30 seconds
  - type: increase_consumer_delay
    at_seconds: 30
    delay_ms: 100

  # Rebalance consumers every 20 seconds (starting at 10s)
  - type: rebalance_consumer
    every_seconds: 20
    initial_delay_seconds: 10

  # Stop broker at 45 seconds
  - type: stop_broker
    at_seconds: 45
    broker: kafka-2  # kafka-1, kafka-2, or kafka-3

  # Start broker at 75 seconds
  - type: start_broker
    at_seconds: 75
    broker: kafka-2

  # Change producer rate at 60 seconds
  - type: change_producer_rate
    at_seconds: 60
    rate: 500  # new messages/second

  # Pause consumer at 30 seconds for 10 seconds
  - type: pause_consumer
    at_seconds: 30
    duration_seconds: 10
```

### Incident Groups (Repeating Incidents)

```yaml
incidents:
  - group:
      repeat: 3              # repeat 3 times
      interval_seconds: 60   # every 60 seconds
      incidents:
        - type: stop_broker
          at_seconds: 0      # relative to group start
          broker: kafka-2
        - type: start_broker
          at_seconds: 30     # 30s after group start
          broker: kafka-2
```

---

## Kafka Cluster Details

The Docker Compose setup creates:

- **3 Kafka brokers**: kafka-1, kafka-2, kafka-3
- **KRaft mode**: No ZooKeeper required
- **Redpanda Console**: http://localhost:8080

### Ports

| Service | Port |
|---------|------|
| kafka-1 | 9092 |
| kafka-2 | 9093 |
| kafka-3 | 9094 |
| Redpanda Console | 8080 |

### Bootstrap Servers

```
127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
```

---

## License

Apache 2.0
