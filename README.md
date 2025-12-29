<div align="center">
  <img src="assets/logo.png" alt="khaos logo" width="200">
  <h1>khaos</h1>
  <p><strong>Kafka data generator and load testing tool</strong> - generate fake messages, simulate producers/consumers, and run chaos engineering scenarios</p>

  [![CI](https://github.com/aleksandarskrbic/khaos/actions/workflows/ci.yml/badge.svg)](https://github.com/aleksandarskrbic/khaos/actions/workflows/ci.yml)
  [![PyPI](https://img.shields.io/pypi/v/khaos-cli.svg)](https://pypi.org/project/khaos-cli/)
  [![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
  [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
</div>

<p align="center">
  <img src="assets/demo.gif" alt="Khaos Demo" width="800">
</p>

> **Khaos** is a Kafka data generator, load testing tool, and chaos engineering CLI. Generate synthetic test data, simulate realistic producer and consumer workloads, and inject failures into your Kafka cluster. Perfect for testing Spark Streaming, Apache Flink, and Kafka Streams applications.

## Table of Contents

- [Use Cases](#use-cases)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Shell Completion](#shell-completion)
- [CLI Reference](#cli-reference)
  - [Commands Overview](#commands-overview)
  - [`run` vs `simulate`](#run-vs-simulate)
  - [Cluster Modes](#cluster-modes)
  - [`khaos cluster-up`](#khaos-cluster-up)
  - [`khaos cluster-down`](#khaos-cluster-down)
  - [`khaos cluster-status`](#khaos-cluster-status)
  - [`khaos list`](#khaos-list)
  - [`khaos validate`](#khaos-validate)
  - [`khaos run`](#khaos-run)
  - [`khaos simulate`](#khaos-simulate)
- [Learning Stream Processing](#learning-stream-processing)
- [Available Scenarios](#available-scenarios)
- [Creating Custom Scenarios](#creating-custom-scenarios)
  - [Basic Structure](#basic-structure)
  - [Topic Configuration](#topic-configuration)
  - [Message Schema](#message-schema)
  - [Structured Field Schemas](#structured-field-schemas)
  - [Serialization Formats](#serialization-formats)
  - [Producer Config](#producer-config)
  - [Incident Primitives](#incident-primitives)
  - [Incident Groups](#incident-groups-repeating-incidents)
- [Correlated Event Flows](#correlated-event-flows)
- [Kafka Cluster Details](#kafka-cluster-details)
- [Running with Docker](#running-with-docker)
- [License](#license)

## Use Cases

- **Kafka Data Generator**: Generate fake Kafka messages with realistic schemas for testing
- **Kafka Load Testing**: Stress test and benchmark your Kafka cluster at high throughput
- **Kafka Producer Simulator**: Simulate multiple producers with configurable rates and patterns
- **Stream Processing Testing**: Generate test data for Apache Flink, Spark Streaming, and Kafka Streams
- **Chaos Engineering**: Inject broker failures, trigger rebalances, simulate consumer lag
- **Kafka Consumer Testing**: Test consumer group behavior, lag scenarios, and rebalancing
- **Monitoring Validation**: Verify Grafana dashboards and alerting rules with real traffic patterns

## Features

- **One-Command Setup**: Spin up a 3-broker Kafka cluster with traffic in seconds
- **YAML-Based Scenarios**: Define traffic patterns declaratively, no code required
- **Producer-Only Mode**: Generate data without built-in consumers (`--no-consumers`)
- **External Cluster Support**: Connect to any Kafka cluster (self-hosted, external)
- **Chaos Engineering**: Built-in incident primitives (backpressure, rebalances, broker failures)
- **Full Authentication**: SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, SSL/TLS, mTLS
- **Live Stats Display**: Real-time producer/consumer metrics
- **Serialization Formats**: JSON, Avro, and Protobuf with Schema Registry support
- **Web UI**: Kafka UI at localhost:8080 for cluster inspection

## Requirements

- Python 3.11+
- Docker and Docker Compose (for local cluster)

## Installation

### Using Homebrew (macOS/Linux)

```bash
brew install khaos
```

### Using uv (recommended)

```bash
uv tool install khaos-cli

# Add to PATH (if not already):
uv tool update-shell

# Or manually:
export PATH="$HOME/.local/bin:$PATH"
```

### Using pipx

```bash
pipx install khaos-cli
```

### Using pip

```bash
pip install khaos-cli
```

### Verify installation

```bash
khaos --version
khaos --help
```

### From source

```bash
git clone https://github.com/aleksandarskrbic/khaos.git
cd khaos

# Option 1: Use without installing globally (recommended for development)
uv sync
uv run khaos --help

# Option 2: Install as global command
uv tool install -e .
export PATH="$HOME/.local/bin:$PATH"  # if not already in PATH
khaos --help
```

### Development Setup

```bash
# Clone and install dependencies
git clone https://github.com/aleksandarskrbic/khaos.git
cd khaos
uv sync

# Install pre-commit hooks
uv run pre-commit install

# Run all checks before committing (lint, format, test, typecheck)
./scripts/check.sh
```

## Quick Start

```bash
# Run a scenario (auto-starts local Kafka cluster)
khaos run traffic/high-throughput

# Press Ctrl+C to stop
```

---

## Shell Completion

Enable tab completion for commands, options, and scenarios:

```bash
# Install completion (bash, zsh, fish, powershell)
khaos --install-completion

# Restart shell or reload config
source ~/.zshrc  # or ~/.bashrc
```

Then use Tab to autocomplete:
```bash
khaos cl<TAB>          # → cluster-up, cluster-down, cluster-status
khaos run <TAB>        # → shows available scenarios
khaos run --<TAB>      # → shows available options
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
- Load testing external/self-hosted clusters
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
khaos cluster-up

# Start with ZooKeeper mode
khaos cluster-up --mode zookeeper
khaos cluster-up -m zookeeper
```

**Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--mode` | `-m` | `kraft` | Cluster mode: `kraft` or `zookeeper` |

This starts:
- 3 Kafka brokers (kafka-1, kafka-2, kafka-3)
- ZooKeeper (only in zookeeper mode)
- Kafka UI at http://localhost:8080

---

### `khaos cluster-down`

Stop the Kafka cluster.

```bash
# Stop cluster (keep data)
khaos cluster-down

# Stop cluster and remove all data volumes
khaos cluster-down --volumes
khaos cluster-down -v
```

**Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--volumes` | `-v` | Remove data volumes |

---

### `khaos cluster-status`

Show the status of Kafka containers.

```bash
khaos cluster-status
```

---

### `khaos list`

List all available traffic scenarios.

```bash
khaos list
```

---

### `khaos validate`

Validate scenario YAML files for errors.

```bash
# Validate all scenarios
khaos validate

# Validate specific scenario(s)
khaos validate traffic/high-throughput
khaos validate traffic/consumer-lag traffic/hot-partition
```

---

### `khaos run`

Run one or more traffic simulation scenarios on the local Docker Kafka cluster.

**Auto-starts the cluster if not running.** After the scenario completes, the cluster is stopped (unless `--keep-cluster` is specified).

```bash
khaos run SCENARIO [SCENARIO...] [OPTIONS]
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
# Run built-in scenario until Ctrl+C
khaos run traffic/high-throughput

# Run custom scenario file
khaos run ./my-scenario.yaml
khaos run /path/to/custom-scenario.yaml

# Run for 60 seconds
khaos run traffic/high-throughput --duration 60
khaos run traffic/high-throughput -d 60

# Run multiple scenarios together
khaos run traffic/hot-partition chaos/rebalance-storm

# Run multiple scenarios for 2 minutes
khaos run traffic/consumer-lag chaos/throughput-drop --duration 120

# Keep cluster running after scenario (for manual inspection)
khaos run traffic/high-throughput --keep-cluster
khaos run traffic/high-throughput -k

# Keep cluster running with duration
khaos run traffic/high-throughput -d 60 -k

# Use custom bootstrap servers (still uses local Docker cluster)
khaos run traffic/high-throughput --bootstrap-servers localhost:9092

# Run with ZooKeeper mode (instead of KRaft)
khaos run traffic/high-throughput --mode zookeeper
khaos run traffic/high-throughput -m zookeeper

# Producer-only mode (no built-in consumers)
# Useful for learning stream processing with Spark/Flink
khaos run traffic/high-throughput --no-consumers -k
```

---

### `khaos simulate`

Run traffic simulation against an **external** Kafka cluster (self-hosted, etc.).

Unlike `run`, this command:
- Does NOT start/stop Docker infrastructure
- Automatically skips broker incidents (`stop_broker`, `start_broker`)
- Supports full authentication (SASL, SSL/TLS, mTLS)

```bash
khaos simulate SCENARIO [SCENARIO...] [OPTIONS]
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
| `--schema-registry-url` | - | No | - | Schema Registry URL for Avro/Protobuf schemas |

**Examples:**

```bash
# Plain connection (no auth)
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9092

# Custom scenario file
khaos simulate ./my-scenario.yaml \
    --bootstrap-servers kafka.example.com:9092

# With duration
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --duration 120

# Multiple scenarios
khaos simulate traffic/consumer-lag chaos/throughput-drop \
    --bootstrap-servers kafka.example.com:9092

# Skip topic creation (topics already exist)
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --skip-topic-creation

# With external Schema Registry (for Avro/Protobuf scenarios)
khaos simulate serialization/avro-example \
    --bootstrap-servers kafka.example.com:9092 \
    --schema-registry-url https://schema-registry.example.com:8081
```

#### Self-hosted with SASL/PLAIN

```bash
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --security-protocol SASL_PLAINTEXT \
    --sasl-mechanism PLAIN \
    --sasl-username admin \
    --sasl-password admin-secret
```

#### Self-hosted with SASL/SCRAM + SSL

```bash
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SASL_SSL \
    --sasl-mechanism SCRAM-SHA-256 \
    --sasl-username myuser \
    --sasl-password mypassword \
    --ssl-ca-location /path/to/ca.pem
```

#### Self-hosted with SSL (server auth only)

```bash
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SSL \
    --ssl-ca-location /path/to/ca.pem
```

#### Self-hosted with mTLS (mutual TLS)

```bash
khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SSL \
    --ssl-ca-location /path/to/ca.pem \
    --ssl-cert-location /path/to/client.pem \
    --ssl-key-location /path/to/client.key

# With encrypted private key
khaos simulate traffic/high-throughput \
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
khaos run traffic/high-throughput --no-consumers --keep-cluster

# 2. Access Kafka UI to inspect topics
open http://localhost:8080

# 3. Connect your own consumer application to:
#    - Bootstrap servers: 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
#    - Topics: orders, events (or check the scenario YAML)

# 4. When done, stop the cluster
khaos cluster-down
```

---

## Available Scenarios

Scenarios are organized into categories. Use `khaos list` to see all available scenarios.

### Traffic Patterns (`traffic/`)

| Scenario | Description |
|----------|-------------|
| `traffic/high-throughput` | High-throughput scenario (2 topics, 4 producers, 4 consumers) |
| `traffic/consumer-lag` | Consumer lag scenario (slow consumers, growing lag) |
| `traffic/hot-partition` | Hot partition scenario (skewed key distribution) |

### Chaos Engineering (`chaos/`)

| Scenario | Description | Recommended Duration |
|----------|-------------|---------------------|
| `chaos/uneven-assignment` | 12 partitions / 5 consumers = uneven distribution | 60s+ |
| `chaos/throughput-drop` | Downstream backpressure at T+30s slows consumers | 60s+ |
| `chaos/rebalance-storm` | Consumer join/leave every 20s triggers rebalances | 60s+ |
| `chaos/leadership-churn` | Broker stop/restart at T+45s causes leader elections | 90s+ |
| `chaos/broker-chaos` | Repeated broker stop/start cycles | 60s+ |

**Note:** `chaos/leadership-churn` and `chaos/broker-chaos` only work with local Docker cluster.

### Event Flows (`flows/`)

| Scenario | Description |
|----------|-------------|
| `flows/order-flow` | Correlated event flow (order → payment → shipment) |
| `flows/ecommerce-orders` | E-commerce order events with realistic fake data |

### Serialization Formats (`serialization/`)

| Scenario | Description |
|----------|-------------|
| `serialization/avro-example` | Avro serialization with Schema Registry |
| `serialization/avro-no-registry` | Avro serialization without Schema Registry |
| `serialization/protobuf-example` | Protobuf serialization with Schema Registry |
| `serialization/protobuf-no-registry` | Protobuf serialization without Schema Registry |

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
| `min_size_bytes` | `100` | Minimum message size (used when `fields` not defined) |
| `max_size_bytes` | `1000` | Maximum message size (used when `fields` not defined) |
| `fields` | - | Structured field definitions (see below) |

### Structured Field Schemas

Define structured JSON messages with typed fields:

```yaml
message_schema:
  fields:
    - name: order_id
      type: uuid
    - name: customer_id
      type: string
      cardinality: 1000          # 1000 unique values, then repeat
    - name: amount
      type: float
      min: 10.0
      max: 5000.0
    - name: status
      type: enum
      values: [pending, shipped, delivered]
    - name: created_at
      type: timestamp
    - name: address
      type: object
      fields:
        - name: city
          type: string
          cardinality: 50
        - name: zip
          type: string
    - name: items
      type: array
      min_items: 1
      max_items: 5
      items:
        type: object
        fields:
          - name: product_id
            type: uuid
          - name: quantity
            type: int
            min: 1
            max: 10
```

#### Supported Field Types

| Type | Parameters | Description |
|------|------------|-------------|
| `string` | `cardinality`, `min_length`, `max_length` | Random string |
| `int` | `min`, `max`, `cardinality` | Integer in range |
| `float` | `min`, `max` | Float in range |
| `boolean` | - | Random true/false |
| `uuid` | - | UUID v4 |
| `timestamp` | - | ISO 8601 timestamp |
| `enum` | `values` (required) | Pick from list |
| `object` | `fields` (required) | Nested object |
| `array` | `items`, `min_items`, `max_items` | Array of items |
| `faker` | `provider` (required), `locale` | Realistic fake data |

#### Faker Providers

Generate realistic data using [Faker](https://faker.readthedocs.io/) providers:

```yaml
fields:
  - name: customer_name
    type: faker
    provider: name
  - name: email
    type: faker
    provider: email
  - name: address
    type: faker
    provider: street_address
  - name: city
    type: faker
    provider: city
  - name: phone
    type: faker
    provider: phone_number
  - name: company
    type: faker
    provider: company
  - name: credit_card
    type: faker
    provider: credit_card_number
  - name: job_title
    type: faker
    provider: job
  - name: text
    type: faker
    provider: text
  # With locale for localized data
  - name: german_name
    type: faker
    provider: name
    locale: de_DE
```

Common providers: `name`, `email`, `phone_number`, `address`, `street_address`, `city`, `country`, `postcode`, `company`, `job`, `text`, `word`, `sentence`, `url`, `ipv4`, `user_agent`, `credit_card_number`, `date`, `date_time`.

See [Faker docs](https://faker.readthedocs.io/en/master/providers.html) for the full list.

**Note:** If `fields` is not defined, messages are random JSON with padding to match size constraints.

### Serialization Formats

khaos supports three serialization formats: JSON (default), Avro, and Protobuf.

#### JSON (Default)

```yaml
message_schema:
  fields:
    - name: order_id
      type: uuid
    - name: amount
      type: float
```

No `data_format` needed - JSON is the default.

#### Avro

```yaml
message_schema:
  data_format: avro
  fields:
    - name: order_id
      type: uuid
    - name: amount
      type: float
    - name: status
      type: enum
      values: [PENDING, COMPLETED]
```

With Schema Registry (auto-registers schemas):

```yaml
schema_registry:
  url: http://localhost:8081

topics:
  - name: orders
    message_schema:
      data_format: avro
      fields: [...]
```

Or override via CLI (useful for external clusters):

```bash
khaos simulate my-scenario \
    --bootstrap-servers kafka.example.com:9092 \
    --schema-registry-url https://schema-registry.example.com:8081
```

#### Protobuf

```yaml
message_schema:
  data_format: protobuf
  fields:
    - name: shipment_id
      type: uuid
    - name: carrier
      type: enum
      values: [UPS, FEDEX, DHL]
    - name: weight_kg
      type: float
```

With Schema Registry:

```yaml
schema_registry:
  url: http://localhost:8081

topics:
  - name: shipments
    message_schema:
      data_format: protobuf
      fields: [...]
```

#### Type Mappings

| Field Type | JSON | Avro | Protobuf |
|------------|------|------|----------|
| `string` | string | string | TYPE_STRING |
| `int` | number | long | TYPE_INT64 |
| `float` | number | double | TYPE_DOUBLE |
| `boolean` | boolean | boolean | TYPE_BOOL |
| `uuid` | string | string (uuid) | TYPE_STRING |
| `timestamp` | number (epoch ms) | long (timestamp-millis) | TYPE_INT64 |
| `enum` | string | enum | TYPE_ENUM |
| `object` | object | record | TYPE_MESSAGE |
| `array` | array | array | repeated |

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

## Correlated Event Flows

Flows simulate real microservice architectures where an event on topic A triggers related events on topics B, C, etc. with realistic delays. Each flow instance shares a correlation ID across all steps.

### Basic Flow Structure

```yaml
name: order-flow
description: "Order processing microservices"

flows:
  - name: order-processing
    rate: 50                 # flow instances per second
    correlation:
      type: uuid             # auto-generate correlation ID
    steps:
      - topic: orders
        event_type: order_created
        fields:
          - name: order_id
            type: uuid
          - name: amount
            type: float
            min: 10.0
            max: 1000.0

      - topic: payments
        event_type: payment_processed
        delay_ms: 500        # 500ms after previous step
        fields:
          - name: payment_id
            type: uuid
          - name: status
            type: enum
            values: [success, failed]

      - topic: shipments
        event_type: shipment_created
        delay_ms: 2000       # 2s after previous step
        fields:
          - name: shipment_id
            type: uuid
          - name: carrier
            type: enum
            values: [fedex, ups, dhl]
```

### Flow Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `name` | required | Flow name (for display) |
| `rate` | `10.0` | Flow instances per second |
| `correlation.type` | `uuid` | `uuid` (auto-generate) or `field_ref` |
| `correlation.field` | - | Field name from first step (when type is `field_ref`) |
| `steps` | required | List of steps (minimum 2 recommended) |

### Step Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `topic` | required | Target Kafka topic |
| `event_type` | required | Event type (included in message) |
| `delay_ms` | `0` | Delay after previous step (milliseconds) |
| `fields` | - | Field definitions (same as topic schemas) |
| `consumers` | - | Optional: built-in consumers for this step |

### Step Consumers (Optional)

By default, flows are producer-only. Add `consumers` to enable built-in consumers for testing lag/backpressure:

```yaml
steps:
  - topic: orders
    event_type: order_created
    consumers:
      groups: 1          # number of consumer groups (default: 1)
      per_group: 2       # consumers per group (default: 1)
      delay_ms: 10       # processing delay per message (default: 0)
    fields:
      - name: order_id
        type: uuid
```

Without `consumers`, you connect your own apps (Spark, Flink, etc.) to consume from flow topics.

### Correlation ID Types

**UUID (default)**: Auto-generates a UUID for each flow instance:

```yaml
correlation:
  type: uuid
```

**Field Reference**: Uses a field value from the first step:

```yaml
correlation:
  type: field_ref
  field: order_id    # uses order_id from first step
```

### Generated Messages

Each step produces a message with:
- `correlation_id`: Shared across all steps in the flow instance
- `event_type`: From the step definition
- All fields defined in the step

Example output for order-processing flow:

```json
// Topic: orders
{"correlation_id": "550e8400-e29b-41d4-a716-446655440000", "event_type": "order_created", "order_id": "...", "amount": 123.45}

// Topic: payments (500ms later)
{"correlation_id": "550e8400-e29b-41d4-a716-446655440000", "event_type": "payment_processed", "payment_id": "...", "status": "success"}

// Topic: shipments (2000ms later)
{"correlation_id": "550e8400-e29b-41d4-a716-446655440000", "event_type": "shipment_created", "shipment_id": "...", "carrier": "fedex"}
```

### Flows with Topics

Scenarios can have both regular topics and flows:

```yaml
name: mixed-scenario

topics:
  - name: logs
    partitions: 6
    producer_rate: 1000

flows:
  - name: order-processing
    rate: 50
    steps:
      # ...
```

### Example: E-commerce Order Flow

See `scenarios/flows/order-flow.yaml` for a complete example simulating:
1. Order creation
2. Payment initiation and completion
3. Inventory reservation
4. Shipment creation
5. Customer notification

```bash
khaos run flows/order-flow --no-consumers -k
```

---

## Kafka Cluster Details

The Docker Compose setup creates:

- **3 Kafka brokers**: kafka-1, kafka-2, kafka-3
- **KRaft mode**: No ZooKeeper required
- **Kafka UI**: http://localhost:8080
- **Schema Registry**: http://localhost:8081 (auto-started when using Avro/Protobuf)

### Ports

| Service | Port |
|---------|------|
| kafka-1 | 9092 |
| kafka-2 | 9093 |
| kafka-3 | 9094 |
| Kafka UI | 8080 |
| Schema Registry | 8081 |

### Bootstrap Servers

```
127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
```

---

## Running with Docker

Run khaos as a Docker container to generate data against your external Kafka cluster without installing Python.

### Build the Image

```bash
docker build -t khaos .
```

### Basic Usage

```bash
# Run built-in scenario
docker run --rm khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --duration 60

# Run custom scenario (mount the file)
docker run --rm -v $(pwd)/my-scenario.yaml:/scenario.yaml \
    khaos simulate /scenario.yaml \
    --bootstrap-servers kafka.example.com:9092
```

### With Authentication

```bash
# SASL/PLAIN
docker run --rm khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9092 \
    --security-protocol SASL_PLAINTEXT \
    --sasl-mechanism PLAIN \
    --sasl-username admin \
    --sasl-password secret

# SSL with certificates (mount certs directory)
docker run --rm -v $(pwd)/certs:/certs \
    khaos simulate traffic/high-throughput \
    --bootstrap-servers kafka.example.com:9093 \
    --security-protocol SSL \
    --ssl-ca-location /certs/ca.pem

# With Schema Registry
docker run --rm khaos simulate serialization/avro-example \
    --bootstrap-servers kafka.example.com:9092 \
    --schema-registry-url https://schema-registry.example.com:8081
```

### Docker Compose Example

```yaml
services:
  khaos:
    build: .
    command: >
      simulate traffic/high-throughput
      --bootstrap-servers kafka:9092
      --duration 300
    depends_on:
      - kafka
```

**Note:** Docker mode only supports `simulate` command (external clusters). The `run` command requires Docker-in-Docker which is not supported.

---

## Keywords

Kafka data generator, Kafka test data, Kafka load testing, Kafka stress testing, Kafka producer simulator, Kafka consumer simulator, Kafka fake data, Kafka synthetic data, Kafka message generator, Kafka traffic generator, Kafka benchmark tool, Kafka chaos testing, Kafka fault injection, Kafka failure testing, Kafka broker failure simulation, Kafka consumer lag testing, Kafka rebalance testing, stream processing test data, Flink test data, Spark Streaming test data, Kafka Streams testing, event streaming testing, message queue testing

---

## License

Apache 2.0
