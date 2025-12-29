# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.3] - 2025-12-29

### Fixed
- Bundle docker compose files in package so `khaos cluster-up` works when installed from PyPI

## [0.6.2] - 2025-12-29

### Fixed
- Bundle scenario YAML files in package so `khaos list` works when installed from PyPI

## [0.6.1] - 2025-12-29

### Changed
- Added `TaskRunner` class for cleaner async task management in executor
- Added `KeyDistribution.from_string()` classmethod
- Consolidated duplicate serializer methods into `_create_serializer()` helper
- Fixed datetime imports in `generators/field.py`
- Cleaned up unused loop variables to use `_` convention

## [0.6.0] - 2025-12-28

### Added
- Unit tests for `generators/schema.py` (SchemaPayloadGenerator)
- Unit tests for `errors.py` (error formatting functions)
- Unit tests for `serialization/avro.py` (Avro schema conversion and serialization)

### Changed
- Extracted `TopicManager`, `SimulatorFactory`, `IncidentScheduler` from `BaseExecutor`
- Extracted `ComposeRunner`, `SchemaRegistryManager` from `DockerManager`
- Extracted `ScenarioParser` from `Scenario` class
- Added `Simulator` base class for producers, consumers, and flow producers
- Moved constants from inline values to `khaos.defaults` module
- Renamed `execute()` to `start()` and `run()` to `_run()` in executors
- Use `asyncio.get_running_loop()` instead of deprecated `get_event_loop()`
- Use `asyncio.to_thread()` for Docker operations in `LocalExecutor`
- `ConsumerSimulator` now uses `ConsumerConfig` object (consistent with `ProducerSimulator`)
- Eliminated global `_executor` singleton - `ThreadPoolExecutor` is now owned by `BaseExecutor`
- Extracted duplicate validation logic in `validators/schema.py` and `validators/scenario.py`
- Replaced mypy with ty type checker from Astral

### Fixed
- Incident console prints not showing during execution (Rich Live console sharing)
- Async callable type hints for broker handler functions

### Removed
- `khaos.runtime` module (global executor singleton)

## [0.5.1] - 2025-12-27

### Fixed
- Code formatting issue in executor module

## [0.5.0] - 2025-12-27

### Added
- `SchemaRegistryProvider` class for fetching and caching schemas from Schema Registry
- `schema_provider: registry` option to fetch schemas from Schema Registry instead of inline YAML
- `get_raw_schema()` method to preserve original schema name/namespace for serialization
- Integration tests using Testcontainers (Kafka + Schema Registry)
- `manual_testing/` folder with schema upload scripts and instructions
- Critical error logging with full tracebacks via `logger.exception()`
- Custom scenario file support - use `khaos run ./my-scenario.yaml` or absolute paths
- Docker support - run khaos as a container against external Kafka clusters
- Dockerfile and .dockerignore for building container images
- `--schema-registry-url` CLI option for `simulate` command to override Schema Registry URL

### Changed
- CI now excludes integration tests by default (`-m "not integration"`)
- `check.sh` script now supports `-i` flag to include integration tests
- Refactored `DockerManager` - removed module-level singleton pattern, now uses dependency injection
- Moved `get_compose_file()` and `get_schema_registry_compose_file()` into `DockerManager` class as static methods
- Moved all inline imports to top-level across the codebase for consistency
- Removed backwards compatibility aliases (`ScenarioExecutor`, `ExternalScenarioExecutor`)
- Test fixtures now use `request.addfinalizer()` for guaranteed cleanup

### Fixed
- Schema name mismatch error when using `schema_provider: registry` - now preserves original Avro schema name/namespace
- Testcontainer cleanup - containers now properly shut down after tests complete

## [0.4.0] - 2025-12-25

### Added
- Serialization Formats documentation section in README

### Changed
- **Breaking:** Scenarios reorganized into categories - use `traffic/high-throughput` instead of `high-throughput`
  - `traffic/` - Basic traffic patterns (high-throughput, consumer-lag, hot-partition)
  - `chaos/` - Chaos engineering scenarios (broker-chaos, rebalance-storm, etc.)
  - `flows/` - Correlated event flows (order-flow, ecommerce-orders)
  - `serialization/` - Format examples (avro-example, protobuf-example, etc.)
- `khaos list` now displays scenarios grouped by category
- Updated README with new scenario paths and Kafka UI references

## [0.3.0] - 2025-12-25

### Added
- Protobuf serialization support with `data_format: protobuf` in message schema
- Dynamic Protobuf schema generation from YAML field definitions
- `ProtobufSerializer` and `ProtobufSerializerNoRegistry` classes
- New scenarios: `protobuf-example`, `protobuf-no-registry`

### Changed
- `cluster-down` now removes volumes by default to clear Schema Registry data
- Schema Registry is stopped with volume cleanup when cluster goes down
- Moved all imports to top of serialization modules

## [0.2.0] - 2025-12-25

### Added
- Avro serialization support with `data_format: avro` in message schema
- Schema Registry integration with auto-start when Avro scenarios run
- Avro without Schema Registry mode for simpler setups
- New serialization module (`khaos.serialization`) with `AvroSerializer`, `AvroSerializerNoRegistry`, `JsonSerializer`
- Dynamic Schema Registry docker compose overlay files
- New scenarios: `avro-example`, `avro-no-registry`, `all-incidents`, `targeted-incidents`, `chaos-loop`
- Unified `ValidationError` and `ValidationResult` in `validators/common.py`
- Centralized Kafka config builder in `kafka/config.py`
- DockerManager methods: `start_schema_registry()`, `stop_schema_registry()`, `is_schema_registry_running()`

### Changed
- Replaced Redpanda Console with Kafka UI (provectuslabs/kafka-ui) for better Schema Registry support
- Timestamp fields now generate epoch milliseconds (int) for Avro compatibility
- Renamed `schemas/` module to `validators/`
- Refactored `DockerManager` from module functions to class
- Changed default `auto_offset_reset` to `latest` for cleaner test runs
- Topics are now deleted before creation to ensure clean state

### Fixed
- KafkaAdmin error handling now uses proper Kafka error codes instead of string matching
- Consumer pause/resume no longer crashes (removed auto-close in consume_loop)
- Consumer rebalance race condition (added delay before close)
- Silent error swallowing in consumer close() now logs errors

## [0.1.1] - 2025-12-23

### Added
- `--version` flag to display current version
- Version read from package metadata

### Changed
- Updated installation documentation

## [0.1.0] - 2025-12-23

### Added
- Initial release
- Kafka traffic generation with configurable producers and consumers
- YAML-based scenario definitions
- Correlated event flows for multi-topic message sequences
- Structured field schemas with typed fields (string, int, float, bool, uuid, timestamp, enum, object, array)
- Faker integration for realistic data generation (name, email, phone, address, etc.)
- Key distribution strategies: uniform, zipfian, single_key, round_robin
- Cardinality constraints for unique value generation
- 6 incident types for chaos engineering:
  - `increase_consumer_delay`
  - `rebalance_consumer`
  - `stop_broker` / `start_broker`
  - `change_producer_rate`
  - `pause_consumer`
- Docker Compose integration (3-broker KRaft and ZooKeeper clusters)
- Full authentication support (SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, SSL/TLS, mTLS)
- Real-time stats display during execution
- 9 pre-built scenarios (high-throughput, consumer-lag, hot-partition, etc.)
- CLI commands: `run`, `simulate`, `validate`, `list`, `cluster-up`, `cluster-down`
- CI/CD pipeline with linting, testing, type checking
- PyPI publishing workflow

[0.6.3]: https://github.com/aleksandarskrbic/khaos/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/aleksandarskrbic/khaos/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/aleksandarskrbic/khaos/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/aleksandarskrbic/khaos/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/aleksandarskrbic/khaos/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/aleksandarskrbic/khaos/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/aleksandarskrbic/khaos/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/aleksandarskrbic/khaos/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/aleksandarskrbic/khaos/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/aleksandarskrbic/khaos/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/aleksandarskrbic/khaos/releases/tag/v0.1.0
