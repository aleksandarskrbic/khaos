# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New test scenarios: `all-incidents`, `targeted-incidents`, `chaos-loop`
- Unified `ValidationError` and `ValidationResult` in `validators/common.py`
- Centralized Kafka config builder in `kafka/config.py`

### Changed
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

[Unreleased]: https://github.com/aleksandarskrbic/khaos/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/aleksandarskrbic/khaos/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/aleksandarskrbic/khaos/releases/tag/v0.1.0
