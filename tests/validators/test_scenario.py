import tempfile
from pathlib import Path

import yaml

from khaos.validators.scenario import (
    ValidationResult,
    validate_incident,
    validate_incident_group,
    validate_message_schema,
    validate_producer_config,
    validate_scenario_file,
    validate_topic,
)


def create_temp_yaml(data: dict) -> Path:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        return Path(f.name)


class TestValidateScenarioFile:
    def test_valid_full_scenario(self):
        data = {
            "name": "full-scenario",
            "description": "A complete scenario",
            "topics": [
                {
                    "name": "events",
                    "partitions": 12,
                    "replication_factor": 3,
                    "num_producers": 2,
                    "num_consumer_groups": 1,
                    "consumers_per_group": 3,
                    "producer_rate": 1000,
                    "consumer_delay_ms": 5,
                    "message_schema": {
                        "key_distribution": "zipfian",
                        "key_cardinality": 100,
                        "min_size_bytes": 200,
                        "max_size_bytes": 500,
                    },
                    "producer_config": {
                        "batch_size": 16384,
                        "linger_ms": 5,
                        "acks": "all",
                        "compression_type": "lz4",
                    },
                }
            ],
            "incidents": [{"type": "stop_broker", "at_seconds": 30, "broker": "kafka-2"}],
        }
        path = create_temp_yaml(data)
        result = validate_scenario_file(path)
        assert result.valid, f"Errors: {[e.message for e in result.errors]}"

    def test_missing_required_fields(self):
        # Missing name
        result = validate_scenario_file(create_temp_yaml({"topics": [{"name": "t"}]}))
        assert not result.valid
        assert any("name" in e.path for e in result.errors)

        # Missing topics
        result = validate_scenario_file(create_temp_yaml({"name": "test"}))
        assert not result.valid
        assert any("topics" in e.path for e in result.errors)

        # Empty topics
        result = validate_scenario_file(create_temp_yaml({"name": "test", "topics": []}))
        assert not result.valid

    def test_file_not_found(self):
        result = validate_scenario_file(Path("/nonexistent/path.yaml"))
        assert not result.valid
        assert any("not found" in e.message for e in result.errors)

    def test_invalid_yaml_syntax(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            path = Path(f.name)
        result = validate_scenario_file(path)
        assert not result.valid
        assert any("YAML" in e.message for e in result.errors)


class TestValidateTopic:
    def test_valid_topic(self):
        result = ValidationResult(valid=True)
        topic = {"name": "test-topic", "partitions": 12, "replication_factor": 3}
        validate_topic(topic, "topics[0]", result)
        assert result.valid

    def test_invalid_partitions(self):
        for partitions in [0, -1]:
            result = ValidationResult(valid=True)
            validate_topic({"name": "test", "partitions": partitions}, "topics[0]", result)
            assert not result.valid

    def test_high_partitions_warning(self):
        result = ValidationResult(valid=True)
        validate_topic({"name": "test", "partitions": 150}, "topics[0]", result)
        assert result.valid  # Still valid, just warning
        assert any("partition count" in w.message for w in result.warnings)

    def test_replication_factor_exceeds_brokers(self):
        result = ValidationResult(valid=True)
        validate_topic({"name": "test", "replication_factor": 5}, "topics[0]", result)
        assert not result.valid
        assert any("cannot exceed 3" in e.message for e in result.errors)

    def test_invalid_rates_and_delays(self):
        result = ValidationResult(valid=True)
        validate_topic({"name": "test", "producer_rate": -100}, "topics[0]", result)
        assert not result.valid

        result = ValidationResult(valid=True)
        validate_topic({"name": "test", "consumer_delay_ms": -10}, "topics[0]", result)
        assert not result.valid


class TestValidateMessageSchema:
    def test_valid_schema(self):
        result = ValidationResult(valid=True)
        schema = {
            "key_distribution": "zipfian",
            "key_cardinality": 100,
            "min_size_bytes": 200,
            "max_size_bytes": 500,
        }
        validate_message_schema(schema, "message_schema", result)
        assert result.valid

    def test_invalid_key_distribution(self):
        result = ValidationResult(valid=True)
        validate_message_schema({"key_distribution": "invalid"}, "message_schema", result)
        assert not result.valid

    def test_all_valid_key_distributions(self):
        for dist in ["uniform", "zipfian", "single_key", "round_robin"]:
            result = ValidationResult(valid=True)
            validate_message_schema({"key_distribution": dist}, "message_schema", result)
            assert result.valid

    def test_min_greater_than_max(self):
        result = ValidationResult(valid=True)
        validate_message_schema(
            {"min_size_bytes": 1000, "max_size_bytes": 500}, "message_schema", result
        )
        assert not result.valid
        assert any("cannot be greater than" in e.message for e in result.errors)


class TestValidateProducerConfig:
    def test_valid_config(self):
        result = ValidationResult(valid=True)
        config = {"batch_size": 16384, "linger_ms": 5, "acks": "all", "compression_type": "lz4"}
        validate_producer_config(config, "producer_config", result)
        assert result.valid

    def test_invalid_acks_and_compression(self):
        result = ValidationResult(valid=True)
        validate_producer_config({"acks": "invalid"}, "producer_config", result)
        assert not result.valid

        result = ValidationResult(valid=True)
        validate_producer_config({"compression_type": "invalid"}, "producer_config", result)
        assert not result.valid

    def test_all_valid_acks_and_compression(self):
        for acks in ["0", "1", "all", "-1"]:
            result = ValidationResult(valid=True)
            validate_producer_config({"acks": acks}, "producer_config", result)
            assert result.valid

        for comp in ["none", "gzip", "snappy", "lz4", "zstd"]:
            result = ValidationResult(valid=True)
            validate_producer_config({"compression_type": comp}, "producer_config", result)
            assert result.valid


class TestValidateIncident:
    def test_valid_incidents(self):
        incidents = [
            {"type": "increase_consumer_delay", "at_seconds": 30, "delay_ms": 100},
            {"type": "rebalance_consumer", "every_seconds": 20},
            {"type": "stop_broker", "at_seconds": 30, "broker": "kafka-1"},
            {"type": "start_broker", "at_seconds": 60, "broker": "kafka-1"},
            {"type": "change_producer_rate", "at_seconds": 30, "rate": 500},
            {"type": "pause_consumer", "at_seconds": 30, "duration_seconds": 10},
        ]
        for incident in incidents:
            result = ValidationResult(valid=True)
            validate_incident(incident, "incidents[0]", result)
            assert result.valid, (
                f"'{incident['type']}' should be valid: {[e.message for e in result.errors]}"
            )

    def test_missing_type_and_timing(self):
        result = ValidationResult(valid=True)
        validate_incident({"at_seconds": 30}, "incidents[0]", result)
        assert not result.valid

        result = ValidationResult(valid=True)
        validate_incident({"type": "rebalance_consumer"}, "incidents[0]", result)
        assert not result.valid

    def test_invalid_type(self):
        result = ValidationResult(valid=True)
        validate_incident({"type": "invalid_incident", "at_seconds": 30}, "incidents[0]", result)
        assert not result.valid

    def test_missing_required_params(self):
        missing_params = [
            ({"type": "stop_broker", "at_seconds": 30}, "broker"),
            ({"type": "increase_consumer_delay", "at_seconds": 30}, "delay_ms"),
            ({"type": "change_producer_rate", "at_seconds": 30}, "rate"),
            ({"type": "pause_consumer", "at_seconds": 30}, "duration_seconds"),
        ]
        for incident, param in missing_params:
            result = ValidationResult(valid=True)
            validate_incident(incident, "incidents[0]", result)
            assert not result.valid, f"Missing {param} should fail"

    def test_invalid_broker_name(self):
        result = ValidationResult(valid=True)
        validate_incident(
            {"type": "stop_broker", "at_seconds": 30, "broker": "kafka-99"}, "incidents[0]", result
        )
        assert not result.valid


class TestValidateIncidentGroup:
    def test_valid_group(self):
        result = ValidationResult(valid=True)
        group = {
            "repeat": 3,
            "interval_seconds": 40,
            "incidents": [
                {"type": "stop_broker", "at_seconds": 0, "broker": "kafka-2"},
                {"type": "start_broker", "at_seconds": 20, "broker": "kafka-2"},
            ],
        }
        validate_incident_group(group, "incidents[0].group", result)
        assert result.valid

    def test_missing_required_fields(self):
        base = {
            "repeat": 3,
            "interval_seconds": 40,
            "incidents": [{"type": "stop_broker", "at_seconds": 0, "broker": "kafka-2"}],
        }

        for field in ["repeat", "interval_seconds", "incidents"]:
            result = ValidationResult(valid=True)
            group = {k: v for k, v in base.items() if k != field}
            validate_incident_group(group, "group", result)
            assert not result.valid

    def test_incident_exceeds_interval_warning(self):
        result = ValidationResult(valid=True)
        group = {
            "repeat": 3,
            "interval_seconds": 30,
            "incidents": [
                {"type": "stop_broker", "at_seconds": 0, "broker": "kafka-2"},
                {"type": "start_broker", "at_seconds": 35, "broker": "kafka-2"},
            ],
        }
        validate_incident_group(group, "group", result)
        assert result.valid
        assert any("overlap" in w.message for w in result.warnings)


class TestIntegrationScenarios:
    def test_scenario_with_incident_group(self):
        data = {
            "name": "broker-cycling",
            "topics": [{"name": "test-events", "partitions": 6}],
            "incidents": [
                {
                    "group": {
                        "repeat": 3,
                        "interval_seconds": 60,
                        "incidents": [
                            {"type": "stop_broker", "at_seconds": 0, "broker": "kafka-2"},
                            {"type": "start_broker", "at_seconds": 30, "broker": "kafka-2"},
                        ],
                    }
                }
            ],
        }
        result = validate_scenario_file(create_temp_yaml(data))
        assert result.valid

    def test_invalid_scenario_multiple_errors(self):
        data = {
            "name": "invalid-scenario",
            "topics": [
                {
                    "name": "test-topic",
                    "partitions": -1,
                    "replication_factor": 5,
                    "message_schema": {
                        "key_distribution": "invalid",
                        "min_size_bytes": 1000,
                        "max_size_bytes": 100,
                    },
                }
            ],
            "incidents": [
                {"type": "invalid_type", "at_seconds": 30},
                {"type": "stop_broker", "at_seconds": 30},
            ],
        }
        result = validate_scenario_file(create_temp_yaml(data))
        assert not result.valid
        assert len(result.errors) >= 5
