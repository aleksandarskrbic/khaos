"""Scenario validator - validates YAML scenario definitions before execution."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from khaos.scenarios.incidents import INCIDENT_HANDLERS


@dataclass
class ValidationError:
    """A single validation error."""

    path: str  # e.g., "topics[0].partitions"
    message: str


@dataclass
class ValidationResult:
    """Result of validating a scenario."""

    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    def add_error(self, path: str, message: str) -> None:
        self.errors.append(ValidationError(path, message))
        self.valid = False

    def add_warning(self, path: str, message: str) -> None:
        self.warnings.append(ValidationError(path, message))


# Valid values for various fields
VALID_KEY_DISTRIBUTIONS = {"uniform", "zipfian", "single_key", "round_robin"}
VALID_COMPRESSION_TYPES = {"none", "gzip", "snappy", "lz4", "zstd"}
VALID_ACKS = {"0", "1", "all", "-1"}
VALID_BROKERS = {"kafka-1", "kafka-2", "kafka-3"}


def validate_scenario_file(file_path: Path) -> ValidationResult:
    """Validate a scenario YAML file."""
    result = ValidationResult(valid=True)

    # Check file exists
    if not file_path.exists():
        result.add_error("file", f"File not found: {file_path}")
        return result

    # Parse YAML
    try:
        with file_path.open() as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        result.add_error("yaml", f"Invalid YAML syntax: {e}")
        return result

    if not isinstance(data, dict):
        result.add_error("root", "Scenario must be a YAML object/dict")
        return result

    # Validate required fields
    if "name" not in data:
        result.add_error("name", "Missing required field 'name'")
    elif not isinstance(data["name"], str):
        result.add_error("name", "Field 'name' must be a string")

    if "topics" not in data:
        result.add_error("topics", "Missing required field 'topics'")
    elif not isinstance(data["topics"], list):
        result.add_error("topics", "Field 'topics' must be a list")
    elif len(data["topics"]) == 0:
        result.add_error("topics", "At least one topic is required")
    else:
        # Validate each topic
        for i, topic in enumerate(data["topics"]):
            validate_topic(topic, f"topics[{i}]", result)

    # Validate incidents (optional)
    if "incidents" in data:
        if not isinstance(data["incidents"], list):
            result.add_error("incidents", "Field 'incidents' must be a list")
        else:
            for i, incident in enumerate(data["incidents"]):
                validate_incident(incident, f"incidents[{i}]", result)

    return result


def validate_topic(topic: dict, path: str, result: ValidationResult) -> None:
    """Validate a topic configuration."""
    if not isinstance(topic, dict):
        result.add_error(path, "Topic must be an object/dict")
        return

    # Required fields
    if "name" not in topic:
        result.add_error(f"{path}.name", "Missing required field 'name'")
    elif not isinstance(topic["name"], str):
        result.add_error(f"{path}.name", "Field 'name' must be a string")

    # Numeric fields with bounds
    if "partitions" in topic:
        if not isinstance(topic["partitions"], int) or topic["partitions"] < 1:
            result.add_error(f"{path}.partitions", "Field 'partitions' must be a positive integer")
        elif topic["partitions"] > 100:
            result.add_warning(
                f"{path}.partitions",
                f"High partition count ({topic['partitions']}) may impact performance",
            )

    if "replication_factor" in topic:
        rf = topic["replication_factor"]
        if not isinstance(rf, int) or rf < 1:
            result.add_error(
                f"{path}.replication_factor",
                "Field 'replication_factor' must be a positive integer",
            )
        elif rf > 3:
            result.add_error(
                f"{path}.replication_factor",
                "Replication factor cannot exceed 3 (cluster has 3 brokers)",
            )

    if "num_producers" in topic:
        if not isinstance(topic["num_producers"], int) or topic["num_producers"] < 1:
            result.add_error(
                f"{path}.num_producers", "Field 'num_producers' must be a positive integer"
            )

    if "num_consumer_groups" in topic:
        if not isinstance(topic["num_consumer_groups"], int) or topic["num_consumer_groups"] < 1:
            result.add_error(
                f"{path}.num_consumer_groups",
                "Field 'num_consumer_groups' must be a positive integer",
            )

    if "consumers_per_group" in topic:
        if not isinstance(topic["consumers_per_group"], int) or topic["consumers_per_group"] < 1:
            result.add_error(
                f"{path}.consumers_per_group",
                "Field 'consumers_per_group' must be a positive integer",
            )

    if "producer_rate" in topic:
        rate = topic["producer_rate"]
        if not isinstance(rate, int | float) or rate <= 0:
            result.add_error(
                f"{path}.producer_rate", "Field 'producer_rate' must be a positive number"
            )

    if "consumer_delay_ms" in topic:
        if not isinstance(topic["consumer_delay_ms"], int) or topic["consumer_delay_ms"] < 0:
            result.add_error(
                f"{path}.consumer_delay_ms",
                "Field 'consumer_delay_ms' must be a non-negative integer",
            )

    # Validate message_schema
    if "message_schema" in topic:
        validate_message_schema(topic["message_schema"], f"{path}.message_schema", result)

    # Validate producer_config
    if "producer_config" in topic:
        validate_producer_config(topic["producer_config"], f"{path}.producer_config", result)


def validate_message_schema(schema: dict, path: str, result: ValidationResult) -> None:
    """Validate message schema configuration."""
    if not isinstance(schema, dict):
        result.add_error(path, "message_schema must be an object/dict")
        return

    if "key_distribution" in schema:
        if schema["key_distribution"] not in VALID_KEY_DISTRIBUTIONS:
            result.add_error(
                f"{path}.key_distribution",
                f"Invalid key_distribution '{schema['key_distribution']}'. "
                f"Valid values: {', '.join(sorted(VALID_KEY_DISTRIBUTIONS))}",
            )

    if "key_cardinality" in schema:
        if not isinstance(schema["key_cardinality"], int) or schema["key_cardinality"] < 1:
            result.add_error(
                f"{path}.key_cardinality", "Field 'key_cardinality' must be a positive integer"
            )

    if "min_size_bytes" in schema:
        if not isinstance(schema["min_size_bytes"], int) or schema["min_size_bytes"] < 1:
            result.add_error(
                f"{path}.min_size_bytes", "Field 'min_size_bytes' must be a positive integer"
            )

    if "max_size_bytes" in schema:
        if not isinstance(schema["max_size_bytes"], int) or schema["max_size_bytes"] < 1:
            result.add_error(
                f"{path}.max_size_bytes", "Field 'max_size_bytes' must be a positive integer"
            )

    # Check min <= max
    min_size = schema.get("min_size_bytes", 200)
    max_size = schema.get("max_size_bytes", 500)
    if isinstance(min_size, int) and isinstance(max_size, int) and min_size > max_size:
        result.add_error(f"{path}", "min_size_bytes cannot be greater than max_size_bytes")


def validate_producer_config(config: dict, path: str, result: ValidationResult) -> None:
    """Validate producer configuration."""
    if not isinstance(config, dict):
        result.add_error(path, "producer_config must be an object/dict")
        return

    if "batch_size" in config:
        if not isinstance(config["batch_size"], int) or config["batch_size"] < 0:
            result.add_error(
                f"{path}.batch_size", "Field 'batch_size' must be a non-negative integer"
            )

    if "linger_ms" in config:
        if not isinstance(config["linger_ms"], int) or config["linger_ms"] < 0:
            result.add_error(
                f"{path}.linger_ms", "Field 'linger_ms' must be a non-negative integer"
            )

    if "acks" in config:
        acks_str = str(config["acks"])
        if acks_str not in VALID_ACKS:
            result.add_error(
                f"{path}.acks",
                f"Invalid acks '{config['acks']}'. Valid values: {', '.join(sorted(VALID_ACKS))}",
            )

    if "compression_type" in config:
        if config["compression_type"] not in VALID_COMPRESSION_TYPES:
            result.add_error(
                f"{path}.compression_type",
                f"Invalid compression_type '{config['compression_type']}'. "
                f"Valid values: {', '.join(sorted(VALID_COMPRESSION_TYPES))}",
            )


def validate_incident(incident: dict, path: str, result: ValidationResult) -> None:
    """Validate an incident configuration."""
    if not isinstance(incident, dict):
        result.add_error(path, "Incident must be an object/dict")
        return

    # Check if it's a group
    if "group" in incident:
        validate_incident_group(incident["group"], f"{path}.group", result)
        return

    # Regular incident
    if "type" not in incident:
        result.add_error(f"{path}.type", "Missing required field 'type'")
        return

    incident_type = incident["type"]
    if incident_type not in INCIDENT_HANDLERS:
        result.add_error(
            f"{path}.type",
            f"Unknown incident type '{incident_type}'. "
            f"Valid types: {', '.join(sorted(INCIDENT_HANDLERS.keys()))}",
        )
        return

    # Check timing - need either at_seconds or every_seconds
    has_at = "at_seconds" in incident
    has_every = "every_seconds" in incident
    if not has_at and not has_every:
        result.add_error(f"{path}", "Incident must have either 'at_seconds' or 'every_seconds'")

    if has_at and not isinstance(incident["at_seconds"], int):
        result.add_error(f"{path}.at_seconds", "Field 'at_seconds' must be an integer")

    if has_every and (
        not isinstance(incident["every_seconds"], int) or incident["every_seconds"] < 1
    ):
        result.add_error(
            f"{path}.every_seconds", "Field 'every_seconds' must be a positive integer"
        )

    # Type-specific validation
    if incident_type in ("stop_broker", "start_broker"):
        if "broker" not in incident:
            result.add_error(
                f"{path}.broker", f"Incident type '{incident_type}' requires 'broker' field"
            )
        elif incident["broker"] not in VALID_BROKERS:
            valid = ", ".join(sorted(VALID_BROKERS))
            result.add_error(f"{path}.broker", f"Invalid broker. Valid: {valid}")

    if incident_type == "increase_consumer_delay":
        if "delay_ms" not in incident:
            result.add_error(
                f"{path}.delay_ms",
                "Incident type 'increase_consumer_delay' requires 'delay_ms' field",
            )
        elif not isinstance(incident["delay_ms"], int) or incident["delay_ms"] < 0:
            result.add_error(f"{path}.delay_ms", "Field 'delay_ms' must be a non-negative integer")

    if incident_type == "change_producer_rate":
        if "rate" not in incident:
            result.add_error(
                f"{path}.rate", "Incident type 'change_producer_rate' requires 'rate' field"
            )
        elif not isinstance(incident["rate"], int | float) or incident["rate"] < 0:
            result.add_error(f"{path}.rate", "Field 'rate' must be a non-negative number")

    if incident_type == "pause_consumer":
        if "duration_seconds" not in incident:
            result.add_error(
                f"{path}.duration_seconds",
                "Incident type 'pause_consumer' requires 'duration_seconds' field",
            )
        elif not isinstance(incident["duration_seconds"], int) or incident["duration_seconds"] < 1:
            result.add_error(
                f"{path}.duration_seconds", "Field 'duration_seconds' must be a positive integer"
            )


def validate_incident_group(group: dict, path: str, result: ValidationResult) -> None:
    """Validate an incident group configuration."""
    if not isinstance(group, dict):
        result.add_error(path, "Incident group must be an object/dict")
        return

    # Required fields
    if "repeat" not in group:
        result.add_error(f"{path}.repeat", "Missing required field 'repeat'")
    elif not isinstance(group["repeat"], int) or group["repeat"] < 1:
        result.add_error(f"{path}.repeat", "Field 'repeat' must be a positive integer")

    if "interval_seconds" not in group:
        result.add_error(f"{path}.interval_seconds", "Missing required field 'interval_seconds'")
    elif not isinstance(group["interval_seconds"], int) or group["interval_seconds"] < 1:
        result.add_error(
            f"{path}.interval_seconds", "Field 'interval_seconds' must be a positive integer"
        )

    if "incidents" not in group:
        result.add_error(f"{path}.incidents", "Missing required field 'incidents'")
    elif not isinstance(group["incidents"], list):
        result.add_error(f"{path}.incidents", "Field 'incidents' must be a list")
    elif len(group["incidents"]) == 0:
        result.add_error(f"{path}.incidents", "Incident group must have at least one incident")
    else:
        for i, incident in enumerate(group["incidents"]):
            # Validate each incident in the group (but they use at_seconds relative to cycle)
            validate_group_incident(incident, f"{path}.incidents[{i}]", result)

    # Check that incidents fit within interval
    if (
        "interval_seconds" in group
        and "incidents" in group
        and isinstance(group["incidents"], list)
    ):
        interval = group["interval_seconds"]
        for i, inc in enumerate(group["incidents"]):
            if isinstance(inc, dict) and "at_seconds" in inc:
                at = inc["at_seconds"]
                if isinstance(at, int) and at >= interval:
                    msg = f"at_seconds ({at}) >= interval ({interval}), may overlap"
                    result.add_warning(f"{path}.incidents[{i}].at_seconds", msg)


def validate_group_incident(incident: dict, path: str, result: ValidationResult) -> None:
    """Validate an incident within a group (uses at_seconds relative to cycle start)."""
    if not isinstance(incident, dict):
        result.add_error(path, "Incident must be an object/dict")
        return

    if "type" not in incident:
        result.add_error(f"{path}.type", "Missing required field 'type'")
        return

    incident_type = incident["type"]
    if incident_type not in INCIDENT_HANDLERS:
        result.add_error(
            f"{path}.type",
            f"Unknown incident type '{incident_type}'. "
            f"Valid types: {', '.join(sorted(INCIDENT_HANDLERS.keys()))}",
        )
        return

    # Group incidents should use at_seconds (relative to cycle start)
    if "at_seconds" not in incident:
        result.add_warning(
            f"{path}", "Group incidents should use 'at_seconds' (relative to cycle start)"
        )

    if "at_seconds" in incident and not isinstance(incident["at_seconds"], int):
        result.add_error(f"{path}.at_seconds", "Field 'at_seconds' must be an integer")

    # Type-specific validation (same as regular incidents)
    if incident_type in ("stop_broker", "start_broker"):
        if "broker" not in incident:
            result.add_error(
                f"{path}.broker", f"Incident type '{incident_type}' requires 'broker' field"
            )
        elif incident["broker"] not in VALID_BROKERS:
            valid = ", ".join(sorted(VALID_BROKERS))
            result.add_error(f"{path}.broker", f"Invalid broker. Valid: {valid}")

    if incident_type == "increase_consumer_delay":
        if "delay_ms" not in incident:
            result.add_error(
                f"{path}.delay_ms",
                "Incident type 'increase_consumer_delay' requires 'delay_ms' field",
            )

    if incident_type == "change_producer_rate":
        if "rate" not in incident:
            result.add_error(
                f"{path}.rate", "Incident type 'change_producer_rate' requires 'rate' field"
            )

    if incident_type == "pause_consumer":
        if "duration_seconds" not in incident:
            result.add_error(
                f"{path}.duration_seconds",
                "Incident type 'pause_consumer' requires 'duration_seconds' field",
            )
