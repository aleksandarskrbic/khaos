from pathlib import Path

import yaml

from khaos.scenarios.incidents import get_incident_names
from khaos.validators.common import ValidationResult
from khaos.validators.flow import FlowValidator
from khaos.validators.schema import SchemaValidator

VALID_KEY_DISTRIBUTIONS = {"uniform", "zipfian", "single_key", "round_robin"}
VALID_COMPRESSION_TYPES = {"none", "gzip", "snappy", "lz4", "zstd"}
VALID_DATA_FORMATS = {"json", "avro", "protobuf"}
VALID_ACKS = {"0", "1", "all", "-1"}
VALID_BROKERS = {"kafka-1", "kafka-2", "kafka-3"}
VALID_SCHEMA_PROVIDERS = {"inline", "registry"}


def validate_scenario_file(file_path: Path) -> ValidationResult:
    result = ValidationResult(valid=True)

    if not file_path.exists():
        result.add_error("file", f"File not found: {file_path}")
        return result

    try:
        with file_path.open() as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        result.add_error("yaml", f"Invalid YAML syntax: {e}")
        return result

    if not isinstance(data, dict):
        result.add_error("root", "Scenario must be a YAML object/dict")
        return result

    if "name" not in data:
        result.add_error("name", "Missing required field 'name'")
    elif not isinstance(data["name"], str):
        result.add_error("name", "Field 'name' must be a string")

    topics = data.get("topics")
    flows = data.get("flows")
    has_topics = isinstance(topics, list) and len(topics) > 0
    has_flows = isinstance(flows, list) and len(flows) > 0

    if not has_topics and not has_flows:
        result.add_error("topics", "Scenario must have at least 'topics' or 'flows'")

    # Validate schema_registry if present
    has_schema_registry = False
    if "schema_registry" in data:
        has_schema_registry = True
        validate_schema_registry(data["schema_registry"], "schema_registry", result)

    if "topics" in data:
        if not isinstance(data["topics"], list):
            result.add_error("topics", "Field 'topics' must be a list")
        else:
            has_schema_format_topic = False
            schema_format_name = None
            for i, topic in enumerate(data["topics"]):
                validate_topic(topic, f"topics[{i}]", result)
                # Check if any topic uses avro or protobuf
                msg_schema = topic.get("message_schema", {})
                if msg_schema.get("data_format") in ("avro", "protobuf"):
                    has_schema_format_topic = True
                    schema_format_name = msg_schema.get("data_format")

            # Info if avro/protobuf is used without schema_registry
            if has_schema_format_topic and not has_schema_registry and schema_format_name:
                result.add_warning(
                    "schema_registry",
                    f"Using {schema_format_name.title()} without Schema Registry (schemaless mode)",
                )

    if "flows" in data:
        if not isinstance(data["flows"], list):
            result.add_error("flows", "Field 'flows' must be a list")
        else:
            flow_validator = FlowValidator()
            flow_result = flow_validator.validate(data["flows"])
            for error in flow_result.errors:
                result.add_error(error.path, error.message)
            for warning in flow_result.warnings:
                result.add_warning(warning.path, warning.message)

    if "incidents" in data:
        if not isinstance(data["incidents"], list):
            result.add_error("incidents", "Field 'incidents' must be a list")
        else:
            for i, incident in enumerate(data["incidents"]):
                validate_incident(incident, f"incidents[{i}]", result)

    return result


def validate_topic(topic: dict, path: str, result: ValidationResult) -> None:
    if not isinstance(topic, dict):
        result.add_error(path, "Topic must be an object/dict")
        return

    if "name" not in topic:
        result.add_error(f"{path}.name", "Missing required field 'name'")
    elif not isinstance(topic["name"], str):
        result.add_error(f"{path}.name", "Field 'name' must be a string")

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
        if not isinstance(topic["num_producers"], int) or topic["num_producers"] < 0:
            result.add_error(
                f"{path}.num_producers", "Field 'num_producers' must be a non-negative integer"
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

    # Get schema_provider early for message_schema validation
    schema_provider = topic.get("schema_provider", "inline")

    if "message_schema" in topic:
        validate_message_schema(
            topic["message_schema"], f"{path}.message_schema", result, schema_provider
        )

    if "producer_config" in topic:
        validate_producer_config(topic["producer_config"], f"{path}.producer_config", result)

    # Validate schema_provider
    if schema_provider not in VALID_SCHEMA_PROVIDERS:
        result.add_error(
            f"{path}.schema_provider",
            f"Invalid schema_provider '{schema_provider}'. "
            f"Valid values: {', '.join(sorted(VALID_SCHEMA_PROVIDERS))}",
        )

    # Validate subject_name when using registry provider
    if schema_provider == "registry":
        if "subject_name" not in topic:
            result.add_error(
                f"{path}.subject_name",
                "Field 'subject_name' is required when schema_provider is 'registry'",
            )
        elif not isinstance(topic["subject_name"], str):
            result.add_error(f"{path}.subject_name", "Field 'subject_name' must be a string")

        # Cannot have both schema_provider=registry and message_schema with fields
        if "message_schema" in topic and topic["message_schema"].get("fields"):
            result.add_error(
                f"{path}",
                "Cannot define 'message_schema.fields' when schema_provider is 'registry' "
                "(schema will be fetched from Schema Registry)",
            )


def validate_message_schema(
    schema: dict, path: str, result: ValidationResult, schema_provider: str = "inline"
) -> None:
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

    if "data_format" in schema:
        if schema["data_format"] not in VALID_DATA_FORMATS:
            result.add_error(
                f"{path}.data_format",
                f"Invalid data_format '{schema['data_format']}'. "
                f"Valid values: {', '.join(sorted(VALID_DATA_FORMATS))}",
            )
        # Avro and Protobuf require fields when using inline provider
        # When using registry provider, fields are fetched from Schema Registry
        if (
            schema_provider == "inline"
            and schema["data_format"] in ("avro", "protobuf")
            and not schema.get("fields")
        ):
            result.add_error(
                f"{path}.fields",
                f"data_format '{schema['data_format']}' requires 'fields' to be defined "
                "(or use schema_provider: registry to fetch from Schema Registry)",
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

    min_size = schema.get("min_size_bytes", 200)
    max_size = schema.get("max_size_bytes", 500)
    if isinstance(min_size, int) and isinstance(max_size, int) and min_size > max_size:
        result.add_error(f"{path}", "min_size_bytes cannot be greater than max_size_bytes")

    if "fields" in schema:
        schema_validator = SchemaValidator()
        schema_result = schema_validator.validate(schema["fields"], f"{path}.fields")
        for error in schema_result.errors:
            result.add_error(error.path, error.message)
        for warning in schema_result.warnings:
            result.add_warning(warning.path, warning.message)


def validate_schema_registry(config: dict, path: str, result: ValidationResult) -> None:
    if not isinstance(config, dict):
        result.add_error(path, "schema_registry must be an object/dict")
        return

    if "url" not in config:
        result.add_error(f"{path}.url", "Missing required field 'url'")
    elif not isinstance(config["url"], str):
        result.add_error(f"{path}.url", "Field 'url' must be a string")
    elif not config["url"].startswith(("http://", "https://")):
        result.add_error(f"{path}.url", "Field 'url' must be a valid HTTP(S) URL")


def validate_producer_config(config: dict, path: str, result: ValidationResult) -> None:
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


def _validate_incident_type_fields(
    incident: dict, incident_type: str, path: str, result: ValidationResult, strict: bool = True
) -> None:
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
        elif strict and (not isinstance(incident["delay_ms"], int) or incident["delay_ms"] < 0):
            result.add_error(f"{path}.delay_ms", "Field 'delay_ms' must be a non-negative integer")

    if incident_type == "change_producer_rate":
        if "rate" not in incident:
            result.add_error(
                f"{path}.rate", "Incident type 'change_producer_rate' requires 'rate' field"
            )
        elif strict and (not isinstance(incident["rate"], int | float) or incident["rate"] < 0):
            result.add_error(f"{path}.rate", "Field 'rate' must be a non-negative number")

    if incident_type == "pause_consumer":
        if "duration_seconds" not in incident:
            result.add_error(
                f"{path}.duration_seconds",
                "Incident type 'pause_consumer' requires 'duration_seconds' field",
            )
        elif strict and (
            not isinstance(incident["duration_seconds"], int) or incident["duration_seconds"] < 1
        ):
            result.add_error(
                f"{path}.duration_seconds", "Field 'duration_seconds' must be a positive integer"
            )


def validate_incident(incident: dict, path: str, result: ValidationResult) -> None:
    if not isinstance(incident, dict):
        result.add_error(path, "Incident must be an object/dict")
        return

    if "group" in incident:
        validate_incident_group(incident["group"], f"{path}.group", result)
        return

    if "type" not in incident:
        result.add_error(f"{path}.type", "Missing required field 'type'")
        return

    incident_type = incident["type"]
    if incident_type not in get_incident_names():
        result.add_error(
            f"{path}.type",
            f"Unknown incident type '{incident_type}'. "
            f"Valid types: {', '.join(sorted(get_incident_names()))}",
        )
        return

    # Validate scheduling
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

    # Validate target if present
    if "target" in incident:
        validate_target(incident["target"], f"{path}.target", result)

    _validate_incident_type_fields(incident, incident_type, path, result, strict=True)


def validate_target(target: dict, path: str, result: ValidationResult) -> None:
    """Validate consumer/producer target."""
    if not isinstance(target, dict):
        result.add_error(path, "Target must be an object/dict")
        return

    if "topic" in target and not isinstance(target["topic"], str):
        result.add_error(f"{path}.topic", "Field 'topic' must be a string")

    if "group" in target and not isinstance(target["group"], str):
        result.add_error(f"{path}.group", "Field 'group' must be a string")

    if "percentage" in target:
        if not isinstance(target["percentage"], int) or not 1 <= target["percentage"] <= 100:
            result.add_error(
                f"{path}.percentage", "Field 'percentage' must be an integer between 1 and 100"
            )

    if "count" in target:
        if not isinstance(target["count"], int) or target["count"] < 1:
            result.add_error(f"{path}.count", "Field 'count' must be a positive integer")

    if "indices" in target:
        if not isinstance(target["indices"], list):
            result.add_error(f"{path}.indices", "Field 'indices' must be a list")
        elif not all(isinstance(i, int) and i >= 0 for i in target["indices"]):
            result.add_error(
                f"{path}.indices", "Field 'indices' must be a list of non-negative integers"
            )

    # Check mutual exclusivity
    selection_fields = ["percentage", "count", "indices"]
    set_fields = [f for f in selection_fields if f in target]
    if len(set_fields) > 1:
        result.add_error(
            path,
            f"Only one of {', '.join(selection_fields)} can be set, got: {', '.join(set_fields)}",
        )


def validate_incident_group(group: dict, path: str, result: ValidationResult) -> None:
    if not isinstance(group, dict):
        result.add_error(path, "Incident group must be an object/dict")
        return

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
            validate_group_incident(incident, f"{path}.incidents[{i}]", result)

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
    if not isinstance(incident, dict):
        result.add_error(path, "Incident must be an object/dict")
        return

    if "type" not in incident:
        result.add_error(f"{path}.type", "Missing required field 'type'")
        return

    incident_type = incident["type"]
    if incident_type not in get_incident_names():
        result.add_error(
            f"{path}.type",
            f"Unknown incident type '{incident_type}'. "
            f"Valid types: {', '.join(sorted(get_incident_names()))}",
        )
        return

    # Group incidents should use at_seconds (relative to cycle start)
    if "at_seconds" not in incident:
        result.add_warning(
            f"{path}", "Group incidents should use 'at_seconds' (relative to cycle start)"
        )

    if "at_seconds" in incident and not isinstance(incident["at_seconds"], int):
        result.add_error(f"{path}.at_seconds", "Field 'at_seconds' must be an integer")

    # Validate target if present
    if "target" in incident:
        validate_target(incident["target"], f"{path}.target", result)

    _validate_incident_type_fields(incident, incident_type, path, result, strict=False)
