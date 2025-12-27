"""Convert Avro and Protobuf schemas to FieldSchema for data generation."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient

from khaos.models.schema import FieldSchema

logger = logging.getLogger(__name__)

AVRO_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "long": "int",
    "int": "int",
    "double": "float",
    "float": "float",
    "boolean": "boolean",
    "bytes": "string",  # Base64 encoded
}

PROTO_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "int32": "int",
    "int64": "int",
    "sint32": "int",
    "sint64": "int",
    "uint32": "int",
    "uint64": "int",
    "fixed32": "int",
    "fixed64": "int",
    "sfixed32": "int",
    "sfixed64": "int",
    "double": "float",
    "float": "float",
    "bool": "boolean",
    "bytes": "string",
}


class SchemaRegistryProvider:
    """Fetches schemas from Schema Registry and converts to FieldSchema.

    This class provides caching and connection reuse for efficient schema fetching.

    Example:
        provider = SchemaRegistryProvider("http://localhost:8081")
        data_format, fields = provider.get_field_schemas("orders-value")
        raw_schema = provider.get_raw_schema("orders-value")  # For serialization
    """

    def __init__(self, registry_url: str):
        """Initialize the provider.

        Args:
            registry_url: Schema Registry URL (e.g., "http://localhost:8081")
        """
        self.registry_url = registry_url
        self._client: SchemaRegistryClient | None = None
        self._cache: dict[str, tuple[str, list[FieldSchema]]] = {}
        self._raw_schema_cache: dict[str, tuple[str, dict | str]] = {}

    @property
    def client(self) -> SchemaRegistryClient:
        """Lazy initialization of Schema Registry client."""
        if self._client is None:
            self._client = SchemaRegistryClient({"url": self.registry_url})
        return self._client

    def get_field_schemas(self, subject_name: str) -> tuple[str, list[FieldSchema]]:
        """Get field schemas for a subject (cached).

        Args:
            subject_name: Subject name (e.g., "orders-value")

        Returns:
            Tuple of (data_format, field_schemas) where data_format is "avro" or "protobuf"

        Raises:
            ValueError: If schema not found or unsupported type
        """
        if subject_name not in self._cache:
            self._fetch_and_cache(subject_name)
        return self._cache[subject_name]

    def get_raw_schema(self, subject_name: str) -> tuple[str, dict | str]:
        """Get the raw schema for a subject (cached).

        Args:
            subject_name: Subject name (e.g., "orders-value")

        Returns:
            Tuple of (data_format, raw_schema) where raw_schema is:
            - dict for Avro schemas
            - str for Protobuf schemas

        Raises:
            ValueError: If schema not found or unsupported type
        """
        if subject_name not in self._raw_schema_cache:
            self._fetch_and_cache(subject_name)
        return self._raw_schema_cache[subject_name]

    def _fetch_and_cache(self, subject_name: str) -> None:
        """Fetch schema from registry and cache both raw and converted forms."""
        try:
            latest = self.client.get_latest_version(subject_name)
        except Exception as e:
            raise ValueError(f"Schema not found for subject '{subject_name}': {e}")

        schema_type = latest.schema.schema_type
        schema_str = latest.schema.schema_str

        if schema_type == "AVRO":
            avro_schema = json.loads(schema_str)
            self._raw_schema_cache[subject_name] = ("avro", avro_schema)
            self._cache[subject_name] = ("avro", avro_to_field_schemas(avro_schema))
        elif schema_type == "PROTOBUF":
            self._raw_schema_cache[subject_name] = ("protobuf", schema_str)
            self._cache[subject_name] = ("protobuf", protobuf_to_field_schemas(schema_str))
        else:
            raise ValueError(f"Unsupported schema type: {schema_type}")

    def clear_cache(self) -> None:
        """Clear the schema cache."""
        self._cache.clear()
        self._raw_schema_cache.clear()

    def is_cached(self, subject_name: str) -> bool:
        """Check if a subject is cached."""
        return subject_name in self._cache


def avro_to_field_schemas(avro_schema: dict[str, Any]) -> list[FieldSchema]:
    """Convert an Avro schema to a list of FieldSchema objects.

    Args:
        avro_schema: Parsed Avro schema (dict)

    Returns:
        List of FieldSchema objects for data generation
    """
    if avro_schema.get("type") != "record":
        raise ValueError(f"Expected Avro record type, got: {avro_schema.get('type')}")

    fields = []
    for field in avro_schema.get("fields", []):
        field_schema = _avro_field_to_field_schema(field)
        if field_schema:
            fields.append(field_schema)

    return fields


def protobuf_to_field_schemas(proto_schema_str: str) -> list[FieldSchema]:
    """Convert a Protobuf schema to a list of FieldSchema objects.

    This parses the .proto text and extracts field definitions.
    Note: This is a basic parser for common cases.

    Args:
        proto_schema_str: Protobuf schema as string (.proto format)

    Returns:
        List of FieldSchema objects for data generation
    """
    fields: list[FieldSchema] = []

    # Find message definition
    message_match = re.search(r"message\s+\w+\s*\{([^}]+)\}", proto_schema_str, re.DOTALL)
    if not message_match:
        logger.warning("Could not find message definition in Protobuf schema")
        return fields

    message_body = message_match.group(1)

    # Parse field definitions: type name = number;
    field_pattern = re.compile(r"(repeated\s+)?(\w+)\s+(\w+)\s*=\s*\d+\s*;", re.MULTILINE)

    for match in field_pattern.finditer(message_body):
        is_repeated = match.group(1) is not None
        proto_type = match.group(2)
        field_name = match.group(3)

        field_schema = _proto_type_to_field_schema(field_name, proto_type, is_repeated)
        if field_schema:
            fields.append(field_schema)

    return fields


def _avro_field_to_field_schema(avro_field: dict[str, Any]) -> FieldSchema | None:
    """Convert a single Avro field to FieldSchema."""
    name = avro_field["name"]
    avro_type = avro_field["type"]

    # Handle union types (e.g., ["null", "string"])
    if isinstance(avro_type, list):
        # Extract non-null type
        non_null_types = [t for t in avro_type if t != "null"]
        if not non_null_types:
            logger.warning(f"Field '{name}' is all nulls, skipping")
            return None
        avro_type = non_null_types[0]

    # Handle logical types
    if isinstance(avro_type, dict):
        logical_type = avro_type.get("logicalType")
        if logical_type == "uuid":
            return FieldSchema(name=name, type="uuid")
        elif logical_type in ("timestamp-millis", "timestamp-micros"):
            return FieldSchema(name=name, type="timestamp")
        elif logical_type in ("date", "time-millis"):
            return FieldSchema(name=name, type="int")

        # Handle nested record
        if avro_type.get("type") == "record":
            nested_fields_raw = [
                _avro_field_to_field_schema(f) for f in avro_type.get("fields", [])
            ]
            nested_fields = [f for f in nested_fields_raw if f is not None]
            return FieldSchema(name=name, type="object", fields=nested_fields)

        # Handle enum
        if avro_type.get("type") == "enum":
            symbols = avro_type.get("symbols", [])
            return FieldSchema(name=name, type="enum", values=symbols)

        # Handle array
        if avro_type.get("type") == "array":
            items_type = avro_type.get("items")
            items_schema = _convert_avro_type_to_field_schema("item", items_type)
            return FieldSchema(name=name, type="array", items=items_schema)

        # Handle map as object
        if avro_type.get("type") == "map":
            logger.warning(f"Field '{name}' is a map, treating as object with string keys")
            return FieldSchema(name=name, type="object", fields=[])

        # Fallback for complex dict types
        inner_type = avro_type.get("type", "string")
        if inner_type in AVRO_TYPE_MAP:
            return FieldSchema(name=name, type=AVRO_TYPE_MAP[inner_type])

    # Handle simple types
    if isinstance(avro_type, str):
        if avro_type in AVRO_TYPE_MAP:
            return FieldSchema(name=name, type=AVRO_TYPE_MAP[avro_type])
        else:
            logger.warning(f"Unknown Avro type '{avro_type}' for field '{name}', using string")
            return FieldSchema(name=name, type="string")

    logger.warning(f"Could not convert Avro field '{name}', using string")
    return FieldSchema(name=name, type="string")


def _convert_avro_type_to_field_schema(name: str, avro_type: Any) -> FieldSchema:
    """Convert an Avro type to FieldSchema (for array items, etc.)."""
    if isinstance(avro_type, str):
        field_type = AVRO_TYPE_MAP.get(avro_type, "string")
        return FieldSchema(name=name, type=field_type)

    if isinstance(avro_type, dict):
        if avro_type.get("type") == "record":
            nested_fields_raw = [
                _avro_field_to_field_schema(f) for f in avro_type.get("fields", [])
            ]
            nested_fields = [f for f in nested_fields_raw if f is not None]
            return FieldSchema(name=name, type="object", fields=nested_fields)

        if avro_type.get("type") == "enum":
            return FieldSchema(name=name, type="enum", values=avro_type.get("symbols", []))

        inner_type = avro_type.get("type", "string")
        field_type = AVRO_TYPE_MAP.get(inner_type, "string")
        return FieldSchema(name=name, type=field_type)

    return FieldSchema(name=name, type="string")


def _proto_type_to_field_schema(
    name: str, proto_type: str, is_repeated: bool = False
) -> FieldSchema | None:
    """Convert a Protobuf type to FieldSchema."""
    if is_repeated:
        # Handle repeated as array
        item_type = PROTO_TYPE_MAP.get(proto_type, "string")
        items_schema = FieldSchema(name="item", type=item_type)
        return FieldSchema(name=name, type="array", items=items_schema)

    if proto_type in PROTO_TYPE_MAP:
        return FieldSchema(name=name, type=PROTO_TYPE_MAP[proto_type])

    # Assume custom message type is an object
    logger.warning(f"Unknown Protobuf type '{proto_type}' for field '{name}', using object")
    return FieldSchema(name=name, type="object", fields=[])
