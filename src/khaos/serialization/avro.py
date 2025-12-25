from __future__ import annotations

import io
import json
from typing import Any, cast

import fastavro
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer as ConfluentAvroDeserializer
from confluent_kafka.schema_registry.avro import AvroSerializer as ConfluentAvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from khaos.models.schema import FieldSchema

FIELD_TYPE_MAP = {
    "string": "string",
    "int": "long",
    "float": "double",
    "boolean": "boolean",
    "faker": "string",
}


def field_schema_to_avro_type(field: FieldSchema) -> dict[str, Any]:
    field_type = field.type

    if field_type in FIELD_TYPE_MAP:
        return {"type": FIELD_TYPE_MAP[field_type]}

    if field_type == "uuid":
        return {"type": "string", "logicalType": "uuid"}

    if field_type == "timestamp":
        return {"type": "long", "logicalType": "timestamp-millis"}

    if field_type == "enum":
        if not field.values:
            return {"type": "string"}
        return {
            "type": "enum",
            "name": f"{field.name.title().replace('_', '')}Enum",
            "symbols": field.values,
        }

    if field_type == "object":
        if not field.fields:
            return {"type": "map", "values": "string"}
        return {
            "type": "record",
            "name": f"{field.name.title().replace('_', '')}Record",
            "fields": [
                {"name": f.name, "type": field_schema_to_avro_type(f)} for f in field.fields
            ],
        }

    if field_type == "array":
        if not field.items:
            return {"type": "array", "items": {"type": "string"}}
        return {"type": "array", "items": field_schema_to_avro_type(field.items)}

    return {"type": "string"}


def field_schemas_to_avro(fields: list[FieldSchema], name: str) -> dict[str, Any]:
    return {
        "type": "record",
        "name": name,
        "namespace": "khaos.generated",
        "fields": [
            {"name": field.name, "type": field_schema_to_avro_type(field)} for field in fields
        ],
    }


class AvroSerializer:
    def __init__(
        self,
        schema_registry_url: str,
        schema: dict[str, Any],
        topic: str,
    ):
        self.schema = schema
        self.topic = topic
        self.parsed_schema = fastavro.parse_schema(schema)
        self.registry_client = SchemaRegistryClient({"url": schema_registry_url})
        avro_schema = Schema(json.dumps(schema), "AVRO")
        self._confluent_serializer = ConfluentAvroSerializer(
            self.registry_client,
            avro_schema,
        )
        self._confluent_deserializer = ConfluentAvroDeserializer(
            self.registry_client,
        )

    def serialize(self, data: dict[str, Any]) -> bytes:
        result: bytes = self._confluent_serializer(
            data,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        return result

    def deserialize(self, data: bytes) -> dict[str, Any]:
        result: dict[str, Any] = self._confluent_deserializer(
            data,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        return result


class AvroSerializerNoRegistry:
    def __init__(self, schema: dict[str, Any]):
        self.schema = schema
        self.parsed_schema = fastavro.parse_schema(schema)

    def serialize(self, data: dict[str, Any]) -> bytes:
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, self.parsed_schema, data)
        return buffer.getvalue()

    def deserialize(self, data: bytes) -> dict[str, Any]:
        buffer = io.BytesIO(data)
        result = fastavro.schemaless_reader(buffer, self.parsed_schema)  # type: ignore[call-arg]
        return cast("dict[str, Any]", result)
