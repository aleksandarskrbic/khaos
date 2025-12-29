"""Serializer factory for creating message serializers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console

from khaos.models.schema import FieldSchema
from khaos.scenarios.scenario import Scenario, SchemaRegistryConfig, TopicConfig

if TYPE_CHECKING:
    from collections.abc import Callable

from khaos.serialization import (
    AvroSerializer,
    AvroSerializerNoRegistry,
    JsonSerializer,
    ProtobufSerializer,
    ProtobufSerializerNoRegistry,
    field_schemas_to_avro,
    field_schemas_to_protobuf,
)

console = Console()

SCHEMA_REGISTRY_URL = "http://localhost:8081"


class SerializerFactory:
    """Factory for creating message serializers based on topic configuration."""

    def __init__(
        self,
        scenarios: list[Scenario],
        is_schema_registry_running_fn: Callable[[], bool],
    ):
        """Initialize the serializer factory.

        Args:
            scenarios: List of scenarios (used for schema registry config)
            is_schema_registry_running_fn: Function to check if schema registry is running
        """
        self._scenarios = scenarios
        self._is_schema_registry_running = is_schema_registry_running_fn

    def needs_schema_registry(self, topics: list[TopicConfig]) -> bool:
        has_schema_format = any(
            topic.message_schema.data_format in ("avro", "protobuf") for topic in topics
        )
        has_registry_provider = any(topic.schema_provider == "registry" for topic in topics)
        has_config = any(s.schema_registry for s in self._scenarios)
        return (has_schema_format and has_config) or has_registry_provider

    def get_schema_registry_config(self) -> SchemaRegistryConfig | None:
        for scenario in self._scenarios:
            if scenario.schema_registry:
                return scenario.schema_registry
        if self._is_schema_registry_running():
            return SchemaRegistryConfig(url=SCHEMA_REGISTRY_URL)
        return None

    def _create_serializer(
        self,
        data_format: str,
        fields: list[FieldSchema] | None,
        topic_name: str,
        raw_avro_schema: dict | None = None,
    ):
        """Create a serializer based on format and fields."""
        message_name = topic_name.title().replace("-", "").replace("_", "") + "Record"

        if data_format == "avro":
            if not fields:
                console.print(
                    f"[yellow]Warning: Topic '{topic_name}' uses Avro but has no fields. "
                    "Falling back to JSON.[/yellow]"
                )
                return JsonSerializer()

            avro_schema = raw_avro_schema or field_schemas_to_avro(fields, name=message_name)
            schema_registry_config = self.get_schema_registry_config()

            if schema_registry_config:
                return AvroSerializer(
                    schema_registry_url=schema_registry_config.url,
                    schema=avro_schema,
                    topic=topic_name,
                )
            return AvroSerializerNoRegistry(schema=avro_schema)

        if data_format == "protobuf":
            if not fields:
                console.print(
                    f"[yellow]Warning: Topic '{topic_name}' uses Protobuf but has no fields. "
                    "Falling back to JSON.[/yellow]"
                )
                return JsonSerializer()

            _, message_class = field_schemas_to_protobuf(fields, name=message_name)
            schema_registry_config = self.get_schema_registry_config()

            if schema_registry_config:
                return ProtobufSerializer(
                    schema_registry_url=schema_registry_config.url,
                    message_class=message_class,
                    topic=topic_name,
                )
            return ProtobufSerializerNoRegistry(message_class=message_class)

        return JsonSerializer()

    def create_serializer_for_topic(self, topic: TopicConfig):
        return self._create_serializer(
            data_format=topic.message_schema.data_format,
            fields=topic.message_schema.fields,
            topic_name=topic.name,
        )

    def create_serializer_with_format(
        self,
        topic: TopicConfig,
        data_format: str,
        fields: list[FieldSchema] | None,
        raw_avro_schema: dict | None = None,
    ):
        return self._create_serializer(
            data_format=data_format,
            fields=fields,
            topic_name=topic.name,
            raw_avro_schema=raw_avro_schema,
        )
