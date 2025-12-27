"""Integration tests for Schema Registry provider."""

import json

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from khaos.serialization import SchemaRegistryProvider


@pytest.mark.integration
class TestSchemaRegistryProvider:
    """Integration tests for SchemaRegistryProvider with real Schema Registry."""

    def test_register_and_fetch_avro_schema(
        self, schema_registry_url, avro_schema, kafka_container, schema_registry_container
    ):
        """Test registering an Avro schema and fetching via provider."""
        subject = "test-avro-value"

        # Register schema directly via client
        client = SchemaRegistryClient({"url": schema_registry_url})
        schema = Schema(json.dumps(avro_schema), "AVRO")
        client.register_schema(subject, schema)

        # Fetch via provider
        provider = SchemaRegistryProvider(schema_registry_url)
        data_format, fields = provider.get_field_schemas(subject)

        assert data_format == "avro"
        assert len(fields) == 5

        # Verify field names and types
        field_map = {f.name: f for f in fields}
        assert field_map["id"].type == "string"
        assert field_map["count"].type == "int"
        assert field_map["amount"].type == "float"
        assert field_map["active"].type == "boolean"
        assert field_map["status"].type == "enum"
        assert field_map["status"].values == ["PENDING", "ACTIVE"]

    def test_complex_avro_schema(
        self, schema_registry_url, complex_avro_schema, kafka_container, schema_registry_container
    ):
        """Test complex Avro schema with nested types."""
        subject = "test-complex-avro-value"

        # Register schema
        client = SchemaRegistryClient({"url": schema_registry_url})
        schema = Schema(json.dumps(complex_avro_schema), "AVRO")
        client.register_schema(subject, schema)

        # Fetch via provider
        provider = SchemaRegistryProvider(schema_registry_url)
        data_format, fields = provider.get_field_schemas(subject)

        assert data_format == "avro"
        assert len(fields) == 5

        field_map = {f.name: f for f in fields}

        # Check UUID logical type
        assert field_map["order_id"].type == "uuid"

        # Check timestamp logical type
        assert field_map["created_at"].type == "timestamp"

        # Check array type
        assert field_map["tags"].type == "array"
        assert field_map["tags"].items is not None
        assert field_map["tags"].items.type == "string"

        # Check nested record
        assert field_map["customer"].type == "object"
        assert field_map["customer"].fields is not None
        nested_fields = {f.name: f for f in field_map["customer"].fields}
        assert nested_fields["name"].type == "string"
        assert nested_fields["email"].type == "string"  # Union with null

    def test_schema_caching(
        self, schema_registry_url, avro_schema, kafka_container, schema_registry_container
    ):
        """Test that schemas are cached."""
        subject = "test-cache-value"

        # Register schema
        client = SchemaRegistryClient({"url": schema_registry_url})
        schema = Schema(json.dumps(avro_schema), "AVRO")
        client.register_schema(subject, schema)

        # Fetch twice
        provider = SchemaRegistryProvider(schema_registry_url)

        assert not provider.is_cached(subject)
        _, fields1 = provider.get_field_schemas(subject)
        assert provider.is_cached(subject)

        _, fields2 = provider.get_field_schemas(subject)

        # Same objects due to caching
        assert fields1 is fields2

    def test_clear_cache(
        self, schema_registry_url, avro_schema, kafka_container, schema_registry_container
    ):
        """Test cache clearing."""
        subject = "test-clear-cache-value"

        # Register schema
        client = SchemaRegistryClient({"url": schema_registry_url})
        schema = Schema(json.dumps(avro_schema), "AVRO")
        client.register_schema(subject, schema)

        provider = SchemaRegistryProvider(schema_registry_url)
        provider.get_field_schemas(subject)
        assert provider.is_cached(subject)

        provider.clear_cache()
        assert not provider.is_cached(subject)

    def test_schema_not_found(
        self, schema_registry_url, kafka_container, schema_registry_container
    ):
        """Test error handling for non-existent schema."""
        provider = SchemaRegistryProvider(schema_registry_url)

        with pytest.raises(ValueError, match="Schema not found"):
            provider.get_field_schemas("non-existent-subject")

    def test_lazy_client_initialization(self, schema_registry_url):
        """Test that client is lazily initialized."""
        provider = SchemaRegistryProvider(schema_registry_url)

        # Client should not be created yet
        assert provider._client is None

        # Accessing client property should create it
        _ = provider.client
        assert provider._client is not None
