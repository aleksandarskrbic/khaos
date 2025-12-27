"""Tests for schema converter (Avro/Protobuf to FieldSchema)."""

import pytest

from khaos.serialization.schema_converter import (
    SchemaRegistryProvider,
    avro_to_field_schemas,
    protobuf_to_field_schemas,
)


class TestAvroToFieldSchemas:
    """Tests for Avro schema to FieldSchema conversion."""

    def test_simple_types(self):
        """Test conversion of simple Avro types."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "count", "type": "long"},
                {"name": "price", "type": "double"},
                {"name": "rate", "type": "float"},
                {"name": "active", "type": "boolean"},
            ],
        }

        fields = avro_to_field_schemas(avro_schema)

        assert len(fields) == 6
        assert fields[0].name == "name"
        assert fields[0].type == "string"
        assert fields[1].name == "age"
        assert fields[1].type == "int"
        assert fields[2].name == "count"
        assert fields[2].type == "int"
        assert fields[3].name == "price"
        assert fields[3].type == "float"
        assert fields[4].name == "rate"
        assert fields[4].type == "float"
        assert fields[5].name == "active"
        assert fields[5].type == "boolean"

    def test_union_types(self):
        """Test conversion of nullable union types."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "optional_name", "type": ["null", "string"]},
                {"name": "optional_count", "type": ["null", "long"]},
            ],
        }

        fields = avro_to_field_schemas(avro_schema)

        assert len(fields) == 2
        assert fields[0].name == "optional_name"
        assert fields[0].type == "string"
        assert fields[1].name == "optional_count"
        assert fields[1].type == "int"

    def test_logical_types(self):
        """Test conversion of Avro logical types."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            ],
        }

        fields = avro_to_field_schemas(avro_schema)

        assert len(fields) == 2
        assert fields[0].name == "id"
        assert fields[0].type == "uuid"
        assert fields[1].name == "created_at"
        assert fields[1].type == "timestamp"

    def test_enum_type(self):
        """Test conversion of Avro enum type."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["PENDING", "ACTIVE", "COMPLETED"],
                    },
                },
            ],
        }

        fields = avro_to_field_schemas(avro_schema)

        assert len(fields) == 1
        assert fields[0].name == "status"
        assert fields[0].type == "enum"
        assert fields[0].values == ["PENDING", "ACTIVE", "COMPLETED"]

    def test_array_type(self):
        """Test conversion of Avro array type."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {
                    "name": "tags",
                    "type": {"type": "array", "items": "string"},
                },
            ],
        }

        fields = avro_to_field_schemas(avro_schema)

        assert len(fields) == 1
        assert fields[0].name == "tags"
        assert fields[0].type == "array"
        assert fields[0].items is not None
        assert fields[0].items.type == "string"

    def test_nested_record(self):
        """Test conversion of nested Avro record."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {
                    "name": "address",
                    "type": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "city", "type": "string"},
                        ],
                    },
                },
            ],
        }

        fields = avro_to_field_schemas(avro_schema)

        assert len(fields) == 1
        assert fields[0].name == "address"
        assert fields[0].type == "object"
        assert fields[0].fields is not None
        assert len(fields[0].fields) == 2
        assert fields[0].fields[0].name == "street"
        assert fields[0].fields[1].name == "city"

    def test_invalid_schema_type(self):
        """Test that non-record schema raises error."""
        avro_schema = {"type": "string"}

        with pytest.raises(ValueError, match="Expected Avro record type"):
            avro_to_field_schemas(avro_schema)


class TestProtobufToFieldSchemas:
    """Tests for Protobuf schema to FieldSchema conversion."""

    def test_simple_types(self):
        """Test conversion of simple Protobuf types."""
        proto_schema = """
        syntax = "proto3";
        message TestRecord {
            string name = 1;
            int32 age = 2;
            int64 count = 3;
            double price = 4;
            float rate = 5;
            bool active = 6;
        }
        """

        fields = protobuf_to_field_schemas(proto_schema)

        assert len(fields) == 6
        assert fields[0].name == "name"
        assert fields[0].type == "string"
        assert fields[1].name == "age"
        assert fields[1].type == "int"
        assert fields[2].name == "count"
        assert fields[2].type == "int"
        assert fields[3].name == "price"
        assert fields[3].type == "float"
        assert fields[4].name == "rate"
        assert fields[4].type == "float"
        assert fields[5].name == "active"
        assert fields[5].type == "boolean"

    def test_repeated_field(self):
        """Test conversion of repeated Protobuf field."""
        proto_schema = """
        syntax = "proto3";
        message TestRecord {
            repeated string tags = 1;
        }
        """

        fields = protobuf_to_field_schemas(proto_schema)

        assert len(fields) == 1
        assert fields[0].name == "tags"
        assert fields[0].type == "array"
        assert fields[0].items is not None
        assert fields[0].items.type == "string"

    def test_bytes_field(self):
        """Test conversion of bytes field."""
        proto_schema = """
        syntax = "proto3";
        message TestRecord {
            bytes data = 1;
        }
        """

        fields = protobuf_to_field_schemas(proto_schema)

        assert len(fields) == 1
        assert fields[0].name == "data"
        assert fields[0].type == "string"  # bytes -> string

    def test_empty_message(self):
        """Test that empty or missing message returns empty list."""
        proto_schema = 'syntax = "proto3";'

        fields = protobuf_to_field_schemas(proto_schema)

        assert fields == []

    def test_various_int_types(self):
        """Test conversion of various Protobuf integer types."""
        proto_schema = """
        syntax = "proto3";
        message TestRecord {
            sint32 signed32 = 1;
            sint64 signed64 = 2;
            uint32 unsigned32 = 3;
            uint64 unsigned64 = 4;
            fixed32 fixed32 = 5;
            fixed64 fixed64 = 6;
        }
        """

        fields = protobuf_to_field_schemas(proto_schema)

        assert len(fields) == 6
        for field in fields:
            assert field.type == "int"


class TestSchemaRegistryProvider:
    """Tests for SchemaRegistryProvider class (unit tests only)."""

    def test_initialization(self):
        """Test provider initialization."""
        provider = SchemaRegistryProvider("http://localhost:8081")

        assert provider.registry_url == "http://localhost:8081"
        assert provider._client is None  # Lazy initialization
        assert provider._cache == {}

    def test_lazy_client_property(self):
        """Test that client property creates client lazily."""
        provider = SchemaRegistryProvider("http://localhost:8081")

        assert provider._client is None

        # Accessing client property should NOT fail even if registry is down
        # (client creation is just config, not connection)
        _ = provider.client
        assert provider._client is not None

    def test_cache_operations(self):
        """Test cache operations without real registry."""
        provider = SchemaRegistryProvider("http://localhost:8081")

        # Initially not cached
        assert not provider.is_cached("test-subject")

        # Manually add to cache for testing
        provider._cache["test-subject"] = ("avro", [])
        assert provider.is_cached("test-subject")

        # Clear cache
        provider.clear_cache()
        assert not provider.is_cached("test-subject")
        assert provider._cache == {}
