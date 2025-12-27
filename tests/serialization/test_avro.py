from khaos.models.schema import FieldSchema
from khaos.serialization.avro import (
    AvroSerializerNoRegistry,
    field_schema_to_avro_type,
    field_schemas_to_avro,
)


class TestFieldSchemaToAvroType:
    def test_string_type(self):
        field = FieldSchema(name="name", type="string")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "string"}

    def test_int_type(self):
        field = FieldSchema(name="count", type="int")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "long"}

    def test_float_type(self):
        field = FieldSchema(name="price", type="float")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "double"}

    def test_boolean_type(self):
        field = FieldSchema(name="active", type="boolean")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "boolean"}

    def test_faker_type(self):
        field = FieldSchema(name="email", type="faker", provider="email")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "string"}

    def test_uuid_type(self):
        field = FieldSchema(name="id", type="uuid")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "string", "logicalType": "uuid"}

    def test_timestamp_type(self):
        field = FieldSchema(name="created_at", type="timestamp")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "long", "logicalType": "timestamp-millis"}

    def test_enum_type_with_values(self):
        field = FieldSchema(name="status", type="enum", values=["pending", "done"])
        result = field_schema_to_avro_type(field)
        assert result["type"] == "enum"
        assert result["name"] == "StatusEnum"
        assert result["symbols"] == ["pending", "done"]

    def test_enum_type_without_values(self):
        field = FieldSchema(name="status", type="enum")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "string"}

    def test_object_type_with_fields(self):
        field = FieldSchema(
            name="address",
            type="object",
            fields=[
                FieldSchema(name="street", type="string"),
                FieldSchema(name="city", type="string"),
            ],
        )
        result = field_schema_to_avro_type(field)
        assert result["type"] == "record"
        assert result["name"] == "AddressRecord"
        assert len(result["fields"]) == 2

    def test_object_type_without_fields(self):
        field = FieldSchema(name="metadata", type="object")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "map", "values": "string"}

    def test_array_type_with_items(self):
        field = FieldSchema(
            name="tags",
            type="array",
            items=FieldSchema(name="item", type="string"),
        )
        result = field_schema_to_avro_type(field)
        assert result["type"] == "array"
        assert result["items"] == {"type": "string"}

    def test_array_type_without_items(self):
        field = FieldSchema(name="tags", type="array")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "array", "items": {"type": "string"}}

    def test_unknown_type_defaults_to_string(self):
        field = FieldSchema(name="unknown", type="unknown_type")
        result = field_schema_to_avro_type(field)
        assert result == {"type": "string"}

    def test_nested_object(self):
        field = FieldSchema(
            name="user",
            type="object",
            fields=[
                FieldSchema(
                    name="profile",
                    type="object",
                    fields=[FieldSchema(name="bio", type="string")],
                ),
            ],
        )
        result = field_schema_to_avro_type(field)
        assert result["type"] == "record"
        assert result["fields"][0]["type"]["type"] == "record"

    def test_array_of_objects(self):
        field = FieldSchema(
            name="items",
            type="array",
            items=FieldSchema(
                name="item",
                type="object",
                fields=[FieldSchema(name="name", type="string")],
            ),
        )
        result = field_schema_to_avro_type(field)
        assert result["type"] == "array"
        assert result["items"]["type"] == "record"


class TestFieldSchemasToAvro:
    def test_creates_record_schema(self):
        fields = [
            FieldSchema(name="id", type="int"),
            FieldSchema(name="name", type="string"),
        ]
        result = field_schemas_to_avro(fields, name="TestRecord")

        assert result["type"] == "record"
        assert result["name"] == "TestRecord"
        assert result["namespace"] == "khaos.generated"
        assert len(result["fields"]) == 2

    def test_empty_fields(self):
        result = field_schemas_to_avro([], name="EmptyRecord")

        assert result["type"] == "record"
        assert result["name"] == "EmptyRecord"
        assert result["fields"] == []

    def test_field_names_preserved(self):
        fields = [
            FieldSchema(name="user_id", type="int"),
            FieldSchema(name="email_address", type="string"),
        ]
        result = field_schemas_to_avro(fields, name="UserRecord")

        field_names = [f["name"] for f in result["fields"]]
        assert "user_id" in field_names
        assert "email_address" in field_names


class TestAvroSerializerNoRegistry:
    def test_serialize_simple_record(self):
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
            ],
        }
        serializer = AvroSerializerNoRegistry(schema)

        data = {"id": 123, "name": "test"}
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_deserialize_simple_record(self):
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
            ],
        }
        serializer = AvroSerializerNoRegistry(schema)

        data = {"id": 123, "name": "test"}
        serialized = serializer.serialize(data)
        result = serializer.deserialize(serialized)

        assert result == data

    def test_roundtrip_with_multiple_types(self):
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "int_field", "type": "long"},
                {"name": "string_field", "type": "string"},
                {"name": "float_field", "type": "double"},
                {"name": "bool_field", "type": "boolean"},
            ],
        }
        serializer = AvroSerializerNoRegistry(schema)

        data = {
            "int_field": 42,
            "string_field": "hello",
            "float_field": 3.14,
            "bool_field": True,
        }
        serialized = serializer.serialize(data)
        result = serializer.deserialize(serialized)

        assert result == data

    def test_roundtrip_with_nested_record(self):
        schema = {
            "type": "record",
            "name": "OuterRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {
                    "name": "inner",
                    "type": {
                        "type": "record",
                        "name": "InnerRecord",
                        "fields": [{"name": "value", "type": "string"}],
                    },
                },
            ],
        }
        serializer = AvroSerializerNoRegistry(schema)

        data = {"id": 1, "inner": {"value": "nested"}}
        serialized = serializer.serialize(data)
        result = serializer.deserialize(serialized)

        assert result == data

    def test_roundtrip_with_array(self):
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
            ],
        }
        serializer = AvroSerializerNoRegistry(schema)

        data = {"tags": ["a", "b", "c"]}
        serialized = serializer.serialize(data)
        result = serializer.deserialize(serialized)

        assert result == data

    def test_schema_stored(self):
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [{"name": "id", "type": "long"}],
        }
        serializer = AvroSerializerNoRegistry(schema)

        assert serializer.schema == schema

    def test_parsed_schema_created(self):
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [{"name": "id", "type": "long"}],
        }
        serializer = AvroSerializerNoRegistry(schema)

        assert serializer.parsed_schema is not None
