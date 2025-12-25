from khaos.models.schema import FieldSchema
from khaos.serialization.protobuf import (
    FIELD_TYPE_MAP,
    ProtobufSerializerNoRegistry,
    dict_to_protobuf_message,
    field_schemas_to_protobuf,
    protobuf_message_to_dict,
)


class TestFieldTypeMapping:
    def test_all_basic_types_mapped(self):
        expected_types = ["string", "int", "float", "boolean", "uuid", "timestamp", "faker"]
        for field_type in expected_types:
            assert field_type in FIELD_TYPE_MAP


class TestFieldSchemasToProtobuf:
    def test_simple_message_generation(self):
        fields = [
            FieldSchema(name="id", type="string"),
            FieldSchema(name="count", type="int"),
            FieldSchema(name="active", type="boolean"),
        ]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "SimpleMessage")

        assert message_class is not None
        field_names = [f.name for f in message_class.DESCRIPTOR.fields]
        assert "id" in field_names
        assert "count" in field_names
        assert "active" in field_names

    def test_uuid_field_generation(self):
        fields = [FieldSchema(name="user_id", type="uuid")]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "UuidMessage")

        field_names = [f.name for f in message_class.DESCRIPTOR.fields]
        assert "user_id" in field_names

    def test_timestamp_field_generation(self):
        fields = [FieldSchema(name="created_at", type="timestamp")]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "TimestampMessage")

        field_names = [f.name for f in message_class.DESCRIPTOR.fields]
        assert "created_at" in field_names

    def test_enum_field_generation(self):
        fields = [
            FieldSchema(name="status", type="enum", values=["PENDING", "COMPLETED"]),
        ]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "EnumMessage")

        # Check enum exists in nested types or message descriptor
        status_field = message_class.DESCRIPTOR.fields_by_name.get("status")
        assert status_field is not None
        assert status_field.enum_type is not None
        enum_values = [v.name for v in status_field.enum_type.values]
        assert "PENDING" in enum_values
        assert "COMPLETED" in enum_values

    def test_nested_object_generation(self):
        fields = [
            FieldSchema(
                name="address",
                type="object",
                fields=[
                    FieldSchema(name="street", type="string"),
                    FieldSchema(name="city", type="string"),
                ],
            ),
        ]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "NestedMessage")

        address_field = message_class.DESCRIPTOR.fields_by_name.get("address")
        assert address_field is not None
        assert address_field.message_type is not None
        nested_fields = [f.name for f in address_field.message_type.fields]
        assert "street" in nested_fields
        assert "city" in nested_fields

    def test_array_field_generation(self):
        fields = [
            FieldSchema(
                name="tags",
                type="array",
                items=FieldSchema(name="tag", type="string"),
            ),
        ]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "ArrayMessage")

        tags_field = message_class.DESCRIPTOR.fields_by_name.get("tags")
        assert tags_field is not None
        # Check that it's a repeated field
        assert tags_field.is_repeated

    def test_array_of_objects_generation(self):
        fields = [
            FieldSchema(
                name="items",
                type="array",
                items=FieldSchema(
                    name="item",
                    type="object",
                    fields=[
                        FieldSchema(name="name", type="string"),
                        FieldSchema(name="quantity", type="int"),
                    ],
                ),
            ),
        ]
        _file_proto, message_class = field_schemas_to_protobuf(fields, "ArrayObjectMessage")

        items_field = message_class.DESCRIPTOR.fields_by_name.get("items")
        assert items_field is not None
        assert items_field.is_repeated
        assert items_field.message_type is not None


class TestDictToProtobufMessage:
    def test_simple_conversion(self):
        fields = [
            FieldSchema(name="name", type="string"),
            FieldSchema(name="age", type="int"),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "Person")

        data = {"name": "Alice", "age": 30}
        message = dict_to_protobuf_message(data, message_class)

        assert message.name == "Alice"
        assert message.age == 30

    def test_enum_string_to_int_conversion(self):
        fields = [
            FieldSchema(name="status", type="enum", values=["PENDING", "COMPLETED"]),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "EnumConversion")

        # String value should be converted to int
        data = {"status": "COMPLETED"}
        message = dict_to_protobuf_message(data, message_class)

        assert message.status == 1  # COMPLETED is index 1

    def test_array_conversion(self):
        fields = [
            FieldSchema(
                name="numbers",
                type="array",
                items=FieldSchema(name="n", type="int"),
            ),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "Numbers")

        data = {"numbers": [1, 2, 3, 4, 5]}
        message = dict_to_protobuf_message(data, message_class)

        assert list(message.numbers) == [1, 2, 3, 4, 5]

    def test_nested_object_conversion(self):
        fields = [
            FieldSchema(
                name="address",
                type="object",
                fields=[
                    FieldSchema(name="street", type="string"),
                    FieldSchema(name="city", type="string"),
                ],
            ),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "NestedConversion")

        data = {"address": {"street": "123 Main St", "city": "Boston"}}
        message = dict_to_protobuf_message(data, message_class)

        assert message.address.street == "123 Main St"
        assert message.address.city == "Boston"


class TestProtobufMessageToDict:
    def test_simple_message_to_dict(self):
        fields = [
            FieldSchema(name="name", type="string"),
            FieldSchema(name="count", type="int"),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "SimpleToDict")

        message = message_class()
        message.name = "test"
        message.count = 42

        result = protobuf_message_to_dict(message)

        assert result["name"] == "test"
        # int64 values are converted to strings by MessageToDict
        assert int(result["count"]) == 42


class TestProtobufSerializerNoRegistry:
    def test_serialize_deserialize_roundtrip(self):
        fields = [
            FieldSchema(name="id", type="string"),
            FieldSchema(name="value", type="int"),
            FieldSchema(name="enabled", type="boolean"),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "TestMessage")

        serializer = ProtobufSerializerNoRegistry(message_class)

        original = {"id": "test-123", "value": 42, "enabled": True}
        serialized = serializer.serialize(original)

        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        deserialized = serializer.deserialize(serialized)

        assert deserialized["id"] == original["id"]
        # int64 values are converted to strings by MessageToDict
        assert int(deserialized["value"]) == original["value"]
        assert deserialized["enabled"] == original["enabled"]

    def test_roundtrip_with_enum(self):
        fields = [
            FieldSchema(name="status", type="enum", values=["PENDING", "ACTIVE", "COMPLETED"]),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "EnumRoundtrip")

        serializer = ProtobufSerializerNoRegistry(message_class)

        # Use string value - should be converted to int
        original = {"status": "ACTIVE"}
        serialized = serializer.serialize(original)
        deserialized = serializer.deserialize(serialized)

        # Deserialized may be int or string depending on MessageToDict settings
        assert deserialized["status"] in (1, "ACTIVE")

    def test_roundtrip_with_uuid_and_timestamp(self):
        fields = [
            FieldSchema(name="id", type="uuid"),
            FieldSchema(name="created_at", type="timestamp"),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "UuidTimestamp")

        serializer = ProtobufSerializerNoRegistry(message_class)

        original = {"id": "550e8400-e29b-41d4-a716-446655440000", "created_at": 1703001234567}
        serialized = serializer.serialize(original)
        deserialized = serializer.deserialize(serialized)

        assert deserialized["id"] == original["id"]
        # Timestamp is stored as string in protobuf int64 -> MessageToDict converts to string
        assert int(deserialized["created_at"]) == original["created_at"]

    def test_roundtrip_with_nested_object(self):
        fields = [
            FieldSchema(
                name="user",
                type="object",
                fields=[
                    FieldSchema(name="name", type="string"),
                    FieldSchema(name="email", type="string"),
                ],
            ),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "NestedRoundtrip")

        serializer = ProtobufSerializerNoRegistry(message_class)

        original = {"user": {"name": "Alice", "email": "alice@example.com"}}
        serialized = serializer.serialize(original)
        deserialized = serializer.deserialize(serialized)

        assert deserialized["user"]["name"] == original["user"]["name"]
        assert deserialized["user"]["email"] == original["user"]["email"]

    def test_roundtrip_with_array(self):
        fields = [
            FieldSchema(
                name="scores",
                type="array",
                items=FieldSchema(name="score", type="int"),
            ),
        ]
        _, message_class = field_schemas_to_protobuf(fields, "ArrayRoundtrip")

        serializer = ProtobufSerializerNoRegistry(message_class)

        original = {"scores": [100, 200, 300]}
        serialized = serializer.serialize(original)
        deserialized = serializer.deserialize(serialized)

        # int64 values are converted to strings by MessageToDict
        assert [int(s) for s in deserialized["scores"]] == original["scores"]


class TestValidatorIntegration:
    def test_protobuf_is_valid_data_format(self):
        from khaos.validators.scenario import VALID_DATA_FORMATS

        assert "protobuf" in VALID_DATA_FORMATS
