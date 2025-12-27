from unittest.mock import Mock

from khaos.generators.schema import SchemaPayloadGenerator
from khaos.models.schema import FieldSchema
from khaos.serialization.json import JsonSerializer


class TestSchemaPayloadGenerator:
    def test_generate_returns_bytes(self):
        fields = [
            FieldSchema(name="id", type="int", min=1, max=100),
            FieldSchema(name="name", type="string", min_length=5, max_length=10),
        ]
        generator = SchemaPayloadGenerator(fields)

        result = generator.generate()

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_generate_dict_returns_dict(self):
        fields = [
            FieldSchema(name="id", type="int", min=1, max=100),
            FieldSchema(name="name", type="string", min_length=5, max_length=10),
        ]
        generator = SchemaPayloadGenerator(fields)

        result = generator.generate_dict()

        assert isinstance(result, dict)
        assert "id" in result
        assert "name" in result
        assert isinstance(result["id"], int)
        assert isinstance(result["name"], str)

    def test_generate_dict_respects_field_constraints(self):
        fields = [
            FieldSchema(name="value", type="int", min=10, max=20),
        ]
        generator = SchemaPayloadGenerator(fields)

        for _ in range(100):
            result = generator.generate_dict()
            assert 10 <= result["value"] <= 20

    def test_uses_default_json_serializer(self):
        fields = [FieldSchema(name="test", type="string")]
        generator = SchemaPayloadGenerator(fields)

        assert isinstance(generator.serializer, JsonSerializer)

    def test_uses_custom_serializer(self):
        fields = [FieldSchema(name="test", type="string")]
        mock_serializer = Mock()
        mock_serializer.serialize.return_value = b"custom"

        generator = SchemaPayloadGenerator(fields, serializer=mock_serializer)
        result = generator.generate()

        mock_serializer.serialize.assert_called_once()
        assert result == b"custom"

    def test_multiple_field_types(self):
        fields = [
            FieldSchema(name="id", type="int", min=1, max=100),
            FieldSchema(name="name", type="string", min_length=1, max_length=50),
            FieldSchema(name="score", type="float", min=0.0, max=1.0),
            FieldSchema(name="active", type="boolean"),
            FieldSchema(name="status", type="enum", values=["pending", "done"]),
        ]
        generator = SchemaPayloadGenerator(fields)

        result = generator.generate_dict()

        assert isinstance(result["id"], int)
        assert isinstance(result["name"], str)
        assert isinstance(result["score"], float)
        assert isinstance(result["active"], bool)
        assert result["status"] in ["pending", "done"]

    def test_empty_fields_list(self):
        generator = SchemaPayloadGenerator(fields=[])

        result = generator.generate_dict()

        assert result == {}

    def test_field_generators_initialized_correctly(self):
        fields = [
            FieldSchema(name="a", type="int"),
            FieldSchema(name="b", type="string"),
        ]
        generator = SchemaPayloadGenerator(fields)

        assert len(generator.field_generators) == 2
        assert generator.field_generators[0][0] == "a"
        assert generator.field_generators[1][0] == "b"

    def test_uuid_field(self):
        fields = [FieldSchema(name="id", type="uuid")]
        generator = SchemaPayloadGenerator(fields)

        result = generator.generate_dict()

        assert "id" in result
        assert isinstance(result["id"], str)
        assert len(result["id"]) == 36  # UUID format

    def test_timestamp_field(self):
        fields = [FieldSchema(name="created_at", type="timestamp")]
        generator = SchemaPayloadGenerator(fields)

        result = generator.generate_dict()

        assert "created_at" in result
        assert isinstance(result["created_at"], int)
