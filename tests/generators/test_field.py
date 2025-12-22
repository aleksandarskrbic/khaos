"""Tests for field value generators."""

import re
import uuid
from datetime import datetime

import pytest

from khaos.generators.field import (
    ArrayFieldGenerator,
    BooleanFieldGenerator,
    EnumFieldGenerator,
    FakerFieldGenerator,
    FloatFieldGenerator,
    IntFieldGenerator,
    ObjectFieldGenerator,
    StringFieldGenerator,
    TimestampFieldGenerator,
    UuidFieldGenerator,
    create_field_generator,
)
from khaos.models.schema import FieldSchema


class TestStringFieldGenerator:
    """Tests for string generator."""

    def test_length_within_range(self):
        """Generated string length is within min/max."""
        gen = StringFieldGenerator(min_length=5, max_length=10)
        for _ in range(100):
            value = gen.generate()
            assert 5 <= len(value) <= 10

    def test_exact_length(self):
        """Exact length when min equals max."""
        gen = StringFieldGenerator(min_length=8, max_length=8)
        for _ in range(50):
            assert len(gen.generate()) == 8

    def test_lowercase_only(self):
        """Generated strings are lowercase letters only."""
        gen = StringFieldGenerator(min_length=20, max_length=20)
        value = gen.generate()
        assert value.islower()
        assert value.isalpha()

    def test_cardinality_limits_unique_values(self):
        """Cardinality limits the number of unique values."""
        gen = StringFieldGenerator(cardinality=5)
        values = {gen.generate() for _ in range(100)}
        assert len(values) == 5

    def test_cardinality_cycles_through_values(self):
        """Values cycle after cardinality is reached."""
        gen = StringFieldGenerator(cardinality=3)
        first_cycle = [gen.generate() for _ in range(3)]
        second_cycle = [gen.generate() for _ in range(3)]
        assert first_cycle == second_cycle


class TestIntFieldGenerator:
    """Tests for integer generator."""

    def test_value_within_range(self):
        """Generated int is within min/max."""
        gen = IntFieldGenerator(min_val=10, max_val=20)
        for _ in range(100):
            value = gen.generate()
            assert 10 <= value <= 20

    def test_exact_value(self):
        """Exact value when min equals max."""
        gen = IntFieldGenerator(min_val=42, max_val=42)
        for _ in range(50):
            assert gen.generate() == 42

    def test_cardinality_limits_unique_values(self):
        """Cardinality limits unique integer values."""
        gen = IntFieldGenerator(min_val=0, max_val=1000, cardinality=10)
        values = {gen.generate() for _ in range(200)}
        assert len(values) == 10

    def test_negative_range(self):
        """Works with negative ranges."""
        gen = IntFieldGenerator(min_val=-100, max_val=-50)
        for _ in range(50):
            value = gen.generate()
            assert -100 <= value <= -50


class TestFloatFieldGenerator:
    """Tests for float generator."""

    def test_value_within_range(self):
        """Generated float is within min/max."""
        gen = FloatFieldGenerator(min_val=1.5, max_val=5.5)
        for _ in range(100):
            value = gen.generate()
            assert 1.5 <= value <= 5.5

    def test_precision(self):
        """Values are rounded to 2 decimal places."""
        gen = FloatFieldGenerator(min_val=0.0, max_val=100.0)
        for _ in range(50):
            value = gen.generate()
            assert value == round(value, 2)


class TestBooleanFieldGenerator:
    """Tests for boolean generator."""

    def test_returns_bool(self):
        """Always returns a boolean."""
        gen = BooleanFieldGenerator()
        for _ in range(50):
            assert isinstance(gen.generate(), bool)

    def test_both_values_generated(self):
        """Both True and False are generated over many iterations."""
        gen = BooleanFieldGenerator()
        values = {gen.generate() for _ in range(100)}
        assert True in values
        assert False in values


class TestUuidFieldGenerator:
    """Tests for UUID generator."""

    def test_valid_uuid_format(self):
        """Generated values are valid UUIDs."""
        gen = UuidFieldGenerator()
        for _ in range(50):
            value = gen.generate()
            uuid.UUID(value)  # Raises if invalid

    def test_unique_values(self):
        """Each generated UUID is unique."""
        gen = UuidFieldGenerator()
        values = [gen.generate() for _ in range(100)]
        assert len(set(values)) == 100


class TestTimestampFieldGenerator:
    """Tests for timestamp generator."""

    def test_iso_format(self):
        """Generated timestamps are ISO 8601 format."""
        gen = TimestampFieldGenerator()
        value = gen.generate()
        # Should parse without error
        datetime.fromisoformat(value.replace("Z", "+00:00"))

    def test_contains_date_and_time(self):
        """Timestamp contains both date and time components."""
        gen = TimestampFieldGenerator()
        value = gen.generate()
        assert "T" in value  # ISO separator


class TestEnumFieldGenerator:
    """Tests for enum generator."""

    def test_values_from_list(self):
        """Only values from the provided list are generated."""
        values = ["red", "green", "blue"]
        gen = EnumFieldGenerator(values)
        for _ in range(100):
            assert gen.generate() in values

    def test_all_values_generated(self):
        """All enum values are eventually generated."""
        values = ["a", "b", "c"]
        gen = EnumFieldGenerator(values)
        generated = {gen.generate() for _ in range(100)}
        assert generated == set(values)

    def test_weighted_enum_simulation(self):
        """Duplicates in list affect distribution (75% success rate example)."""
        values = ["success", "success", "success", "failed"]
        gen = EnumFieldGenerator(values)
        counts = {"success": 0, "failed": 0}
        for _ in range(1000):
            counts[gen.generate()] += 1
        # Success should be roughly 3x more common
        assert counts["success"] > counts["failed"] * 2


class TestObjectFieldGenerator:
    """Tests for nested object generator."""

    def test_generates_dict(self):
        """Generates a dictionary."""
        gen = ObjectFieldGenerator([("name", StringFieldGenerator())])
        value = gen.generate()
        assert isinstance(value, dict)

    def test_includes_all_fields(self):
        """Generated object includes all fields."""
        gen = ObjectFieldGenerator(
            [
                ("name", StringFieldGenerator()),
                ("age", IntFieldGenerator()),
                ("active", BooleanFieldGenerator()),
            ]
        )
        value = gen.generate()
        assert set(value.keys()) == {"name", "age", "active"}

    def test_nested_types_correct(self):
        """Each field has correct type."""
        gen = ObjectFieldGenerator(
            [
                ("s", StringFieldGenerator()),
                ("i", IntFieldGenerator()),
                ("b", BooleanFieldGenerator()),
            ]
        )
        value = gen.generate()
        assert isinstance(value["s"], str)
        assert isinstance(value["i"], int)
        assert isinstance(value["b"], bool)


class TestArrayFieldGenerator:
    """Tests for array generator."""

    def test_length_within_range(self):
        """Array length is within min/max items."""
        gen = ArrayFieldGenerator(IntFieldGenerator(), min_items=3, max_items=7)
        for _ in range(50):
            value = gen.generate()
            assert 3 <= len(value) <= 7

    def test_items_have_correct_type(self):
        """All items have the correct type."""
        gen = ArrayFieldGenerator(UuidFieldGenerator(), min_items=5, max_items=5)
        value = gen.generate()
        for item in value:
            uuid.UUID(item)  # Validates UUID format

    def test_exact_length(self):
        """Exact length when min equals max."""
        gen = ArrayFieldGenerator(IntFieldGenerator(), min_items=4, max_items=4)
        for _ in range(50):
            assert len(gen.generate()) == 4


class TestFakerFieldGenerator:
    """Tests for Faker integration."""

    def test_name_provider(self):
        """Name provider generates realistic names."""
        gen = FakerFieldGenerator(provider="name")
        value = gen.generate()
        assert isinstance(value, str)
        assert len(value) > 0

    def test_email_provider(self):
        """Email provider generates valid emails."""
        gen = FakerFieldGenerator(provider="email")
        value = gen.generate()
        assert "@" in value
        assert "." in value

    def test_date_provider_returns_string(self):
        """Date provider returns ISO string, not date object."""
        gen = FakerFieldGenerator(provider="date_this_year")
        value = gen.generate()
        assert isinstance(value, str)
        assert re.match(r"\d{4}-\d{2}-\d{2}", value)

    def test_datetime_provider_returns_string(self):
        """Datetime provider returns ISO string."""
        gen = FakerFieldGenerator(provider="date_time_this_year")
        value = gen.generate()
        assert isinstance(value, str)
        assert "T" in value or "-" in value

    def test_invalid_provider_raises(self):
        """Unknown provider raises ValueError."""
        with pytest.raises(ValueError, match="Unknown faker provider"):
            FakerFieldGenerator(provider="nonexistent_provider_xyz")

    def test_locale_support(self):
        """Locale parameter is supported."""
        gen = FakerFieldGenerator(provider="name", locale="de_DE")
        value = gen.generate()
        assert isinstance(value, str)


class TestCreateFieldGenerator:
    """Tests for factory function."""

    def test_creates_string_generator(self):
        """Creates StringFieldGenerator for type 'string'."""
        schema = FieldSchema(name="test", type="string", min_length=5, max_length=10)
        gen = create_field_generator(schema)
        assert isinstance(gen, StringFieldGenerator)
        value = gen.generate()
        assert 5 <= len(value) <= 10

    def test_creates_int_generator(self):
        """Creates IntFieldGenerator for type 'int'."""
        schema = FieldSchema(name="test", type="int", min=10, max=20)
        gen = create_field_generator(schema)
        assert isinstance(gen, IntFieldGenerator)
        value = gen.generate()
        assert 10 <= value <= 20

    def test_creates_float_generator(self):
        """Creates FloatFieldGenerator for type 'float'."""
        schema = FieldSchema(name="test", type="float", min=1.5, max=5.5)
        gen = create_field_generator(schema)
        assert isinstance(gen, FloatFieldGenerator)

    def test_creates_boolean_generator(self):
        """Creates BooleanFieldGenerator for type 'boolean'."""
        schema = FieldSchema(name="test", type="boolean")
        gen = create_field_generator(schema)
        assert isinstance(gen, BooleanFieldGenerator)

    def test_creates_uuid_generator(self):
        """Creates UuidFieldGenerator for type 'uuid'."""
        schema = FieldSchema(name="test", type="uuid")
        gen = create_field_generator(schema)
        assert isinstance(gen, UuidFieldGenerator)

    def test_creates_timestamp_generator(self):
        """Creates TimestampFieldGenerator for type 'timestamp'."""
        schema = FieldSchema(name="test", type="timestamp")
        gen = create_field_generator(schema)
        assert isinstance(gen, TimestampFieldGenerator)

    def test_creates_enum_generator(self):
        """Creates EnumFieldGenerator for type 'enum'."""
        schema = FieldSchema(name="test", type="enum", values=["a", "b", "c"])
        gen = create_field_generator(schema)
        assert isinstance(gen, EnumFieldGenerator)
        assert gen.generate() in ["a", "b", "c"]

    def test_enum_requires_values(self):
        """Enum type without values raises error."""
        schema = FieldSchema(name="test", type="enum")
        with pytest.raises(ValueError, match="requires 'values'"):
            create_field_generator(schema)

    def test_creates_object_generator(self):
        """Creates ObjectFieldGenerator for type 'object'."""
        inner_field = FieldSchema(name="inner", type="string")
        schema = FieldSchema(name="test", type="object", fields=[inner_field])
        gen = create_field_generator(schema)
        assert isinstance(gen, ObjectFieldGenerator)
        value = gen.generate()
        assert "inner" in value

    def test_object_requires_fields(self):
        """Object type without fields raises error."""
        schema = FieldSchema(name="test", type="object")
        with pytest.raises(ValueError, match="requires 'fields'"):
            create_field_generator(schema)

    def test_creates_array_generator(self):
        """Creates ArrayFieldGenerator for type 'array'."""
        item_schema = FieldSchema(name="item", type="int")
        schema = FieldSchema(name="test", type="array", items=item_schema, min_items=2, max_items=5)
        gen = create_field_generator(schema)
        assert isinstance(gen, ArrayFieldGenerator)
        value = gen.generate()
        assert 2 <= len(value) <= 5

    def test_array_requires_items(self):
        """Array type without items raises error."""
        schema = FieldSchema(name="test", type="array")
        with pytest.raises(ValueError, match="requires 'items'"):
            create_field_generator(schema)

    def test_creates_faker_generator(self):
        """Creates FakerFieldGenerator for type 'faker'."""
        schema = FieldSchema(name="test", type="faker", provider="email")
        gen = create_field_generator(schema)
        assert isinstance(gen, FakerFieldGenerator)
        assert "@" in gen.generate()

    def test_faker_requires_provider(self):
        """Faker type without provider raises error."""
        schema = FieldSchema(name="test", type="faker")
        with pytest.raises(ValueError, match="requires 'provider'"):
            create_field_generator(schema)

    def test_unknown_type_raises(self):
        """Unknown field type raises error."""
        schema = FieldSchema(name="test", type="unknown_type")
        with pytest.raises(ValueError, match="Unknown field type"):
            create_field_generator(schema)
