import re
import uuid
from datetime import UTC, datetime

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
    def test_length_within_range(self):
        gen = StringFieldGenerator(min_length=5, max_length=10)
        for _ in range(100):
            value = gen.generate()
            assert 5 <= len(value) <= 10

    def test_exact_length(self):
        gen = StringFieldGenerator(min_length=8, max_length=8)
        for _ in range(50):
            assert len(gen.generate()) == 8

    def test_lowercase_only(self):
        gen = StringFieldGenerator(min_length=20, max_length=20)
        value = gen.generate()
        assert value.islower()
        assert value.isalpha()

    def test_cardinality_limits_unique_values(self):
        gen = StringFieldGenerator(cardinality=5)
        values = {gen.generate() for _ in range(100)}
        assert len(values) == 5

    def test_cardinality_cycles_through_values(self):
        gen = StringFieldGenerator(cardinality=3)
        first_cycle = [gen.generate() for _ in range(3)]
        second_cycle = [gen.generate() for _ in range(3)]
        assert first_cycle == second_cycle


class TestIntFieldGenerator:
    def test_value_within_range(self):
        gen = IntFieldGenerator(min_val=10, max_val=20)
        for _ in range(100):
            value = gen.generate()
            assert 10 <= value <= 20

    def test_exact_value(self):
        gen = IntFieldGenerator(min_val=42, max_val=42)
        for _ in range(50):
            assert gen.generate() == 42

    def test_cardinality_limits_unique_values(self):
        gen = IntFieldGenerator(min_val=0, max_val=1000, cardinality=10)
        values = {gen.generate() for _ in range(200)}
        assert len(values) == 10

    def test_negative_range(self):
        gen = IntFieldGenerator(min_val=-100, max_val=-50)
        for _ in range(50):
            value = gen.generate()
            assert -100 <= value <= -50


class TestFloatFieldGenerator:
    def test_value_within_range(self):
        gen = FloatFieldGenerator(min_val=1.5, max_val=5.5)
        for _ in range(100):
            value = gen.generate()
            assert 1.5 <= value <= 5.5

    def test_precision(self):
        gen = FloatFieldGenerator(min_val=0.0, max_val=100.0)
        for _ in range(50):
            value = gen.generate()
            assert value == round(value, 2)


class TestBooleanFieldGenerator:
    def test_returns_bool(self):
        gen = BooleanFieldGenerator()
        for _ in range(50):
            assert isinstance(gen.generate(), bool)

    def test_both_values_generated(self):
        gen = BooleanFieldGenerator()
        values = {gen.generate() for _ in range(100)}
        assert True in values
        assert False in values


class TestUuidFieldGenerator:
    def test_valid_uuid_format(self):
        gen = UuidFieldGenerator()
        for _ in range(50):
            value = gen.generate()
            uuid.UUID(value)  # Raises if invalid

    def test_unique_values(self):
        gen = UuidFieldGenerator()
        values = [gen.generate() for _ in range(100)]
        assert len(set(values)) == 100


class TestTimestampFieldGenerator:
    def test_returns_epoch_millis(self):
        gen = TimestampFieldGenerator()
        value = gen.generate()
        assert isinstance(value, int)
        # Should be a reasonable timestamp (after year 2020, before year 2100)
        assert 1577836800000 < value < 4102444800000

    def test_is_current_time(self):
        gen = TimestampFieldGenerator()
        before = int(datetime.now(UTC).timestamp() * 1000)
        value = gen.generate()
        after = int(datetime.now(UTC).timestamp() * 1000)
        assert before <= value <= after


class TestEnumFieldGenerator:
    def test_values_from_list(self):
        values = ["red", "green", "blue"]
        gen = EnumFieldGenerator(values)
        for _ in range(100):
            assert gen.generate() in values

    def test_all_values_generated(self):
        values = ["a", "b", "c"]
        gen = EnumFieldGenerator(values)
        generated = {gen.generate() for _ in range(100)}
        assert generated == set(values)

    def test_weighted_enum_simulation(self):
        values = ["success", "success", "success", "failed"]
        gen = EnumFieldGenerator(values)
        counts = {"success": 0, "failed": 0}
        for _ in range(1000):
            counts[gen.generate()] += 1
        # Success should be roughly 3x more common
        assert counts["success"] > counts["failed"] * 2


class TestObjectFieldGenerator:
    def test_generates_dict(self):
        gen = ObjectFieldGenerator([("name", StringFieldGenerator())])
        value = gen.generate()
        assert isinstance(value, dict)

    def test_includes_all_fields(self):
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
    def test_length_within_range(self):
        gen = ArrayFieldGenerator(IntFieldGenerator(), min_items=3, max_items=7)
        for _ in range(50):
            value = gen.generate()
            assert 3 <= len(value) <= 7

    def test_items_have_correct_type(self):
        gen = ArrayFieldGenerator(UuidFieldGenerator(), min_items=5, max_items=5)
        value = gen.generate()
        for item in value:
            uuid.UUID(item)  # Validates UUID format

    def test_exact_length(self):
        gen = ArrayFieldGenerator(IntFieldGenerator(), min_items=4, max_items=4)
        for _ in range(50):
            assert len(gen.generate()) == 4


class TestFakerFieldGenerator:
    def test_name_provider(self):
        gen = FakerFieldGenerator(provider="name")
        value = gen.generate()
        assert isinstance(value, str)
        assert len(value) > 0

    def test_email_provider(self):
        gen = FakerFieldGenerator(provider="email")
        value = gen.generate()
        assert "@" in value
        assert "." in value

    def test_date_provider_returns_string(self):
        gen = FakerFieldGenerator(provider="date_this_year")
        value = gen.generate()
        assert isinstance(value, str)
        assert re.match(r"\d{4}-\d{2}-\d{2}", value)

    def test_datetime_provider_returns_string(self):
        gen = FakerFieldGenerator(provider="date_time_this_year")
        value = gen.generate()
        assert isinstance(value, str)
        assert "T" in value or "-" in value

    def test_invalid_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown faker provider"):
            FakerFieldGenerator(provider="nonexistent_provider_xyz")

    def test_locale_support(self):
        gen = FakerFieldGenerator(provider="name", locale="de_DE")
        value = gen.generate()
        assert isinstance(value, str)


class TestCreateFieldGenerator:
    def test_creates_string_generator(self):
        schema = FieldSchema(name="test", type="string", min_length=5, max_length=10)
        gen = create_field_generator(schema)
        assert isinstance(gen, StringFieldGenerator)
        value = gen.generate()
        assert 5 <= len(value) <= 10

    def test_creates_int_generator(self):
        schema = FieldSchema(name="test", type="int", min=10, max=20)
        gen = create_field_generator(schema)
        assert isinstance(gen, IntFieldGenerator)
        value = gen.generate()
        assert 10 <= value <= 20

    def test_creates_float_generator(self):
        schema = FieldSchema(name="test", type="float", min=1.5, max=5.5)
        gen = create_field_generator(schema)
        assert isinstance(gen, FloatFieldGenerator)

    def test_creates_boolean_generator(self):
        schema = FieldSchema(name="test", type="boolean")
        gen = create_field_generator(schema)
        assert isinstance(gen, BooleanFieldGenerator)

    def test_creates_uuid_generator(self):
        schema = FieldSchema(name="test", type="uuid")
        gen = create_field_generator(schema)
        assert isinstance(gen, UuidFieldGenerator)

    def test_creates_timestamp_generator(self):
        schema = FieldSchema(name="test", type="timestamp")
        gen = create_field_generator(schema)
        assert isinstance(gen, TimestampFieldGenerator)

    def test_creates_enum_generator(self):
        schema = FieldSchema(name="test", type="enum", values=["a", "b", "c"])
        gen = create_field_generator(schema)
        assert isinstance(gen, EnumFieldGenerator)
        assert gen.generate() in ["a", "b", "c"]

    def test_enum_requires_values(self):
        schema = FieldSchema(name="test", type="enum")
        with pytest.raises(ValueError, match="requires 'values'"):
            create_field_generator(schema)

    def test_creates_object_generator(self):
        inner_field = FieldSchema(name="inner", type="string")
        schema = FieldSchema(name="test", type="object", fields=[inner_field])
        gen = create_field_generator(schema)
        assert isinstance(gen, ObjectFieldGenerator)
        value = gen.generate()
        assert "inner" in value

    def test_object_requires_fields(self):
        schema = FieldSchema(name="test", type="object")
        with pytest.raises(ValueError, match="requires 'fields'"):
            create_field_generator(schema)

    def test_creates_array_generator(self):
        item_schema = FieldSchema(name="item", type="int")
        schema = FieldSchema(name="test", type="array", items=item_schema, min_items=2, max_items=5)
        gen = create_field_generator(schema)
        assert isinstance(gen, ArrayFieldGenerator)
        value = gen.generate()
        assert 2 <= len(value) <= 5

    def test_array_requires_items(self):
        schema = FieldSchema(name="test", type="array")
        with pytest.raises(ValueError, match="requires 'items'"):
            create_field_generator(schema)

    def test_creates_faker_generator(self):
        schema = FieldSchema(name="test", type="faker", provider="email")
        gen = create_field_generator(schema)
        assert isinstance(gen, FakerFieldGenerator)
        assert "@" in gen.generate()

    def test_faker_requires_provider(self):
        schema = FieldSchema(name="test", type="faker")
        with pytest.raises(ValueError, match="requires 'provider'"):
            create_field_generator(schema)

    def test_unknown_type_raises(self):
        schema = FieldSchema(name="test", type="unknown_type")
        with pytest.raises(ValueError, match="Unknown field type"):
            create_field_generator(schema)
