from __future__ import annotations

import random
import string
import uuid
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from typing import Any

from faker import Faker

from khaos.models.schema import FieldSchema


class FieldGenerator(ABC):
    @abstractmethod
    def generate(self) -> Any: ...


class StringFieldGenerator(FieldGenerator):
    def __init__(
        self,
        min_length: int = 5,
        max_length: int = 20,
        cardinality: int | None = None,
    ):
        self.min_length = min_length
        self.max_length = max_length
        self.cardinality = cardinality
        self._cache: list[str] = []
        self._index = 0

    def generate(self) -> str:
        if self.cardinality:
            if len(self._cache) < self.cardinality:
                while True:
                    value = self._generate_random()
                    if value not in self._cache:
                        self._cache.append(value)
                        return value
            value = self._cache[self._index % self.cardinality]
            self._index += 1
            return value
        return self._generate_random()

    def _generate_random(self) -> str:
        length = random.randint(self.min_length, self.max_length)
        return "".join(random.choices(string.ascii_lowercase, k=length))


class IntFieldGenerator(FieldGenerator):
    def __init__(
        self,
        min_val: int = 0,
        max_val: int = 1000,
        cardinality: int | None = None,
    ):
        self.min_val = min_val
        self.max_val = max_val
        self.cardinality = cardinality
        self._cache: list[int] = []
        self._index = 0

    def generate(self) -> int:
        if self.cardinality:
            if len(self._cache) < self.cardinality:
                while True:
                    value = self._generate_random()
                    if value not in self._cache:
                        self._cache.append(value)
                        return value
            value = self._cache[self._index % self.cardinality]
            self._index += 1
            return value
        return self._generate_random()

    def _generate_random(self) -> int:
        return random.randint(self.min_val, self.max_val)


class FloatFieldGenerator(FieldGenerator):
    def __init__(self, min_val: float = 0.0, max_val: float = 1000.0):
        self.min_val = min_val
        self.max_val = max_val

    def generate(self) -> float:
        return round(random.uniform(self.min_val, self.max_val), 2)


class BooleanFieldGenerator(FieldGenerator):
    def generate(self) -> bool:
        return random.choice([True, False])


class UuidFieldGenerator(FieldGenerator):
    def generate(self) -> str:
        return str(uuid.uuid4())


class TimestampFieldGenerator(FieldGenerator):
    def generate(self) -> int:
        return int(datetime.now(UTC).timestamp() * 1000)


class EnumFieldGenerator(FieldGenerator):
    def __init__(self, values: list[str]):
        self.values = values

    def generate(self) -> str:
        return random.choice(self.values)


class ObjectFieldGenerator(FieldGenerator):
    def __init__(self, field_generators: list[tuple[str, FieldGenerator]]):
        self.field_generators = field_generators

    def generate(self) -> dict[str, Any]:
        return {name: gen.generate() for name, gen in self.field_generators}


class ArrayFieldGenerator(FieldGenerator):
    def __init__(
        self,
        item_generator: FieldGenerator,
        min_items: int = 1,
        max_items: int = 5,
    ):
        self.item_generator = item_generator
        self.min_items = min_items
        self.max_items = max_items

    def generate(self) -> list[Any]:
        count = random.randint(self.min_items, self.max_items)
        return [self.item_generator.generate() for _ in range(count)]


class FakerFieldGenerator(FieldGenerator):
    def __init__(self, provider: str, locale: str | None = None):
        self.fake = Faker(locale) if locale else Faker()
        self.provider = provider

        # Validate provider exists
        if not hasattr(self.fake, provider):
            raise ValueError(f"Unknown faker provider: '{provider}'")

    def generate(self) -> Any:
        provider_method = getattr(self.fake, self.provider)
        value = provider_method()

        # Convert date/datetime to ISO string for JSON serialization
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()

        return value


def create_field_generator(schema: FieldSchema) -> FieldGenerator:
    field_type = schema.type

    if field_type == "string":
        return StringFieldGenerator(
            min_length=schema.min_length or 5,
            max_length=schema.max_length or 20,
            cardinality=schema.cardinality,
        )

    if field_type == "int":
        return IntFieldGenerator(
            min_val=int(schema.min) if schema.min is not None else 0,
            max_val=int(schema.max) if schema.max is not None else 1000,
            cardinality=schema.cardinality,
        )

    if field_type == "float":
        return FloatFieldGenerator(
            min_val=schema.min if schema.min is not None else 0.0,
            max_val=schema.max if schema.max is not None else 1000.0,
        )

    if field_type == "boolean":
        return BooleanFieldGenerator()

    if field_type == "uuid":
        return UuidFieldGenerator()

    if field_type == "timestamp":
        return TimestampFieldGenerator()

    if field_type == "enum":
        if not schema.values:
            raise ValueError(f"Enum field '{schema.name}' requires 'values' list")
        return EnumFieldGenerator(schema.values)

    if field_type == "object":
        if not schema.fields:
            raise ValueError(f"Object field '{schema.name}' requires 'fields' list")
        nested_generators = [(field.name, create_field_generator(field)) for field in schema.fields]
        return ObjectFieldGenerator(nested_generators)

    if field_type == "array":
        if not schema.items:
            raise ValueError(f"Array field '{schema.name}' requires 'items' schema")
        item_generator = create_field_generator(schema.items)
        return ArrayFieldGenerator(
            item_generator=item_generator,
            min_items=schema.min_items,
            max_items=schema.max_items,
        )

    if field_type == "faker":
        if not schema.provider:
            raise ValueError(f"Faker field '{schema.name}' requires 'provider'")
        return FakerFieldGenerator(provider=schema.provider, locale=schema.locale)

    raise ValueError(f"Unknown field type: {field_type}")
