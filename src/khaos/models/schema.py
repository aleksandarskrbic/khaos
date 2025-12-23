from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FieldSchema:
    name: str
    type: str  # string, int, float, boolean, uuid, timestamp, enum, object, array

    # Numeric constraints
    min: float | None = None
    max: float | None = None

    # String constraints
    min_length: int | None = None
    max_length: int | None = None

    # Cardinality (for controlled uniqueness)
    cardinality: int | None = None

    # Enum values
    values: list[str] | None = None

    # Nested object fields
    fields: list[FieldSchema] | None = None

    # Array item schema
    items: FieldSchema | None = None
    min_items: int = 1
    max_items: int = 5

    # Faker provider (for type: faker)
    provider: str | None = None
    locale: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> FieldSchema:
        # Handle nested fields recursively
        nested_fields = None
        if data.get("fields"):
            nested_fields = [cls.from_dict(f) for f in data["fields"]]

        # Handle array items recursively
        items_schema = None
        if data.get("items"):
            items_schema = cls.from_dict(data["items"])

        return cls(
            name=data.get("name", ""),
            type=data["type"],
            min=data.get("min"),
            max=data.get("max"),
            min_length=data.get("min_length"),
            max_length=data.get("max_length"),
            cardinality=data.get("cardinality"),
            values=data.get("values"),
            fields=nested_fields,
            items=items_schema,
            min_items=data.get("min_items", 1),
            max_items=data.get("max_items", 5),
            provider=data.get("provider"),
            locale=data.get("locale"),
        )


# Valid field types
VALID_FIELD_TYPES = frozenset(
    {
        "string",
        "int",
        "float",
        "boolean",
        "uuid",
        "timestamp",
        "enum",
        "object",
        "array",
        "faker",
    }
)
