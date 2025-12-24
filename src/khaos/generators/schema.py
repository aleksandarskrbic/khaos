from __future__ import annotations

from typing import Any

from khaos.generators.field import FieldGenerator, create_field_generator
from khaos.generators.payload import PayloadGenerator
from khaos.models.schema import FieldSchema
from khaos.serialization import JsonSerializer
from khaos.serialization.base import Serializer


class SchemaPayloadGenerator(PayloadGenerator):
    def __init__(
        self,
        fields: list[FieldSchema],
        serializer: Serializer | None = None,
    ):
        self.fields = fields
        self.field_generators: list[tuple[str, FieldGenerator]] = [
            (field.name, create_field_generator(field)) for field in fields
        ]
        self.serializer: Serializer = serializer or JsonSerializer()

    def generate(self) -> bytes:
        obj = {name: gen.generate() for name, gen in self.field_generators}
        return self.serializer.serialize(obj)

    def generate_dict(self) -> dict[str, Any]:
        return {name: gen.generate() for name, gen in self.field_generators}
