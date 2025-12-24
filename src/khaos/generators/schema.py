from __future__ import annotations

import json

from khaos.generators.field import FieldGenerator, create_field_generator
from khaos.generators.payload import PayloadGenerator
from khaos.models.schema import FieldSchema


class SchemaPayloadGenerator(PayloadGenerator):
    def __init__(self, fields: list[FieldSchema]):
        self.fields = fields
        self.field_generators: list[tuple[str, FieldGenerator]] = [
            (field.name, create_field_generator(field)) for field in fields
        ]

    def generate(self) -> bytes:
        obj = {name: gen.generate() for name, gen in self.field_generators}
        return json.dumps(obj).encode()
