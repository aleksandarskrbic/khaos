"""Schema-based payload generator for structured messages."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from khaos.generators.field import FieldGenerator, create_field_generator
from khaos.generators.payload import PayloadGenerator

if TYPE_CHECKING:
    from khaos.models.schema import FieldSchema


class SchemaPayloadGenerator(PayloadGenerator):
    """Generate JSON payloads based on field schema definitions."""

    def __init__(self, fields: list[FieldSchema]):
        self.fields = fields
        self.field_generators: list[tuple[str, FieldGenerator]] = [
            (field.name, create_field_generator(field)) for field in fields
        ]

    def generate(self) -> bytes:
        """Generate a JSON payload from field schemas."""
        obj = {name: gen.generate() for name, gen in self.field_generators}
        return json.dumps(obj).encode()
