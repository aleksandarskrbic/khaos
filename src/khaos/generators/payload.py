from __future__ import annotations

import json
import random
import time
from abc import ABC, abstractmethod
from typing import Any

from khaos.models.message import MessageSchema
from khaos.serialization.base import Serializer


class PayloadGenerator(ABC):
    @abstractmethod
    def generate(self) -> bytes:
        pass


class RandomPayloadGenerator(PayloadGenerator):
    def __init__(self, min_size: int, max_size: int):
        self.min_size = min_size
        self.max_size = max_size

    def generate(self) -> bytes:
        size = random.randint(self.min_size, self.max_size)
        return random.randbytes(size)


class JsonPayloadGenerator(PayloadGenerator):
    def __init__(
        self,
        min_size: int,
        max_size: int,
        include_timestamp: bool = True,
        include_sequence: bool = True,
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.include_timestamp = include_timestamp
        self.include_sequence = include_sequence
        self._sequence = 0

    def generate(self) -> bytes:
        self._sequence += 1
        seq = self._sequence

        payload: dict[str, Any] = {
            "id": f"msg-{seq}",
        }

        if self.include_timestamp:
            payload["timestamp"] = int(time.time() * 1000)

        if self.include_sequence:
            payload["sequence"] = seq

        base_json = json.dumps(payload).encode()
        current_size = len(base_json)

        target_size = random.randint(self.min_size, self.max_size)

        if current_size < target_size:
            padding_needed = target_size - current_size - 20
            if padding_needed > 0:
                payload["data"] = "x" * padding_needed

        return json.dumps(payload).encode()


class FixedPayloadGenerator(PayloadGenerator):
    def __init__(self, size: int):
        self.size = size
        self._sequence = 0

    def generate(self) -> bytes:
        self._sequence += 1
        prefix = f"msg-{self._sequence}-".encode()
        padding_size = max(0, self.size - len(prefix))
        return prefix + (b"x" * padding_size)


def create_payload_generator(
    schema: MessageSchema,
    serializer: Serializer | None = None,
) -> PayloadGenerator:
    if schema.fields:
        from khaos.generators.schema import SchemaPayloadGenerator

        return SchemaPayloadGenerator(schema.fields, serializer=serializer)

    return JsonPayloadGenerator(
        min_size=schema.min_size_bytes,
        max_size=schema.max_size_bytes,
        include_timestamp=schema.include_timestamp,
        include_sequence=schema.include_sequence,
    )
