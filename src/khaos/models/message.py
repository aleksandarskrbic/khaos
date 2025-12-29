from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from khaos.defaults import (
    DEFAULT_KEY_CARDINALITY,
    DEFAULT_MAX_SIZE_BYTES,
    DEFAULT_MIN_SIZE_BYTES,
)
from khaos.models.schema import FieldSchema


class KeyDistribution(Enum):
    UNIFORM = "uniform"  # Even distribution across partitions
    ZIPFIAN = "zipfian"  # 80/20 hot key distribution
    SINGLE_KEY = "single_key"  # All messages to one partition
    ROUND_ROBIN = "round_robin"  # Sequential key assignment

    @classmethod
    def from_string(cls, name: str) -> KeyDistribution:
        try:
            return cls(name) if name else cls.UNIFORM
        except ValueError:
            return cls.UNIFORM


@dataclass
class MessageSchema:
    min_size_bytes: int = DEFAULT_MIN_SIZE_BYTES
    max_size_bytes: int = DEFAULT_MAX_SIZE_BYTES
    key_distribution: KeyDistribution = KeyDistribution.UNIFORM
    key_cardinality: int = DEFAULT_KEY_CARDINALITY
    include_timestamp: bool = True
    include_sequence: bool = True
    # Structured field schemas (optional - if not set, use random bytes)
    fields: list[FieldSchema] | None = None

    def __post_init__(self):
        if self.min_size_bytes < 1:
            raise ValueError("min_size_bytes must be at least 1")
        if self.max_size_bytes < self.min_size_bytes:
            raise ValueError("max_size_bytes must be >= min_size_bytes")
        if self.key_cardinality < 1:
            raise ValueError("key_cardinality must be at least 1")
