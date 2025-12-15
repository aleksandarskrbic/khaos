"""Message schema and key distribution models."""

from dataclasses import dataclass
from enum import Enum


class KeyDistribution(Enum):
    """Key generation distribution strategies."""

    UNIFORM = "uniform"  # Even distribution across partitions
    ZIPFIAN = "zipfian"  # 80/20 hot key distribution
    SINGLE_KEY = "single_key"  # All messages to one partition
    ROUND_ROBIN = "round_robin"  # Sequential key assignment


@dataclass
class MessageSchema:
    """Schema for generated messages."""

    min_size_bytes: int = 100
    max_size_bytes: int = 1000
    key_distribution: KeyDistribution = KeyDistribution.UNIFORM
    key_cardinality: int = 100  # Number of unique keys
    include_timestamp: bool = True
    include_sequence: bool = True

    def __post_init__(self):
        if self.min_size_bytes < 1:
            raise ValueError("min_size_bytes must be at least 1")
        if self.max_size_bytes < self.min_size_bytes:
            raise ValueError("max_size_bytes must be >= min_size_bytes")
        if self.key_cardinality < 1:
            raise ValueError("key_cardinality must be at least 1")
