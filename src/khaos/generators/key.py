"""Key generation strategies."""

import random
from abc import ABC, abstractmethod

from khaos.models.message import KeyDistribution, MessageSchema


class KeyGenerator(ABC):
    """Abstract base class for key generators."""

    @abstractmethod
    def generate(self) -> bytes:
        """Generate a key."""
        pass


class UniformKeyGenerator(KeyGenerator):
    """Generate keys with uniform distribution."""

    def __init__(self, cardinality: int):
        self.cardinality = cardinality
        self._keys = [f"key-{i}".encode() for i in range(cardinality)]

    def generate(self) -> bytes:
        return random.choice(self._keys)


class ZipfianKeyGenerator(KeyGenerator):
    """Generate keys with Zipfian (power-law) distribution.

    This creates a "hot key" pattern where a small number of keys
    receive the majority of traffic.
    """

    def __init__(self, cardinality: int, skew: float = 1.5):
        self.cardinality = cardinality
        self.skew = skew
        self._keys = [f"key-{i}".encode() for i in range(cardinality)]
        # Pre-compute weights for Zipfian distribution
        self._weights = [1.0 / ((i + 1) ** skew) for i in range(cardinality)]
        total = sum(self._weights)
        self._weights = [w / total for w in self._weights]

    def generate(self) -> bytes:
        return random.choices(self._keys, weights=self._weights, k=1)[0]


class SingleKeyGenerator(KeyGenerator):
    """Generate the same key for all messages (hot partition)."""

    def __init__(self, key: str = "hot-key"):
        self._key = key.encode()

    def generate(self) -> bytes:
        return self._key


class RoundRobinKeyGenerator(KeyGenerator):
    """Generate keys in round-robin fashion."""

    def __init__(self, cardinality: int):
        self.cardinality = cardinality
        self._keys = [f"key-{i}".encode() for i in range(cardinality)]
        self._index = 0

    def generate(self) -> bytes:
        key = self._keys[self._index]
        self._index = (self._index + 1) % self.cardinality
        return key


def create_key_generator(schema: MessageSchema) -> KeyGenerator:
    """Factory function to create key generator based on schema."""
    if schema.key_distribution == KeyDistribution.UNIFORM:
        return UniformKeyGenerator(schema.key_cardinality)
    elif schema.key_distribution == KeyDistribution.ZIPFIAN:
        return ZipfianKeyGenerator(schema.key_cardinality)
    elif schema.key_distribution == KeyDistribution.SINGLE_KEY:
        return SingleKeyGenerator()
    elif schema.key_distribution == KeyDistribution.ROUND_ROBIN:
        return RoundRobinKeyGenerator(schema.key_cardinality)
    else:
        raise ValueError(f"Unknown key distribution: {schema.key_distribution}")
