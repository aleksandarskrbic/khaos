"""Tests for key generators."""

from collections import Counter

from khaos.generators.key import (
    RoundRobinKeyGenerator,
    SingleKeyGenerator,
    UniformKeyGenerator,
    ZipfianKeyGenerator,
    create_key_generator,
)
from khaos.models.message import KeyDistribution, MessageSchema


class TestUniformKeyGenerator:
    """Tests for UniformKeyGenerator."""

    def test_respects_cardinality(self):
        """Test that only keys within cardinality are generated."""
        cardinality = 5
        gen = UniformKeyGenerator(cardinality=cardinality)

        generated_keys = set()
        for _ in range(1000):
            generated_keys.add(gen.generate())

        expected_keys = {f"key-{i}".encode() for i in range(cardinality)}
        assert generated_keys.issubset(expected_keys)

    def test_roughly_uniform_distribution(self):
        """Test that distribution is roughly uniform."""
        cardinality = 5
        gen = UniformKeyGenerator(cardinality=cardinality)

        counts = Counter()
        num_samples = 10000
        for _ in range(num_samples):
            counts[gen.generate()] += 1

        # Each key should get roughly 1/cardinality of samples (20% each)
        expected_count = num_samples / cardinality
        for _key, count in counts.items():
            assert abs(count - expected_count) < expected_count * 0.2


class TestZipfianKeyGenerator:
    """Tests for ZipfianKeyGenerator."""

    def test_respects_cardinality(self):
        """Test that only keys within cardinality are generated."""
        cardinality = 10
        gen = ZipfianKeyGenerator(cardinality=cardinality)

        generated_keys = set()
        for _ in range(1000):
            generated_keys.add(gen.generate())

        expected_keys = {f"key-{i}".encode() for i in range(cardinality)}
        assert generated_keys.issubset(expected_keys)

    def test_skewed_distribution(self):
        """Test that distribution is skewed toward lower-indexed keys."""
        cardinality = 10
        gen = ZipfianKeyGenerator(cardinality=cardinality, skew=1.5)

        counts = Counter()
        for _ in range(10000):
            counts[gen.generate()] += 1

        # key-0 should be most frequent, significantly more than key-9
        key_0_count = counts[b"key-0"]
        key_9_count = counts[b"key-9"]
        assert key_0_count > key_9_count * 5

    def test_higher_skew_more_concentrated(self):
        """Test that higher skew leads to more concentrated distribution."""
        cardinality = 10

        gen_low_skew = ZipfianKeyGenerator(cardinality=cardinality, skew=0.5)
        gen_high_skew = ZipfianKeyGenerator(cardinality=cardinality, skew=2.0)

        counts_low = Counter()
        counts_high = Counter()

        for _ in range(10000):
            counts_low[gen_low_skew.generate()] += 1
            counts_high[gen_high_skew.generate()] += 1

        ratio_low = counts_low[b"key-0"] / counts_low[b"key-9"]
        ratio_high = counts_high[b"key-0"] / counts_high[b"key-9"]

        assert ratio_high > ratio_low


class TestSingleKeyGenerator:
    """Tests for SingleKeyGenerator."""

    def test_always_same_key(self):
        """Test that always returns the same key."""
        gen = SingleKeyGenerator()

        first_key = gen.generate()
        for _ in range(100):
            assert gen.generate() == first_key

    def test_custom_key(self):
        """Test custom key value."""
        gen = SingleKeyGenerator(key="my-custom-key")
        assert gen.generate() == b"my-custom-key"


class TestRoundRobinKeyGenerator:
    """Tests for RoundRobinKeyGenerator."""

    def test_sequential_order(self):
        """Test that keys are generated in sequential order."""
        cardinality = 5
        gen = RoundRobinKeyGenerator(cardinality=cardinality)

        for i in range(cardinality):
            expected = f"key-{i}".encode()
            assert gen.generate() == expected

    def test_wraps_around(self):
        """Test that generator wraps around after cardinality."""
        cardinality = 3
        gen = RoundRobinKeyGenerator(cardinality=cardinality)

        # First cycle
        assert gen.generate() == b"key-0"
        assert gen.generate() == b"key-1"
        assert gen.generate() == b"key-2"

        # Should wrap to beginning
        assert gen.generate() == b"key-0"

    def test_perfectly_even_distribution(self):
        """Test that distribution is perfectly even over full cycles."""
        cardinality = 4
        gen = RoundRobinKeyGenerator(cardinality=cardinality)

        counts = Counter()
        num_cycles = 100
        for _ in range(cardinality * num_cycles):
            counts[gen.generate()] += 1

        # Each key should have exactly the same count
        for key in counts.values():
            assert key == num_cycles


class TestCreateKeyGenerator:
    """Tests for create_key_generator factory function."""

    def test_creates_correct_generator_types(self):
        """Test that factory creates correct generator types."""
        test_cases = [
            (KeyDistribution.UNIFORM, UniformKeyGenerator),
            (KeyDistribution.ZIPFIAN, ZipfianKeyGenerator),
            (KeyDistribution.SINGLE_KEY, SingleKeyGenerator),
            (KeyDistribution.ROUND_ROBIN, RoundRobinKeyGenerator),
        ]

        for dist, expected_type in test_cases:
            schema = MessageSchema(key_distribution=dist, key_cardinality=10)
            gen = create_key_generator(schema)
            assert isinstance(gen, expected_type)

    def test_passes_cardinality(self):
        """Test that cardinality is passed correctly."""
        schema = MessageSchema(
            key_distribution=KeyDistribution.UNIFORM,
            key_cardinality=50,
        )
        gen = create_key_generator(schema)
        assert gen.cardinality == 50
