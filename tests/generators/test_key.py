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
    def test_respects_cardinality(self):
        cardinality = 5
        gen = UniformKeyGenerator(cardinality=cardinality)

        generated_keys = set()
        for _ in range(1000):
            generated_keys.add(gen.generate())

        expected_keys = {f"key-{i}".encode() for i in range(cardinality)}
        assert generated_keys.issubset(expected_keys)

    def test_roughly_uniform_distribution(self):
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
    def test_respects_cardinality(self):
        cardinality = 10
        gen = ZipfianKeyGenerator(cardinality=cardinality)

        generated_keys = set()
        for _ in range(1000):
            generated_keys.add(gen.generate())

        expected_keys = {f"key-{i}".encode() for i in range(cardinality)}
        assert generated_keys.issubset(expected_keys)

    def test_skewed_distribution(self):
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
    def test_always_same_key(self):
        gen = SingleKeyGenerator()

        first_key = gen.generate()
        for _ in range(100):
            assert gen.generate() == first_key

    def test_custom_key(self):
        gen = SingleKeyGenerator(key="my-custom-key")
        assert gen.generate() == b"my-custom-key"


class TestRoundRobinKeyGenerator:
    def test_sequential_order(self):
        cardinality = 5
        gen = RoundRobinKeyGenerator(cardinality=cardinality)

        for i in range(cardinality):
            expected = f"key-{i}".encode()
            assert gen.generate() == expected

    def test_wraps_around(self):
        cardinality = 3
        gen = RoundRobinKeyGenerator(cardinality=cardinality)

        # First cycle
        assert gen.generate() == b"key-0"
        assert gen.generate() == b"key-1"
        assert gen.generate() == b"key-2"

        # Should wrap to beginning
        assert gen.generate() == b"key-0"

    def test_perfectly_even_distribution(self):
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
    def test_creates_correct_generator_types(self):
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
        schema = MessageSchema(
            key_distribution=KeyDistribution.UNIFORM,
            key_cardinality=50,
        )
        gen = create_key_generator(schema)
        assert gen.cardinality == 50
