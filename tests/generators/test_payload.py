import json

from khaos.generators.payload import (
    FixedPayloadGenerator,
    JsonPayloadGenerator,
    RandomPayloadGenerator,
    create_payload_generator,
)
from khaos.models.message import MessageSchema


class TestRandomPayloadGenerator:
    def test_size_within_range(self):
        min_size = 50
        max_size = 100
        gen = RandomPayloadGenerator(min_size=min_size, max_size=max_size)

        for _ in range(100):
            payload = gen.generate()
            assert min_size <= len(payload) <= max_size

    def test_fixed_size_when_min_equals_max(self):
        size = 150
        gen = RandomPayloadGenerator(min_size=size, max_size=size)

        for _ in range(100):
            payload = gen.generate()
            assert len(payload) == size

    def test_random_content(self):
        gen = RandomPayloadGenerator(min_size=100, max_size=100)

        payloads = [gen.generate() for _ in range(10)]
        assert len(set(payloads)) == 10


class TestJsonPayloadGenerator:
    def test_generates_valid_json(self):
        gen = JsonPayloadGenerator(min_size=100, max_size=200)
        payload = gen.generate()

        data = json.loads(payload.decode())
        assert isinstance(data, dict)
        assert "id" in data
        assert data["id"].startswith("msg-")

    def test_sequence_increments(self):
        gen = JsonPayloadGenerator(min_size=100, max_size=200)

        sequences = []
        for _ in range(10):
            payload = gen.generate()
            data = json.loads(payload.decode())
            sequences.append(data["sequence"])

        assert sequences == list(range(1, 11))

    def test_can_disable_timestamp_and_sequence(self):
        gen = JsonPayloadGenerator(
            min_size=100, max_size=200, include_timestamp=False, include_sequence=False
        )
        payload = gen.generate()
        data = json.loads(payload.decode())

        assert "timestamp" not in data
        assert "sequence" not in data

    def test_adds_padding_for_min_size(self):
        min_size = 500
        gen = JsonPayloadGenerator(min_size=min_size, max_size=1000)
        payload = gen.generate()
        data = json.loads(payload.decode())

        # Should have data field for padding
        assert "data" in data
        assert len(payload) >= min_size - 50  # Allow some variance for JSON overhead


class TestFixedPayloadGenerator:
    def test_generates_exact_size(self):
        size = 150
        gen = FixedPayloadGenerator(size=size)

        for _ in range(10):
            payload = gen.generate()
            assert len(payload) == size

    def test_sequence_increments_in_prefix(self):
        gen = FixedPayloadGenerator(size=100)

        for i in range(1, 6):
            payload = gen.generate()
            expected_prefix = f"msg-{i}-".encode()
            assert payload.startswith(expected_prefix)

    def test_small_size_outputs_prefix_only(self):
        gen = FixedPayloadGenerator(size=3)
        payload = gen.generate()
        # Prefix "msg-1-" is longer than size, but no truncation occurs
        assert payload == b"msg-1-"


class TestCreatePayloadGenerator:
    def test_creates_json_generator_with_correct_settings(self):
        schema = MessageSchema(
            min_size_bytes=150,
            max_size_bytes=300,
            include_timestamp=False,
            include_sequence=False,
        )
        gen = create_payload_generator(schema)

        assert isinstance(gen, JsonPayloadGenerator)
        assert gen.min_size == 150
        assert gen.max_size == 300
        assert gen.include_timestamp is False
        assert gen.include_sequence is False

    def test_generated_payload_is_valid_json(self):
        schema = MessageSchema(min_size_bytes=100, max_size_bytes=500)
        gen = create_payload_generator(schema)
        payload = gen.generate()

        data = json.loads(payload.decode())
        assert "id" in data
