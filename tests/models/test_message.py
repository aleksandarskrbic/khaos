import pytest

from khaos.models.message import KeyDistribution, MessageSchema


class TestMessageSchema:
    def test_default_values(self):
        schema = MessageSchema()

        assert schema.min_size_bytes == 100
        assert schema.max_size_bytes == 1000
        assert schema.key_distribution == KeyDistribution.UNIFORM
        assert schema.key_cardinality == 100
        assert schema.include_timestamp is True
        assert schema.include_sequence is True

    def test_custom_values(self):
        schema = MessageSchema(
            min_size_bytes=50,
            max_size_bytes=500,
            key_distribution=KeyDistribution.ZIPFIAN,
            key_cardinality=50,
            include_timestamp=False,
            include_sequence=False,
        )

        assert schema.min_size_bytes == 50
        assert schema.max_size_bytes == 500
        assert schema.key_distribution == KeyDistribution.ZIPFIAN
        assert schema.key_cardinality == 50
        assert schema.include_timestamp is False
        assert schema.include_sequence is False

    def test_min_size_must_be_positive(self):
        with pytest.raises(ValueError) as exc_info:
            MessageSchema(min_size_bytes=0)

        assert "min_size_bytes must be at least 1" in str(exc_info.value)

    def test_max_size_must_be_gte_min(self):
        with pytest.raises(ValueError) as exc_info:
            MessageSchema(min_size_bytes=100, max_size_bytes=50)

        assert "max_size_bytes must be >= min_size_bytes" in str(exc_info.value)

    def test_key_cardinality_must_be_positive(self):
        with pytest.raises(ValueError) as exc_info:
            MessageSchema(key_cardinality=0)

        assert "key_cardinality must be at least 1" in str(exc_info.value)

    def test_all_key_distributions_valid(self):
        for dist in KeyDistribution:
            schema = MessageSchema(key_distribution=dist)
            assert schema.key_distribution == dist
