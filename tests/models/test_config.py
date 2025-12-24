import pytest

from khaos.models.config import ConsumerConfig, ProducerConfig


class TestProducerConfig:
    def test_default_values(self):
        config = ProducerConfig()

        assert config.messages_per_second == 1000.0
        assert config.batch_size == 16384
        assert config.linger_ms == 5
        assert config.acks == "all"
        assert config.compression_type == "none"

    def test_custom_values(self):
        config = ProducerConfig(
            messages_per_second=500.0,
            batch_size=32768,
            linger_ms=10,
            acks="1",
            compression_type="lz4",
        )

        assert config.messages_per_second == 500.0
        assert config.batch_size == 32768
        assert config.linger_ms == 10
        assert config.acks == "1"
        assert config.compression_type == "lz4"

    def test_messages_per_second_must_be_positive(self):
        with pytest.raises(ValueError) as exc_info:
            ProducerConfig(messages_per_second=0)

        assert "messages_per_second must be positive" in str(exc_info.value)

    def test_messages_per_second_negative(self):
        with pytest.raises(ValueError):
            ProducerConfig(messages_per_second=-100)

    def test_invalid_acks(self):
        with pytest.raises(ValueError) as exc_info:
            ProducerConfig(acks="2")

        assert "acks must be" in str(exc_info.value)

    def test_invalid_compression_type(self):
        with pytest.raises(ValueError) as exc_info:
            ProducerConfig(compression_type="invalid")

        assert "compression_type" in str(exc_info.value)

    def test_all_valid_combinations(self):
        valid_acks = ["0", "1", "all"]
        valid_compression = ["none", "gzip", "snappy", "lz4", "zstd"]

        for acks in valid_acks:
            for comp in valid_compression:
                config = ProducerConfig(acks=acks, compression_type=comp)
                assert config.acks == acks
                assert config.compression_type == comp


class TestConsumerConfig:
    def test_default_values(self):
        config = ConsumerConfig(group_id="test-group")

        assert config.processing_delay_ms == 0
        assert config.max_poll_records == 500
        assert config.auto_offset_reset == "latest"
        assert config.session_timeout_ms == 45000

    def test_custom_values(self):
        config = ConsumerConfig(
            group_id="custom-group",
            processing_delay_ms=100,
            max_poll_records=1000,
            auto_offset_reset="earliest",
            session_timeout_ms=60000,
        )

        assert config.group_id == "custom-group"
        assert config.processing_delay_ms == 100
        assert config.max_poll_records == 1000
        assert config.auto_offset_reset == "earliest"
        assert config.session_timeout_ms == 60000

    def test_processing_delay_cannot_be_negative(self):
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", processing_delay_ms=-1)

        assert "processing_delay_ms cannot be negative" in str(exc_info.value)

    def test_invalid_auto_offset_reset(self):
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", auto_offset_reset="none")

        assert "auto_offset_reset must be" in str(exc_info.value)
