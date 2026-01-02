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
        assert config.duplicate_rate == 0.0

    def test_custom_values(self):
        config = ProducerConfig(
            messages_per_second=500.0,
            batch_size=32768,
            linger_ms=10,
            acks="1",
            compression_type="lz4",
            duplicate_rate=0.1,
        )

        assert config.messages_per_second == 500.0
        assert config.batch_size == 32768
        assert config.linger_ms == 10
        assert config.acks == "1"
        assert config.compression_type == "lz4"
        assert config.duplicate_rate == 0.1

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

    def test_duplicate_rate_validation_bounds(self):
        ProducerConfig(duplicate_rate=0.0)
        ProducerConfig(duplicate_rate=0.5)
        ProducerConfig(duplicate_rate=1.0)

        with pytest.raises(ValueError) as exc_info:
            ProducerConfig(duplicate_rate=-0.1)
        assert "duplicate_rate must be between 0.0 and 1.0" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            ProducerConfig(duplicate_rate=1.5)
        assert "duplicate_rate must be between 0.0 and 1.0" in str(exc_info.value)


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

    # Failure simulation tests
    def test_failure_simulation_default_values(self):
        config = ConsumerConfig(group_id="test-group")

        assert config.failure_rate == 0.0
        assert config.commit_failure_rate == 0.0
        assert config.on_failure == "skip"
        assert config.max_retries == 3
        assert config.failure_simulation_enabled is False

    def test_failure_simulation_enabled_with_failure_rate(self):
        config = ConsumerConfig(group_id="test-group", failure_rate=0.1)

        assert config.failure_simulation_enabled is True

    def test_failure_simulation_enabled_with_commit_failure_rate(self):
        config = ConsumerConfig(group_id="test-group", commit_failure_rate=0.05)

        assert config.failure_simulation_enabled is True

    def test_failure_rate_validation_bounds(self):
        # Valid values
        ConsumerConfig(group_id="test", failure_rate=0.0)
        ConsumerConfig(group_id="test", failure_rate=0.5)
        ConsumerConfig(group_id="test", failure_rate=1.0)

        # Invalid: below 0
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", failure_rate=-0.1)
        assert "failure_rate must be between 0.0 and 1.0" in str(exc_info.value)

        # Invalid: above 1
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", failure_rate=1.1)
        assert "failure_rate must be between 0.0 and 1.0" in str(exc_info.value)

    def test_commit_failure_rate_validation_bounds(self):
        # Valid values
        ConsumerConfig(group_id="test", commit_failure_rate=0.0)
        ConsumerConfig(group_id="test", commit_failure_rate=0.5)
        ConsumerConfig(group_id="test", commit_failure_rate=1.0)

        # Invalid: below 0
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", commit_failure_rate=-0.1)
        assert "commit_failure_rate must be between 0.0 and 1.0" in str(exc_info.value)

        # Invalid: above 1
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", commit_failure_rate=1.5)
        assert "commit_failure_rate must be between 0.0 and 1.0" in str(exc_info.value)

    def test_on_failure_validation(self):
        # Valid values
        ConsumerConfig(group_id="test", on_failure="skip")
        ConsumerConfig(group_id="test", on_failure="dlq")
        ConsumerConfig(group_id="test", on_failure="retry")

        # Invalid value
        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", on_failure="invalid")  # type: ignore[arg-type]
        assert "on_failure must be 'skip', 'dlq', or 'retry'" in str(exc_info.value)

    def test_max_retries_validation(self):
        ConsumerConfig(group_id="test", max_retries=0)
        ConsumerConfig(group_id="test", max_retries=5)

        with pytest.raises(ValueError) as exc_info:
            ConsumerConfig(group_id="test", max_retries=-1)
        assert "max_retries cannot be negative" in str(exc_info.value)

    def test_full_failure_simulation_config(self):
        config = ConsumerConfig(
            group_id="test-group",
            failure_rate=0.1,
            commit_failure_rate=0.05,
            on_failure="dlq",
            max_retries=5,
        )

        assert config.failure_rate == 0.1
        assert config.commit_failure_rate == 0.05
        assert config.on_failure == "dlq"
        assert config.max_retries == 5
        assert config.failure_simulation_enabled is True
