from dataclasses import dataclass

from khaos.defaults import (
    DEFAULT_ACKS,
    DEFAULT_AUTO_OFFSET_RESET,
    DEFAULT_BATCH_SIZE,
    DEFAULT_COMPRESSION_TYPE,
    DEFAULT_LINGER_MS,
    DEFAULT_MAX_POLL_RECORDS,
    DEFAULT_MESSAGES_PER_SECOND,
    DEFAULT_PROCESSING_DELAY_MS,
    DEFAULT_SESSION_TIMEOUT_MS,
)


@dataclass
class ProducerConfig:
    messages_per_second: float = DEFAULT_MESSAGES_PER_SECOND
    batch_size: int = DEFAULT_BATCH_SIZE
    linger_ms: int = DEFAULT_LINGER_MS
    acks: str = DEFAULT_ACKS
    compression_type: str = DEFAULT_COMPRESSION_TYPE

    def __post_init__(self):
        if self.messages_per_second <= 0:
            raise ValueError("messages_per_second must be positive")
        if self.acks not in ("0", "1", "all"):
            raise ValueError("acks must be '0', '1', or 'all'")
        if self.compression_type not in ("none", "gzip", "snappy", "lz4", "zstd"):
            raise ValueError("Invalid compression_type")


@dataclass
class ConsumerConfig:
    group_id: str
    processing_delay_ms: int = DEFAULT_PROCESSING_DELAY_MS
    max_poll_records: int = DEFAULT_MAX_POLL_RECORDS
    auto_offset_reset: str = DEFAULT_AUTO_OFFSET_RESET
    session_timeout_ms: int = DEFAULT_SESSION_TIMEOUT_MS

    def __post_init__(self):
        if self.processing_delay_ms < 0:
            raise ValueError("processing_delay_ms cannot be negative")
        if self.auto_offset_reset not in ("earliest", "latest"):
            raise ValueError("auto_offset_reset must be 'earliest' or 'latest'")
