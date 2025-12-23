from dataclasses import dataclass


@dataclass
class ProducerConfig:
    messages_per_second: float = 1000.0
    batch_size: int = 16384  # 16KB
    linger_ms: int = 5
    acks: str = "all"  # "0", "1", or "all"
    compression_type: str = "none"  # "none", "gzip", "snappy", "lz4", "zstd"

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
    processing_delay_ms: int = 0  # Simulated processing time
    max_poll_records: int = 500
    auto_offset_reset: str = "earliest"  # "earliest", "latest"
    session_timeout_ms: int = 45000

    def __post_init__(self):
        if self.processing_delay_ms < 0:
            raise ValueError("processing_delay_ms cannot be negative")
        if self.auto_offset_reset not in ("earliest", "latest"):
            raise ValueError("auto_offset_reset must be 'earliest' or 'latest'")
