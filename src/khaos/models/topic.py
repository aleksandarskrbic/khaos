"""Topic configuration model."""

from dataclasses import dataclass


@dataclass
class TopicConfig:
    """Configuration for a Kafka topic."""

    name: str
    partitions: int = 6
    replication_factor: int = 3
    retention_ms: int = 604800000  # 7 days (in milliseconds)

    def __post_init__(self):
        if self.partitions < 1:
            raise ValueError("partitions must be at least 1")
        if self.replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
