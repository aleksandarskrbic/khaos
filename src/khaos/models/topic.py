from dataclasses import dataclass

from khaos.defaults import (
    DEFAULT_PARTITIONS,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_RETENTION_MS,
)


@dataclass
class TopicConfig:
    name: str
    partitions: int = DEFAULT_PARTITIONS
    replication_factor: int = DEFAULT_REPLICATION_FACTOR
    retention_ms: int = DEFAULT_RETENTION_MS

    def __post_init__(self):
        if self.partitions < 1:
            raise ValueError("partitions must be at least 1")
        if self.replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
