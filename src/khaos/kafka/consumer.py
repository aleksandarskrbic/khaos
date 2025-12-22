"""Kafka Consumer wrapper with processing delay simulation."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError

from khaos.errors import KhaosConnectionError, format_kafka_error
from khaos.runtime import get_executor

if TYPE_CHECKING:
    from khaos.models.cluster import ClusterConfig


@dataclass
class ConsumerStats:
    """Statistics for consumer."""

    messages_consumed: int = 0
    bytes_consumed: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_message(self, size: int) -> None:
        with self._lock:
            self.messages_consumed += 1
            self.bytes_consumed += size

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1


class ConsumerSimulator:
    """Kafka consumer with processing delay simulation."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        processing_delay_ms: int = 0,
        auto_offset_reset: str = "earliest",
        max_poll_records: int = 500,
        cluster_config: ClusterConfig | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.processing_delay_ms = processing_delay_ms
        self.cluster_config = cluster_config
        self.stats = ConsumerStats()
        self._stop_event = threading.Event()

        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
            "log_level": 0,  # Disable librdkafka logging to stderr
            "logger": lambda *args: None,  # Silent logger callback
        }

        # Merge security configuration if provided
        if cluster_config:
            security_config = cluster_config.to_kafka_config()
            security_config.pop("bootstrap.servers", None)
            config.update(security_config)

        try:
            self._consumer = Consumer(config)
            self._consumer.subscribe(topics)
        except Exception as e:
            raise KhaosConnectionError(
                f"Failed to create consumer for {bootstrap_servers}: {format_kafka_error(e)}"
            )

    def _poll_sync(self, timeout: float = 0.1):
        """Synchronous poll - runs in thread pool."""
        return self._consumer.poll(timeout)

    def stop(self) -> None:
        """Signal consumer to stop."""
        self._stop_event.set()

    @property
    def should_stop(self) -> bool:
        """Check if consumer should stop."""
        return self._stop_event.is_set()

    async def consume_loop(
        self,
        duration_seconds: int = 60,
        on_message=None,
    ) -> None:
        """Consume messages in a loop using thread pool for blocking poll()."""
        start_time = time.time()
        loop = asyncio.get_event_loop()
        executor = get_executor()

        try:
            while not self.should_stop:
                if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                    break

                msg = await loop.run_in_executor(executor, self._poll_sync, 0.1)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.stats.record_error()
                    continue

                value_size = len(msg.value()) if msg.value() else 0
                self.stats.record_message(value_size)

                if on_message:
                    on_message(msg)

                # Simulate processing delay
                if self.processing_delay_ms > 0:
                    await asyncio.sleep(self.processing_delay_ms / 1000.0)

        finally:
            self.close()

    def close(self) -> None:
        """Close the consumer."""
        try:
            self._consumer.close()
        except Exception:
            pass

    def get_stats(self) -> ConsumerStats:
        """Get current statistics."""
        return self.stats
