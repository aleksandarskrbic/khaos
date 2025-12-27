from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaError

from khaos.defaults import (
    DEFAULT_AUTO_COMMIT_INTERVAL_MS,
    DEFAULT_MAX_POLL_INTERVAL_MS,
)
from khaos.errors import KhaosConnectionError, format_kafka_error
from khaos.kafka.config import build_kafka_config
from khaos.kafka.simulator import Simulator, SimulatorStats
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ConsumerConfig

logger = logging.getLogger(__name__)


@dataclass
class ConsumerStats(SimulatorStats):
    messages_consumed: int = 0
    bytes_consumed: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_message(self, size: int) -> None:
        with self._lock:
            self.messages_consumed += 1
            self.bytes_consumed += size

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1


class ConsumerSimulator(Simulator[ConsumerStats]):
    def __init__(
        self,
        bootstrap_servers: str,
        topics: list[str],
        config: ConsumerConfig,
        executor: ThreadPoolExecutor,
        cluster_config: ClusterConfig | None = None,
    ):
        super().__init__(executor)
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.config = config
        self.cluster_config = cluster_config
        self.stats = ConsumerStats()

        kafka_config = build_kafka_config(
            bootstrap_servers,
            cluster_config,
            **{
                "group.id": config.group_id,
                "auto.offset.reset": config.auto_offset_reset,
                "enable.auto.commit": True,
                "auto.commit.interval.ms": DEFAULT_AUTO_COMMIT_INTERVAL_MS,
                "max.poll.interval.ms": DEFAULT_MAX_POLL_INTERVAL_MS,
                "session.timeout.ms": config.session_timeout_ms,
            },
        )

        try:
            self._consumer = Consumer(kafka_config)
            self._consumer.subscribe(topics)
        except Exception as e:
            raise KhaosConnectionError(
                f"Failed to create consumer for {bootstrap_servers}: {format_kafka_error(e)}"
            )

    def _poll_sync(self, timeout: float = 0.1):
        return self._consumer.poll(timeout)

    async def consume_loop(
        self,
        duration_seconds: int = 60,
        on_message=None,
    ):
        """
        Run the consume loop.

        Args:
            duration_seconds: How long to run. Use 0 for infinite (until stop() is called).
            on_message: Optional callback invoked for each message.
        """
        start_time = time.time()
        loop = asyncio.get_running_loop()

        while not self.should_stop:
            if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                break

            msg = await loop.run_in_executor(self._executor, self._poll_sync, 0.1)

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
            if self.config.processing_delay_ms > 0:
                await asyncio.sleep(self.config.processing_delay_ms / 1000.0)

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as e:
            logger.debug(f"Error closing consumer: {e}")

    def get_stats(self) -> ConsumerStats:
        return self.stats
