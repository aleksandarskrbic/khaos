from __future__ import annotations

import asyncio
import logging
import random
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
from khaos.kafka.dlq import DLQProducer
from khaos.kafka.simulator import Simulator, SimulatorStats
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ConsumerConfig

logger = logging.getLogger(__name__)


@dataclass
class ConsumerStats(SimulatorStats):
    messages_consumed: int = 0
    bytes_consumed: int = 0
    errors: int = 0
    simulated_failures: int = 0
    dlq_sent: int = 0
    retries: int = 0
    commit_failures: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_message(self, size: int) -> None:
        with self._lock:
            self.messages_consumed += 1
            self.bytes_consumed += size

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    def record_simulated_failure(self) -> None:
        with self._lock:
            self.simulated_failures += 1

    def record_dlq_sent(self) -> None:
        with self._lock:
            self.dlq_sent += 1

    def record_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def record_commit_failure(self) -> None:
        with self._lock:
            self.commit_failures += 1


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
        self._dlq_producer: DLQProducer | None = None

        # Disable auto-commit when failure simulation is enabled
        enable_auto_commit = not config.failure_simulation_enabled

        kafka_config = build_kafka_config(
            bootstrap_servers,
            cluster_config,
            **{
                "group.id": config.group_id,
                "auto.offset.reset": config.auto_offset_reset,
                "enable.auto.commit": enable_auto_commit,
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

    def _get_dlq_producer(self) -> DLQProducer:
        """Lazily initialize the DLQ producer."""
        if self._dlq_producer is None:
            self._dlq_producer = DLQProducer(
                bootstrap_servers=self.bootstrap_servers,
                cluster_config=self.cluster_config,
            )
        return self._dlq_producer

    def _should_fail(self) -> bool:
        """Check if processing should fail based on failure_rate."""
        return random.random() < self.config.failure_rate

    def _should_commit_fail(self) -> bool:
        """Check if commit should fail based on commit_failure_rate."""
        return random.random() < self.config.commit_failure_rate

    def _handle_failure(self, msg, retry_count: int) -> bool:
        """Handle a simulated processing failure.

        Args:
            msg: The Kafka message that failed
            retry_count: Current retry attempt number

        Returns:
            True if message should be retried, False otherwise
        """
        self.stats.record_simulated_failure()

        if self.config.on_failure == "skip":
            logger.debug(f"Skipping failed message at offset {msg.offset()}")
            return False

        if self.config.on_failure == "dlq":
            dlq_producer = self._get_dlq_producer()
            dlq_producer.send_to_dlq(msg, "simulated_processing_failure")
            self.stats.record_dlq_sent()
            logger.debug(f"Sent message at offset {msg.offset()} to DLQ")
            return False

        if self.config.on_failure == "retry":
            if retry_count < self.config.max_retries:
                self.stats.record_retry()
                logger.debug(
                    f"Retrying message at offset {msg.offset()} "
                    f"(attempt {retry_count + 1}/{self.config.max_retries})"
                )
                return True
            logger.debug(
                f"Max retries ({self.config.max_retries}) exceeded "
                f"for message at offset {msg.offset()}"
            )
            return False

        return False

    def _try_commit(self, msg) -> bool:
        """Attempt to commit the message offset with failure simulation.

        Returns:
            True if commit succeeded, False if it failed
        """
        if self._should_commit_fail():
            self.stats.record_commit_failure()
            logger.debug(f"Simulated commit failure for offset {msg.offset()}")
            return False

        try:
            self._consumer.commit(msg)
            return True
        except Exception as e:
            logger.debug(f"Commit failed: {e}")
            self.stats.record_commit_failure()
            return False

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

            # Failure simulation logic
            if self.config.failure_simulation_enabled:
                retry_count = 0
                while True:
                    if self._should_fail():
                        should_retry = self._handle_failure(msg, retry_count)
                        if should_retry:
                            retry_count += 1
                            continue
                        # Failed and not retrying - skip message processing
                        break
                    else:
                        # Processing succeeded
                        value_size = len(msg.value()) if msg.value() else 0
                        self.stats.record_message(value_size)

                        if on_message:
                            on_message(msg)

                        # Manual commit
                        self._try_commit(msg)
                        break
            else:
                # Normal mode (no failure simulation)
                value_size = len(msg.value()) if msg.value() else 0
                self.stats.record_message(value_size)

                if on_message:
                    on_message(msg)

            # Simulate processing delay
            if self.config.processing_delay_ms > 0:
                await asyncio.sleep(self.config.processing_delay_ms / 1000.0)

    def close(self) -> None:
        try:
            if self._dlq_producer is not None:
                self._dlq_producer.close()
            self._consumer.close()
        except Exception as e:
            logger.debug(f"Error closing consumer: {e}")

    def get_stats(self) -> ConsumerStats:
        return self.stats
