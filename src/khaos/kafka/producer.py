from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from confluent_kafka import Producer

from khaos.errors import KhaosConnectionError, format_kafka_error
from khaos.models.config import ProducerConfig
from khaos.runtime import get_executor

if TYPE_CHECKING:
    from khaos.models.cluster import ClusterConfig


@dataclass
class ProducerStats:
    messages_sent: int = 0
    bytes_sent: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_success(self, size: int) -> None:
        with self._lock:
            self.messages_sent += 1
            self.bytes_sent += size

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1


class ProducerSimulator:
    def __init__(
        self,
        bootstrap_servers: str,
        config: ProducerConfig | None = None,
        cluster_config: ClusterConfig | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.config = config or ProducerConfig()
        self.cluster_config = cluster_config
        self.stats = ProducerStats()
        self._stop_event = threading.Event()

        # Build producer config
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "batch.size": self.config.batch_size,
            "linger.ms": self.config.linger_ms,
            "acks": self.config.acks,
            "compression.type": self.config.compression_type,
            "log_level": 0,  # Disable librdkafka logging to stderr
            "logger": lambda *args: None,  # Silent logger callback
        }

        # Merge security configuration if provided
        if cluster_config:
            security_config = cluster_config.to_kafka_config()
            security_config.pop("bootstrap.servers", None)
            producer_config.update(security_config)

        try:
            self._producer = Producer(producer_config)
        except Exception as e:
            raise KhaosConnectionError(
                f"Failed to create producer for {bootstrap_servers}: {format_kafka_error(e)}"
            )

    def _delivery_callback(self, err, msg) -> None:
        if err:
            self.stats.record_error()
        else:
            self.stats.record_success(len(msg.value()) if msg.value() else 0)

    def _produce_sync(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
    ) -> None:
        kwargs = {
            "topic": topic,
            "value": value,
            "callback": self._delivery_callback,
        }
        if key is not None:
            kwargs["key"] = key

        self._producer.produce(**kwargs)
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        result: int = self._producer.flush(timeout)
        return result

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    async def produce_at_rate(
        self,
        topic: str,
        message_generator,
        key_generator=None,
        duration_seconds: int = 60,
    ) -> None:
        messages_per_second = self.config.messages_per_second
        interval = 1.0 / messages_per_second if messages_per_second > 0 else 0

        loop = asyncio.get_event_loop()
        executor = get_executor()

        start_time = time.time()
        message_count = 0

        while not self.should_stop:
            if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                break

            value = message_generator.generate()
            key = key_generator.generate() if key_generator else None

            await loop.run_in_executor(executor, self._produce_sync, topic, value, key)
            message_count += 1

            if interval > 0:
                expected_time = start_time + (message_count * interval)
                sleep_time = expected_time - time.time()
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        self.flush()

    @property
    def messages_per_second(self) -> float:
        return self.config.messages_per_second

    @messages_per_second.setter
    def messages_per_second(self, value: float) -> None:
        self.config.messages_per_second = value

    def get_stats(self) -> ProducerStats:
        return self.stats
