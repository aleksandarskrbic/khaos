from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from confluent_kafka import Producer

from khaos.defaults import FLUSH_TIMEOUT_SECONDS
from khaos.errors import KhaosConnectionError, format_kafka_error
from khaos.kafka.config import build_kafka_config
from khaos.kafka.simulator import Simulator, SimulatorStats
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ProducerConfig


@dataclass
class ProducerStats(SimulatorStats):
    messages_sent: int = 0
    bytes_sent: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_success(self, size: int) -> None:
        with self._lock:
            self.messages_sent += 1
            self.bytes_sent += size

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1


class ProducerSimulator(Simulator[ProducerStats]):
    def __init__(
        self,
        bootstrap_servers: str,
        executor: ThreadPoolExecutor,
        config: ProducerConfig | None = None,
        cluster_config: ClusterConfig | None = None,
    ):
        super().__init__(executor)
        self.bootstrap_servers = bootstrap_servers
        self.config = config or ProducerConfig()
        self.cluster_config = cluster_config
        self.stats = ProducerStats()

        producer_config = build_kafka_config(
            bootstrap_servers,
            cluster_config,
            **{
                "batch.size": self.config.batch_size,
                "linger.ms": self.config.linger_ms,
                "acks": self.config.acks,
                "compression.type": self.config.compression_type,
            },
        )

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

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> int:
        result: int = self._producer.flush(timeout)
        return result

    async def produce_at_rate(
        self,
        topic: str,
        message_generator,
        key_generator=None,
        duration_seconds: int = 60,
    ) -> None:
        messages_per_second = self.config.messages_per_second
        interval = 1.0 / messages_per_second if messages_per_second > 0 else 0

        loop = asyncio.get_running_loop()

        start_time = time.time()
        message_count = 0

        while not self.should_stop:
            if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                break

            value = message_generator.generate()
            key = key_generator.generate() if key_generator else None

            await loop.run_in_executor(self._executor, self._produce_sync, topic, value, key)
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
