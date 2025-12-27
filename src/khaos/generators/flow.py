from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Producer

from khaos.defaults import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_FLOW_ACKS,
    DEFAULT_FLOW_COMPRESSION,
    DEFAULT_LINGER_MS,
    FLUSH_TIMEOUT_SECONDS,
)
from khaos.errors import KhaosConnectionError, format_kafka_error
from khaos.generators.field import create_field_generator
from khaos.kafka.config import build_kafka_config
from khaos.kafka.simulator import Simulator, SimulatorStats
from khaos.models.cluster import ClusterConfig
from khaos.models.flow import FlowConfig, FlowStep


@dataclass
class FlowStats(SimulatorStats):
    flows_completed: int = 0
    messages_sent: int = 0
    messages_per_topic: dict[str, int] = field(default_factory=dict)
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_flow_complete(self) -> None:
        with self._lock:
            self.flows_completed += 1

    def record_message(self, topic: str) -> None:
        with self._lock:
            self.messages_sent += 1
            self.messages_per_topic[topic] = self.messages_per_topic.get(topic, 0) + 1

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    def get_topic_count(self, topic: str) -> int:
        with self._lock:
            return self.messages_per_topic.get(topic, 0)


class FlowProducer(Simulator[FlowStats]):
    def __init__(
        self,
        flow: FlowConfig,
        bootstrap_servers: str,
        executor: ThreadPoolExecutor,
        cluster_config: ClusterConfig | None = None,
    ):
        super().__init__(executor)
        self.flow = flow
        self.bootstrap_servers = bootstrap_servers
        self.cluster_config = cluster_config
        self.stats = FlowStats()

        producer_config = build_kafka_config(
            bootstrap_servers,
            cluster_config,
            **{
                "batch.size": DEFAULT_BATCH_SIZE,
                "linger.ms": DEFAULT_LINGER_MS,
                "acks": DEFAULT_FLOW_ACKS,
                "compression.type": DEFAULT_FLOW_COMPRESSION,
            },
        )

        try:
            self._producer = Producer(producer_config)
        except Exception as e:
            raise KhaosConnectionError(
                f"Failed to create producer for {bootstrap_servers}: {format_kafka_error(e)}"
            )

        # Pre-create field generators for each step
        self._step_generators: list[list[tuple[str, Any]]] = []
        for step in flow.steps:
            if step.fields:
                generators = [(f.name, create_field_generator(f)) for f in step.fields]
            else:
                generators = []
            self._step_generators.append(generators)

    def _delivery_callback(self, err, msg) -> None:
        if err:
            self.stats.record_error()
        else:
            self.stats.record_message(msg.topic())

    def _produce_sync(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        kwargs = {
            "topic": topic,
            "value": value,
            "callback": self._delivery_callback,
        }
        if key is not None:
            kwargs["key"] = key

        self._producer.produce(**kwargs)
        self._producer.poll(0)

    def _generate_correlation_id(self, first_step_data: dict[str, Any] | None = None) -> str:
        if self.flow.correlation.type == "field_ref" and first_step_data:
            field_name = self.flow.correlation.field
            if field_name and field_name in first_step_data:
                return str(first_step_data[field_name])
        return str(uuid.uuid4())

    def _generate_step_message(
        self,
        step_index: int,
        step: FlowStep,
        correlation_id: str,
    ) -> tuple[bytes, dict[str, Any]]:
        message: dict[str, Any] = {
            "correlation_id": correlation_id,
            "event_type": step.event_type,
        }

        for field_name, generator in self._step_generators[step_index]:
            message[field_name] = generator.generate()

        return json.dumps(message).encode(), message

    async def produce_flow_instance(self) -> None:
        loop = asyncio.get_running_loop()

        first_step_data: dict[str, Any] | None = None
        correlation_id: str | None = None

        for i, step in enumerate(self.flow.steps):
            # Apply delay (except for first step)
            if i > 0 and step.delay_ms > 0:
                await asyncio.sleep(step.delay_ms / 1000)

            if self.should_stop:
                return

            # Generate correlation ID from first step if using field_ref
            if i == 0:
                value_bytes, first_step_data = self._generate_step_message(i, step, "")
                correlation_id = self._generate_correlation_id(first_step_data)
                # Regenerate with actual correlation ID
                first_step_data["correlation_id"] = correlation_id
                value_bytes = json.dumps(first_step_data).encode()
            else:
                value_bytes, _ = self._generate_step_message(i, step, correlation_id or "")

            # Use correlation_id as key for partitioning
            key = correlation_id.encode() if correlation_id else None

            await loop.run_in_executor(
                self._executor, self._produce_sync, step.topic, value_bytes, key
            )

        self.stats.record_flow_complete()

    async def run_at_rate(self, duration_seconds: int = 0) -> None:
        rate = self.flow.rate
        interval = 1.0 / rate if rate > 0 else 0

        start_time = time.time()
        flow_count = 0

        while not self.should_stop:
            if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                break

            # Start a flow instance (fire and forget - runs with delays)
            asyncio.create_task(self.produce_flow_instance())
            flow_count += 1

            if interval > 0:
                expected_time = start_time + (flow_count * interval)
                sleep_time = expected_time - time.time()
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        # Wait a bit for in-flight flows to complete
        max_delay = max((step.delay_ms for step in self.flow.steps), default=0)
        if max_delay > 0:
            await asyncio.sleep(max_delay / 1000 + 0.5)

        self.flush()

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> int:
        result: int = self._producer.flush(timeout)
        return result

    def get_stats(self) -> FlowStats:
        return self.stats
