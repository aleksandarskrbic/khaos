"""Base executor with common functionality."""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor

from rich.console import Console
from rich.live import Live

from khaos.defaults import DEFAULT_EXECUTOR_WORKERS, FLUSH_TIMEOUT_SECONDS
from khaos.executor.incident_scheduler import IncidentScheduler
from khaos.executor.result import ExecutionResult
from khaos.executor.serializer import SerializerFactory
from khaos.executor.simulator_factory import SimulatorFactory
from khaos.executor.stats import StatsDisplay
from khaos.executor.topic_manager import TopicManager
from khaos.generators.flow import FlowProducer
from khaos.generators.key import create_key_generator
from khaos.generators.payload import create_payload_generator
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.flow import FlowConfig, StepConsumerConfig
from khaos.models.message import KeyDistribution, MessageSchema
from khaos.scenarios.incidents import Incident, IncidentGroup
from khaos.scenarios.scenario import Scenario, TopicConfig
from khaos.serialization import SchemaRegistryProvider

console = Console()
logger = logging.getLogger(__name__)


class TaskRunner:
    """Collects async tasks and runs them with error handling."""

    def __init__(self, result: ExecutionResult):
        self._tasks: list[Coroutine] = []
        self._result = result

    def add(
        self,
        coroutine: Coroutine,
        context: str | None = None,
        cleanup: Callable[[], None] | None = None,
    ) -> None:
        if context is None and cleanup is None:
            self._tasks.append(coroutine)
            return

        async def wrapped():
            try:
                await coroutine
            except Exception as e:
                if context:
                    logger.exception(f"{context}: {e}")
                    self._result.add_error(f"{context}: {e}")
            finally:
                if cleanup:
                    cleanup()

        self._tasks.append(wrapped())

    async def run(self) -> None:
        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass


class BaseExecutor(ABC):
    def __init__(
        self,
        bootstrap_servers: str,
        scenarios: list[Scenario],
        no_consumers: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.scenarios = scenarios
        self.no_consumers = no_consumers

        self._executor = ThreadPoolExecutor(
            max_workers=DEFAULT_EXECUTOR_WORKERS, thread_name_prefix="khaos"
        )
        self.topic_manager = TopicManager(bootstrap_servers)
        self.simulator_factory = SimulatorFactory(
            bootstrap_servers=self.bootstrap_servers,
            executor=self._executor,
        )

        self._stop_event = asyncio.Event()
        self.producers: list[ProducerSimulator] = []
        self.consumers: list[ConsumerSimulator] = []
        self.flow_producers: list[FlowProducer] = []

        # Per-topic tracking for accurate stats
        self._producers_by_topic: dict[str, list[ProducerSimulator]] = {}
        self._consumers_by_topic: dict[str, dict[str, list[ConsumerSimulator]]] = {}
        self._consumers_by_group: dict[str, list[ConsumerSimulator]] = {}
        self._consumers_by_topic_flat: dict[str, list[ConsumerSimulator]] = {}
        self._topic_to_scenario: dict[str, str] = {}
        self._flow_producers_by_name: dict[str, FlowProducer] = {}

        self._all_topics: list[TopicConfig] = []
        self._all_incidents: list[Incident] = []
        self._all_incident_groups: list[IncidentGroup] = []
        self._all_flows: list[FlowConfig] = []

        for scenario in scenarios:
            for topic in scenario.topics:
                self._topic_to_scenario[topic.name] = scenario.name
            self._all_topics.extend(scenario.topics)
            self._all_incidents.extend(scenario.incidents)
            self._all_incident_groups.extend(scenario.incident_groups)
            self._all_flows.extend(scenario.flows)

    @abstractmethod
    def _is_schema_registry_running(self) -> bool: ...

    @abstractmethod
    async def _handle_stop_broker(self, broker: str) -> None: ...

    @abstractmethod
    async def _handle_start_broker(self, broker: str) -> None: ...

    def _create_serializer_factory(self) -> SerializerFactory:
        return SerializerFactory(
            scenarios=self.scenarios,
            is_schema_registry_running_fn=self._is_schema_registry_running,
        )

    def _create_stats_display(self) -> StatsDisplay:
        return StatsDisplay(
            scenario_names=[s.name for s in self.scenarios],
            topics=self._all_topics,
            producers=self.producers,
            consumers=self.consumers,
            flow_producers=self.flow_producers,
            producers_by_topic=self._producers_by_topic,
            consumers_by_topic=self._consumers_by_topic,
        )

    def _create_incident_scheduler(self) -> IncidentScheduler:
        return IncidentScheduler(
            consumers=self.consumers,
            producers=self.producers,
            bootstrap_servers=self.bootstrap_servers,
            consumers_by_topic=self._consumers_by_topic_flat,
            consumers_by_group=self._consumers_by_group,
            producers_by_topic=self._producers_by_topic,
            create_consumer_fn=self._create_single_consumer,
            handle_stop_broker_fn=self._handle_stop_broker,
            handle_start_broker_fn=self._handle_start_broker,
            should_stop_fn=lambda: self.should_stop,
            console=console,
        )

    async def setup(self) -> None:
        await self.topic_manager.setup_topics(self._all_topics, self._all_flows)

    async def teardown(self) -> None:
        for producer in self.producers:
            producer.stop()
            producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)

        for flow_producer in self.flow_producers:
            flow_producer.stop()
            flow_producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)

        for consumer in self.consumers:
            consumer.stop()
            consumer.close()

        self._executor.shutdown(wait=True)

    def request_stop(self) -> None:
        self._stop_event.set()
        for producer in self.producers:
            producer.stop()
        for flow_producer in self.flow_producers:
            flow_producer.stop()
        for consumer in self.consumers:
            consumer.stop()

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def _create_single_consumer(
        self,
        group_id: str,
        topics: list[str],
        processing_delay_ms: int,
    ) -> ConsumerSimulator:
        from khaos.models.config import ConsumerConfig

        config = ConsumerConfig(group_id=group_id, processing_delay_ms=processing_delay_ms)
        return self.simulator_factory.create_consumer(topics=topics, config=config)

    def _register_producer(self, topic_name: str, producer: ProducerSimulator) -> None:
        self.producers.append(producer)
        if topic_name not in self._producers_by_topic:
            self._producers_by_topic[topic_name] = []
        self._producers_by_topic[topic_name].append(producer)

    def _register_consumer(
        self, topic_name: str, group_id: str, consumer: ConsumerSimulator
    ) -> None:
        self.consumers.append(consumer)

        if topic_name not in self._consumers_by_topic:
            self._consumers_by_topic[topic_name] = {}

        if group_id not in self._consumers_by_topic[topic_name]:
            self._consumers_by_topic[topic_name][group_id] = []

        self._consumers_by_topic[topic_name][group_id].append(consumer)

        if topic_name not in self._consumers_by_topic_flat:
            self._consumers_by_topic_flat[topic_name] = []

        self._consumers_by_topic_flat[topic_name].append(consumer)

        if group_id not in self._consumers_by_group:
            self._consumers_by_group[group_id] = []

        self._consumers_by_group[group_id].append(consumer)

    def _create_producers_for_topic(self, topic: TopicConfig) -> list[ProducerSimulator]:
        producers = []
        for _, producer in self.simulator_factory.create_producers_for_topic(topic):
            self._register_producer(topic.name, producer)
            producers.append(producer)
        return producers

    def _create_consumers_for_topic(self, topic: TopicConfig) -> list[ConsumerSimulator]:
        consumers = []
        for group_id, _, consumer in self.simulator_factory.create_consumers_for_topic(topic):
            self._register_consumer(topic.name, group_id, consumer)
            consumers.append(consumer)
        return consumers

    def _create_flow_step_consumers(
        self,
        flow_name: str,
        topic_name: str,
        consumers: StepConsumerConfig,
        duration_seconds: int,
        runner: TaskRunner,
    ) -> None:
        for g in range(consumers.groups):
            group_id = f"{flow_name}-{topic_name}-group-{g + 1}"

            for _ in range(consumers.per_group):
                consumer = self._create_single_consumer(
                    group_id=group_id,
                    topics=[topic_name],
                    processing_delay_ms=consumers.delay_ms,
                )
                self._register_consumer(topic_name, group_id, consumer)

                runner.add(
                    consumer.consume_loop(duration_seconds=duration_seconds),
                    f"Flow consumer error for group '{group_id}'",
                )

    async def _run(self, duration_seconds: int) -> ExecutionResult:
        result = ExecutionResult()
        start_time = time.time()
        runner = TaskRunner(result)

        serializer_factory = self._create_serializer_factory()
        incident_scheduler = self._create_incident_scheduler()

        # Create schema registry provider for caching (if needed)
        schema_registry_provider: SchemaRegistryProvider | None = None
        schema_registry_config = serializer_factory.get_schema_registry_config()
        if schema_registry_config:
            schema_registry_provider = SchemaRegistryProvider(schema_registry_config.url)

        for topic in self._all_topics:
            fields = topic.message_schema.fields
            data_format = topic.message_schema.data_format
            raw_avro_schema: dict | None = None

            # Fetch schema from registry if using registry provider
            if topic.schema_provider == "registry" and topic.subject_name:
                if not schema_registry_provider:
                    result.add_error(
                        f"Topic '{topic.name}' uses registry provider but no schema_registry_config"
                    )
                    continue

                try:
                    console.print(
                        f"[dim]Fetching schema for '{topic.subject_name}' from registry...[/dim]"
                    )
                    data_format, fields = schema_registry_provider.get_field_schemas(
                        topic.subject_name
                    )
                    _, raw_schema = schema_registry_provider.get_raw_schema(topic.subject_name)
                    if data_format == "avro" and isinstance(raw_schema, dict):
                        raw_avro_schema = raw_schema
                    console.print(
                        f"[dim]Loaded {len(fields)} fields from {data_format.upper()} schema[/dim]"
                    )
                except Exception as e:
                    result.add_error(f"Failed to fetch schema for '{topic.subject_name}': {e}")
                    continue

            msg_schema = MessageSchema(
                min_size_bytes=topic.message_schema.min_size_bytes,
                max_size_bytes=topic.message_schema.max_size_bytes,
                key_distribution=KeyDistribution.from_string(topic.message_schema.key_distribution),
                key_cardinality=topic.message_schema.key_cardinality,
                fields=fields,
            )

            serializer = serializer_factory.create_serializer_with_format(
                topic, data_format, fields, raw_avro_schema
            )

            producers = self._create_producers_for_topic(topic)
            key_gen = create_key_generator(msg_schema)
            payload_gen = create_payload_generator(msg_schema, serializer=serializer)

            for producer in producers:
                runner.add(
                    producer.produce_at_rate(
                        topic=topic.name,
                        message_generator=payload_gen,
                        key_generator=key_gen,
                        duration_seconds=duration_seconds,
                    ),
                    f"Producer error for topic '{topic.name}'",
                    cleanup=lambda p=producer: p.flush(),
                )

            # Consumers (skip if no_consumers mode)
            if not self.no_consumers:
                consumers = self._create_consumers_for_topic(topic)
                for consumer in consumers:
                    runner.add(
                        consumer.consume_loop(duration_seconds=duration_seconds),
                        "Consumer error",
                    )

        # Create flow producers and their consumers
        for flow in self._all_flows:
            flow_producer = self.simulator_factory.create_flow_producer(flow)
            self.flow_producers.append(flow_producer)
            self._flow_producers_by_name[flow.name] = flow_producer

            runner.add(
                flow_producer.run_at_rate(duration_seconds=duration_seconds),
                f"Flow producer error for '{flow.name}'",
                cleanup=lambda fp=flow_producer: fp.flush(),
            )

            # Create consumers for flow steps
            if not self.no_consumers:
                for step in flow.steps:
                    if step.consumers:
                        self._create_flow_step_consumers(
                            flow.name, step.topic, step.consumers, duration_seconds, runner
                        )

        for incident in self._all_incidents:
            runner.add(incident_scheduler.schedule_incident(incident, start_time))

        for group in self._all_incident_groups:
            runner.add(incident_scheduler.schedule_incident_group(group, start_time))

        stats_display = self._create_stats_display()

        # Display update task
        async def update_display(live: Live):
            while not self.should_stop:
                live.update(stats_display.generate_stats_table())
                await asyncio.sleep(0.5)
                if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                    break
            self.request_stop()

        # Run with live display
        with Live(
            stats_display.generate_stats_table(), refresh_per_second=4, console=console
        ) as live:
            runner.add(update_display(live))
            await runner.run()

        result.messages_produced = sum(p.get_stats().messages_sent for p in self.producers)
        result.messages_consumed = sum(c.get_stats().messages_consumed for c in self.consumers)
        result.flows_completed = sum(fp.get_stats().flows_completed for fp in self.flow_producers)
        result.flow_messages_sent = sum(fp.get_stats().messages_sent for fp in self.flow_producers)
        result.duration_seconds = time.time() - start_time

        return result

    async def start(self, duration_seconds: int) -> ExecutionResult:
        loop = asyncio.get_event_loop()

        def signal_handler():
            console.print("\n[yellow]Shutting down gracefully...[/yellow]")
            self.request_stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        try:
            await self.setup()
            result = await self._run(duration_seconds)
            return result
        finally:
            await self.teardown()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
