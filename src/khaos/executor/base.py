"""Base executor with common functionality."""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from abc import ABC, abstractmethod

from rich.console import Console
from rich.live import Live

from khaos.executor.result import ExecutionResult
from khaos.executor.serializer import SerializerFactory
from khaos.executor.stats import StatsDisplay
from khaos.generators.flow import FlowProducer
from khaos.generators.key import create_key_generator
from khaos.generators.payload import create_payload_generator
from khaos.kafka.admin import KafkaAdmin
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.config import ProducerConfig
from khaos.models.flow import FlowConfig, FlowStep
from khaos.models.message import KeyDistribution, MessageSchema
from khaos.models.topic import TopicConfig as KafkaTopicConfig
from khaos.runtime import shutdown_executor
from khaos.scenarios.incidents import (
    Command,
    CreateConsumer,
    Delay,
    Incident,
    IncidentContext,
    IncidentGroup,
    IncrementRebalanceCount,
    PrintMessage,
    ResumeConsumers,
    SetConsumerDelay,
    SetProducerRate,
    StartBroker,
    StopBroker,
    StopConsumer,
    StopConsumers,
)
from khaos.scenarios.scenario import Scenario, TopicConfig
from khaos.serialization import SchemaRegistryProvider

console = Console()
logger = logging.getLogger(__name__)


class BaseExecutor(ABC):
    """Base class for scenario executors."""

    def __init__(
        self,
        bootstrap_servers: str,
        scenarios: list[Scenario],
        no_consumers: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.scenarios = scenarios
        self.no_consumers = no_consumers
        self.admin = KafkaAdmin(bootstrap_servers)

        self._stop_event = asyncio.Event()
        self.producers: list[ProducerSimulator] = []
        self.consumers: list[ConsumerSimulator] = []
        self.flow_producers: list[FlowProducer] = []
        self.rebalance_count = 0

        # Per-topic tracking for accurate stats
        self._producers_by_topic: dict[str, list[ProducerSimulator]] = {}
        # Nested structure: {topic_name: {group_id: [consumer1, consumer2, ...]}}
        self._consumers_by_topic: dict[str, dict[str, list[ConsumerSimulator]]] = {}
        # Flat structure for incident targeting: {group_id: [consumer1, consumer2, ...]}
        self._consumers_by_group: dict[str, list[ConsumerSimulator]] = {}
        # Flat structure: {topic_name: [consumer1, consumer2, ...]}
        self._consumers_by_topic_flat: dict[str, list[ConsumerSimulator]] = {}
        # Map topic to scenario name for display
        self._topic_to_scenario: dict[str, str] = {}
        # Flow producers by flow name
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
    def _is_schema_registry_running(self) -> bool:
        pass

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

    async def setup(self) -> None:
        """Set up topics and resources. Override in subclasses for additional setup."""
        created_topics: set[str] = set()

        for topic in self._all_topics:
            console.print(
                f"[dim]Creating topic: {topic.name} ({topic.partitions} partitions)[/dim]"
            )
            topic_config = KafkaTopicConfig(
                name=topic.name,
                partitions=topic.partitions,
                replication_factor=topic.replication_factor,
            )
            self.admin.delete_topic(topic.name)
            self.admin.create_topic(topic_config)
            created_topics.add(topic.name)

        for flow in self._all_flows:
            for topic_name in flow.get_all_topics():
                if topic_name not in created_topics:
                    console.print(f"[dim]Creating topic for flow: {topic_name}[/dim]")
                    topic_config = KafkaTopicConfig(
                        name=topic_name,
                        partitions=12,
                        replication_factor=3,
                    )
                    self.admin.delete_topic(topic_name)
                    self.admin.create_topic(topic_config)
                    created_topics.add(topic_name)

        if created_topics:
            await asyncio.sleep(10)

    async def teardown(self) -> None:
        """Clean up resources."""
        for producer in self.producers:
            producer.stop()
            producer.flush(timeout=5)

        for flow_producer in self.flow_producers:
            flow_producer.stop()
            flow_producer.flush(timeout=5)

        for consumer in self.consumers:
            consumer.stop()
            consumer.close()

        shutdown_executor()

    def request_stop(self) -> None:
        """Request all producers and consumers to stop."""
        self._stop_event.set()
        for producer in self.producers:
            producer.stop()
        for flow_producer in self.flow_producers:
            flow_producer.stop()
        for consumer in self.consumers:
            consumer.stop()

    @property
    def should_stop(self) -> bool:
        """Check if stop has been requested."""
        return self._stop_event.is_set()

    async def execute_commands(self, commands: list[Command]) -> None:
        """Execute a list of incident commands."""
        for cmd in commands:
            if self.should_stop:
                break

            match cmd:
                case Delay(seconds=s):
                    await asyncio.sleep(s)

                case PrintMessage(message=msg, style=style):
                    console.print(f"\n[{style}]{msg}[/]")

                case SetConsumerDelay(index=idx, delay_ms=delay):
                    if idx < len(self.consumers):
                        self.consumers[idx].processing_delay_ms = delay

                case SetProducerRate(index=idx, rate=r):
                    if idx < len(self.producers):
                        self.producers[idx].messages_per_second = r

                case IncrementRebalanceCount():
                    self.rebalance_count += 1

                case StopConsumers(indices=indices):
                    for idx in indices:
                        if idx < len(self.consumers):
                            self.consumers[idx].stop()

                case ResumeConsumers(indices=indices):
                    for idx in indices:
                        if idx < len(self.consumers):
                            self.consumers[idx]._stop_event.clear()
                            asyncio.create_task(self.consumers[idx].consume_loop(duration_seconds=0))

                case StopConsumer(index=idx):
                    if idx < len(self.consumers):
                        self.consumers[idx].stop()
                        # Wait for poll to finish before closing
                        await asyncio.sleep(5)
                        self.consumers[idx].close()

                case CreateConsumer(index=idx, group_id=gid, topics=t, processing_delay_ms=d):
                    new_consumer = self._create_single_consumer(gid, t, d)
                    if idx < len(self.consumers):
                        self.consumers[idx] = new_consumer
                    asyncio.create_task(new_consumer.consume_loop(duration_seconds=0))

                case StopBroker(broker=b):
                    await self._handle_stop_broker(b)

                case StartBroker(broker=b):
                    await self._handle_start_broker(b)

    def _create_single_consumer(
        self,
        group_id: str,
        topics: list[str],
        processing_delay_ms: int,
    ) -> ConsumerSimulator:
        """Create a single consumer. Override in subclasses for custom config."""
        return ConsumerSimulator(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            topics=topics,
            processing_delay_ms=processing_delay_ms,
        )

    async def _handle_stop_broker(self, broker: str) -> None:  # noqa: B027
        """Handle StopBroker command. Override in subclasses."""
        pass

    async def _handle_start_broker(self, broker: str) -> None:  # noqa: B027
        """Handle StartBroker command. Override in subclasses."""
        pass

    def _to_key_distribution(self, name: str) -> KeyDistribution:
        """Convert string to KeyDistribution enum."""
        mapping = {
            "uniform": KeyDistribution.UNIFORM,
            "zipfian": KeyDistribution.ZIPFIAN,
            "single_key": KeyDistribution.SINGLE_KEY,
            "round_robin": KeyDistribution.ROUND_ROBIN,
        }
        return mapping.get(name, KeyDistribution.UNIFORM)

    def _create_producers_for_topic(
        self, topic: TopicConfig
    ) -> list[tuple[str, ProducerSimulator]]:
        """Create producers for a topic. Override in subclasses for custom config."""
        producers = []
        config = ProducerConfig(
            messages_per_second=topic.producer_rate,
            batch_size=topic.producer_config.batch_size,
            linger_ms=topic.producer_config.linger_ms,
            acks=topic.producer_config.acks,
            compression_type=topic.producer_config.compression_type,
        )
        if topic.name not in self._producers_by_topic:
            self._producers_by_topic[topic.name] = []
        for i in range(topic.num_producers):
            producer = ProducerSimulator(
                bootstrap_servers=self.bootstrap_servers,
                config=config,
            )
            self.producers.append(producer)
            self._producers_by_topic[topic.name].append(producer)
            producers.append((f"{topic.name}-producer-{i + 1}", producer))
        return producers

    def _create_consumers_for_topic(
        self, topic: TopicConfig
    ) -> list[tuple[str, str, ConsumerSimulator]]:
        """Create consumers for a topic. Override in subclasses for custom config."""
        consumers = []
        if topic.name not in self._consumers_by_topic:
            self._consumers_by_topic[topic.name] = {}
        if topic.name not in self._consumers_by_topic_flat:
            self._consumers_by_topic_flat[topic.name] = []

        for g in range(topic.num_consumer_groups):
            group_id = f"{topic.name}-group-{g + 1}"
            if group_id not in self._consumers_by_topic[topic.name]:
                self._consumers_by_topic[topic.name][group_id] = []
            if group_id not in self._consumers_by_group:
                self._consumers_by_group[group_id] = []

            for c in range(topic.consumers_per_group):
                consumer = self._create_single_consumer(
                    group_id=group_id,
                    topics=[topic.name],
                    processing_delay_ms=topic.consumer_delay_ms,
                )
                self.consumers.append(consumer)
                self._consumers_by_topic[topic.name][group_id].append(consumer)
                self._consumers_by_topic_flat[topic.name].append(consumer)
                self._consumers_by_group[group_id].append(consumer)
                consumers.append((group_id, f"{group_id}-consumer-{c + 1}", consumer))
        return consumers

    def _create_flow_step_consumers(
        self,
        flow_name: str,
        step: FlowStep,
        duration_seconds: int,
        result: ExecutionResult,
    ) -> list:
        """Create consumers for a flow step."""
        tasks = []
        config = step.consumers
        assert config is not None
        topic_name = step.topic

        if topic_name not in self._consumers_by_topic:
            self._consumers_by_topic[topic_name] = {}
        if topic_name not in self._consumers_by_topic_flat:
            self._consumers_by_topic_flat[topic_name] = []

        for g in range(config.groups):
            group_id = f"{flow_name}-{topic_name}-group-{g + 1}"
            if group_id not in self._consumers_by_topic[topic_name]:
                self._consumers_by_topic[topic_name][group_id] = []
            if group_id not in self._consumers_by_group:
                self._consumers_by_group[group_id] = []

            for _c in range(config.per_group):
                consumer = self._create_single_consumer(
                    group_id=group_id,
                    topics=[topic_name],
                    processing_delay_ms=config.delay_ms,
                )
                self.consumers.append(consumer)
                self._consumers_by_topic[topic_name][group_id].append(consumer)
                self._consumers_by_topic_flat[topic_name].append(consumer)
                self._consumers_by_group[group_id].append(consumer)

                async def consumer_task(cons=consumer, gid=group_id):
                    try:
                        await cons.consume_loop(duration_seconds=duration_seconds)
                    except Exception as e:
                        logger.exception(f"Flow consumer error for group '{gid}': {e}")
                        result.add_error(f"Flow consumer error: {e}")

                tasks.append(consumer_task())

        return tasks

    def _build_incident_context(self) -> IncidentContext:
        """Build context for incident execution."""
        return IncidentContext(
            consumers=self.consumers,
            producers=self.producers,
            bootstrap_servers=self.bootstrap_servers,
            rebalance_count=self.rebalance_count,
            consumers_by_topic=self._consumers_by_topic_flat,
            consumers_by_group=self._consumers_by_group,
            producers_by_topic=self._producers_by_topic,
        )

    async def _schedule_incident(self, incident: Incident, start_time: float) -> None:
        """Schedule an incident for execution."""
        schedule = incident.schedule

        if schedule.every_seconds:
            # Recurring incident
            await asyncio.sleep(schedule.initial_delay_seconds)
            while not self.should_stop:
                ctx = self._build_incident_context()
                commands = incident.get_commands(ctx)
                await self.execute_commands(commands)
                await asyncio.sleep(schedule.every_seconds)
        elif schedule.at_seconds is not None:
            # One-time incident at specific time
            elapsed = time.time() - start_time
            wait_time = schedule.at_seconds - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            if not self.should_stop:
                ctx = self._build_incident_context()
                commands = incident.get_commands(ctx)
                await self.execute_commands(commands)

    async def _schedule_incident_group(self, group: IncidentGroup, start_time: float) -> None:
        """Schedule an incident group for execution."""
        for cycle in range(group.repeat):
            if self.should_stop:
                break

            cycle_start = start_time + (cycle * group.interval_seconds)

            # Wait until this cycle should start
            now = time.time()
            wait_time = cycle_start - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)

            if self.should_stop:
                break

            console.print(f"\n[bold magenta]>>> GROUP: Cycle {cycle + 1}/{group.repeat}[/]")

            # Execute all incidents in the group
            for incident in group.incidents:
                if self.should_stop:
                    break

                schedule = incident.schedule

                # at_seconds is relative to cycle start
                if schedule.at_seconds is not None:
                    incident_time = cycle_start + schedule.at_seconds
                    now = time.time()
                    wait_time = incident_time - now
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

                if self.should_stop:
                    break

                ctx = self._build_incident_context()
                commands = incident.get_commands(ctx)
                await self.execute_commands(commands)

    async def run(self, duration_seconds: int) -> ExecutionResult:
        """Run the scenario execution."""
        result = ExecutionResult()
        start_time = time.time()
        tasks = []

        serializer_factory = self._create_serializer_factory()

        # Create schema registry provider for caching (if needed)
        schema_registry_provider: SchemaRegistryProvider | None = None
        schema_registry = serializer_factory.get_schema_registry_config()
        if schema_registry:
            schema_registry_provider = SchemaRegistryProvider(schema_registry.url)

        # Create producers and consumers for all topics
        for topic in self._all_topics:
            fields = topic.message_schema.fields
            data_format = topic.message_schema.data_format
            raw_avro_schema: dict | None = None

            # Fetch schema from registry if using registry provider
            if topic.schema_provider == "registry" and topic.subject_name:
                if not schema_registry_provider:
                    result.add_error(
                        f"Topic '{topic.name}' uses registry provider but no schema_registry"
                    )
                    continue

                try:
                    console.print(
                        f"[dim]Fetching schema for '{topic.subject_name}' from registry...[/dim]"
                    )
                    data_format, fields = schema_registry_provider.get_field_schemas(
                        topic.subject_name
                    )
                    # Get raw schema to preserve original name/namespace for serialization
                    _, raw_schema = schema_registry_provider.get_raw_schema(topic.subject_name)
                    # For Avro, raw_schema is a dict
                    if data_format == "avro" and isinstance(raw_schema, dict):
                        raw_avro_schema = raw_schema
                    console.print(
                        f"[dim]Loaded {len(fields)} fields from {data_format.upper()} schema[/dim]"
                    )
                except Exception as e:
                    result.add_error(f"Failed to fetch schema for '{topic.subject_name}': {e}")
                    continue

            # Create message schema
            msg_schema = MessageSchema(
                min_size_bytes=topic.message_schema.min_size_bytes,
                max_size_bytes=topic.message_schema.max_size_bytes,
                key_distribution=self._to_key_distribution(topic.message_schema.key_distribution),
                key_cardinality=topic.message_schema.key_cardinality,
                fields=fields,
            )

            # Create serializer based on data format
            serializer = serializer_factory.create_serializer_with_format(
                topic, data_format, fields, raw_avro_schema
            )

            # Producers
            producers = self._create_producers_for_topic(topic)
            key_gen = create_key_generator(msg_schema)
            payload_gen = create_payload_generator(msg_schema, serializer=serializer)

            for _name, producer in producers:

                async def producer_task(p=producer, t=topic.name, kg=key_gen, pg=payload_gen):
                    try:
                        await p.produce_at_rate(
                            topic=t,
                            message_generator=pg,
                            key_generator=kg,
                            duration_seconds=duration_seconds,
                        )
                    except Exception as e:
                        logger.exception(f"Producer error for topic '{t}': {e}")
                        result.add_error(f"Producer error: {e}")
                    finally:
                        p.flush()

                tasks.append(producer_task())

            # Consumers (skip if no_consumers mode)
            if not self.no_consumers:
                consumers = self._create_consumers_for_topic(topic)

                for _group_id, _name, consumer in consumers:

                    async def consumer_task(c=consumer, gid=_group_id):
                        try:
                            await c.consume_loop(duration_seconds=duration_seconds)
                        except Exception as e:
                            logger.exception(f"Consumer error for group '{gid}': {e}")
                            result.add_error(f"Consumer error: {e}")

                    tasks.append(consumer_task())

        # Create flow producers and their consumers
        for flow in self._all_flows:
            flow_producer = FlowProducer(
                flow=flow,
                bootstrap_servers=self.bootstrap_servers,
            )
            self.flow_producers.append(flow_producer)
            self._flow_producers_by_name[flow.name] = flow_producer

            async def flow_task(fp=flow_producer):
                try:
                    await fp.run_at_rate(duration_seconds=duration_seconds)
                except Exception as e:
                    logger.exception(f"Flow producer error for '{fp.flow.name}': {e}")
                    result.add_error(f"Flow producer error: {e}")
                finally:
                    fp.flush()

            tasks.append(flow_task())

            # Create consumers for flow steps (if configured and not no_consumers mode)
            if not self.no_consumers:
                for step in flow.steps:
                    if step.consumers:
                        tasks.extend(
                            self._create_flow_step_consumers(
                                flow.name, step, duration_seconds, result
                            )
                        )

        # Schedule incidents
        for incident in self._all_incidents:
            tasks.append(self._schedule_incident(incident, start_time))

        # Schedule incident groups
        for group in self._all_incident_groups:
            tasks.append(self._schedule_incident_group(group, start_time))

        # Create stats display
        stats_display = self._create_stats_display()

        # Display update task
        async def update_display(live: Live):
            while not self.should_stop:
                live.update(stats_display.generate_stats_table())
                await asyncio.sleep(0.5)
                if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                    break
            # Signal all tasks to stop when duration is reached
            self.request_stop()

        # Run with live display
        with Live(
            stats_display.generate_stats_table(), refresh_per_second=4, console=console
        ) as live:
            tasks.append(update_display(live))

            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                pass

        # Collect results
        result.messages_produced = sum(p.get_stats().messages_sent for p in self.producers)
        result.messages_consumed = sum(c.get_stats().messages_consumed for c in self.consumers)
        result.flows_completed = sum(fp.get_stats().flows_completed for fp in self.flow_producers)
        result.flow_messages_sent = sum(fp.get_stats().messages_sent for fp in self.flow_producers)
        result.duration_seconds = time.time() - start_time

        return result

    async def execute(self, duration_seconds: int) -> ExecutionResult:
        """Execute the scenario with signal handling."""
        loop = asyncio.get_event_loop()

        def signal_handler():
            console.print("\n[yellow]Shutting down gracefully...[/yellow]")
            self.request_stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        try:
            await self.setup()
            result = await self.run(duration_seconds)
            return result
        finally:
            await self.teardown()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
