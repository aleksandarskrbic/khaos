from __future__ import annotations

import asyncio
import signal
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.live import Live
from rich.table import Table

from khaos.generators.flow import FlowProducer
from khaos.generators.key import create_key_generator
from khaos.generators.payload import create_payload_generator
from khaos.kafka.admin import KafkaAdmin
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.config import ProducerConfig
from khaos.models.flow import FlowConfig
from khaos.models.message import KeyDistribution, MessageSchema
from khaos.models.topic import TopicConfig as KafkaTopicConfig
from khaos.runtime import shutdown_executor
from khaos.scenarios.incidents import INCIDENT_HANDLERS, IncidentContext
from khaos.scenarios.scenario import Incident, IncidentGroup, Scenario, TopicConfig

if TYPE_CHECKING:
    from khaos.models.flow import FlowStep

console = Console()


@dataclass
class ExecutionResult:
    messages_produced: int = 0
    messages_consumed: int = 0
    flows_completed: int = 0
    flow_messages_sent: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        self.errors.append(error)


class ScenarioExecutor:
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

    def _get_display_title(self) -> str:
        names = [s.name for s in self.scenarios]
        if len(names) == 1:
            return f"Scenario: {names[0]}"
        return f"Scenarios: {', '.join(names)}"

    async def setup(self) -> None:
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
                    self.admin.create_topic(topic_config)
                    created_topics.add(topic_name)

    async def teardown(self) -> None:
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

    def _to_key_distribution(self, name: str) -> KeyDistribution:
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
        consumers = []
        if topic.name not in self._consumers_by_topic:
            self._consumers_by_topic[topic.name] = {}
        for g in range(topic.num_consumer_groups):
            group_id = f"{topic.name}-group-{g + 1}"
            if group_id not in self._consumers_by_topic[topic.name]:
                self._consumers_by_topic[topic.name][group_id] = []
            for c in range(topic.consumers_per_group):
                consumer = ConsumerSimulator(
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=group_id,
                    topics=[topic.name],
                    processing_delay_ms=topic.consumer_delay_ms,
                )
                self.consumers.append(consumer)
                self._consumers_by_topic[topic.name][group_id].append(consumer)
                consumers.append((group_id, f"{group_id}-consumer-{c + 1}", consumer))
        return consumers

    def _create_flow_step_consumers(
        self,
        flow_name: str,
        step: FlowStep,
        duration_seconds: int,
        result: ExecutionResult,
    ) -> list:
        tasks = []
        config = step.consumers
        assert config is not None  # todo: fix this?
        topic_name = step.topic

        if topic_name not in self._consumers_by_topic:
            self._consumers_by_topic[topic_name] = {}

        for g in range(config.groups):
            group_id = f"{flow_name}-{topic_name}-group-{g + 1}"
            if group_id not in self._consumers_by_topic[topic_name]:
                self._consumers_by_topic[topic_name][group_id] = []

            for _c in range(config.per_group):
                consumer = ConsumerSimulator(
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=group_id,
                    topics=[topic_name],
                    processing_delay_ms=config.delay_ms,
                )
                self.consumers.append(consumer)
                self._consumers_by_topic[topic_name][group_id].append(consumer)

                async def consumer_task(cons=consumer):
                    try:
                        await cons.consume_loop(duration_seconds=duration_seconds)
                    except Exception as e:
                        result.add_error(f"Flow consumer error: {e}")

                tasks.append(consumer_task())

        return tasks

    def generate_stats_table(self) -> Table:
        table = Table(title=self._get_display_title())
        table.add_column("Name", style="cyan")
        table.add_column("Produced", style="green")
        table.add_column("Consumed", style="yellow")
        table.add_column("Lag", style="red")

        for topic in self._all_topics:
            topic_producers = self._producers_by_topic.get(topic.name, [])
            topic_groups = self._consumers_by_topic.get(topic.name, {})

            produced = sum(p.get_stats().messages_sent for p in topic_producers)
            total_consumed = sum(
                c.get_stats().messages_consumed
                for group_consumers in topic_groups.values()
                for c in group_consumers
            )

            lag = produced - total_consumed
            lag_display = f"[red]{lag:,}[/red]" if lag > 100 else f"[green]{lag:,}[/green]"

            table.add_row(
                f"[bold]{topic.name}[/bold]",
                f"[bold]{produced:,}[/bold]",
                f"[bold]{total_consumed:,}[/bold]",
                lag_display,
            )

            group_names = list(topic_groups.keys())
            for g_idx, group_id in enumerate(group_names):
                consumers = topic_groups[group_id]
                group_consumed = sum(c.get_stats().messages_consumed for c in consumers)
                is_last_group = g_idx == len(group_names) - 1
                group_prefix = "└─ " if is_last_group else "├─ "

                table.add_row(
                    f"[dim]  {group_prefix}{group_id}[/dim]",
                    "",
                    f"[dim]{group_consumed:,}[/dim]",
                    "",
                )

        if self.flow_producers:
            table.add_section()
            for flow_producer in self.flow_producers:
                stats = flow_producer.get_stats()
                flow = flow_producer.flow

                table.add_row(
                    f"[bold cyan]Flow: {flow.name}[/bold cyan]",
                    f"[bold]{stats.messages_sent:,}[/bold]",
                    "",
                    "",
                )

                unique_topics = list(dict.fromkeys(s.topic for s in flow.steps))
                for idx, topic_name in enumerate(unique_topics):
                    is_last = idx == len(unique_topics) - 1
                    prefix = "└─" if is_last else "├─"

                    topic_produced = stats.get_topic_count(topic_name)
                    produced_display = f"{topic_produced:,}" if topic_produced > 0 else ""

                    topic_groups = self._consumers_by_topic.get(topic_name, {})
                    flow_groups = {
                        k: v
                        for k, v in topic_groups.items()
                        if k.startswith(f"{flow.name}-{topic_name}-")
                    }

                    if flow_groups:
                        total_consumed = sum(
                            c.get_stats().messages_consumed
                            for consumers in flow_groups.values()
                            for c in consumers
                        )
                        consumed_display = f"{total_consumed:,}"
                        lag = topic_produced - total_consumed
                    else:
                        consumed_display = "[dim]no consumer[/dim]"
                        lag = topic_produced

                    if lag > 100:
                        lag_display = f"[red]{lag:,}[/red]"
                    elif lag > 0:
                        lag_display = f"[yellow]{lag:,}[/yellow]"
                    else:
                        lag_display = f"[green]{lag:,}[/green]"

                    table.add_row(
                        f"  {prefix} [cyan]{topic_name}[/cyan]",
                        produced_display,
                        consumed_display,
                        lag_display,
                    )

        table.add_section()
        total_produced = sum(p.get_stats().messages_sent for p in self.producers)
        total_flow_messages = sum(fp.get_stats().messages_sent for fp in self.flow_producers)
        total_consumed = sum(c.get_stats().messages_consumed for c in self.consumers)
        total_lag = total_produced - total_consumed

        table.add_row(
            "[bold]TOTAL[/bold]",
            f"[bold]{total_produced + total_flow_messages:,}[/bold]",
            f"[bold]{total_consumed:,}[/bold]",
            f"[bold red]{total_lag:,}[/bold red]"
            if total_lag > 100
            else f"[bold green]{total_lag:,}[/bold green]",
        )

        return table

    async def _schedule_incident(
        self, incident: Incident, ctx: IncidentContext, start_time: float
    ) -> None:
        handler = INCIDENT_HANDLERS.get(incident.type)
        if not handler:
            console.print(f"[red]Unknown incident type: {incident.type}[/red]")
            return

        kwargs: dict[str, Any] = {}
        if incident.delay_ms is not None:
            kwargs["delay_ms"] = incident.delay_ms
        if incident.broker is not None:
            kwargs["broker"] = incident.broker
        if incident.rate is not None:
            kwargs["rate"] = incident.rate
        if incident.duration_seconds is not None:
            kwargs["duration_seconds"] = incident.duration_seconds

        if incident.every_seconds:
            # Recurring incident
            await asyncio.sleep(incident.initial_delay_seconds)
            while not self.should_stop:
                await handler(ctx, **kwargs)
                await asyncio.sleep(incident.every_seconds)
        elif incident.at_seconds is not None:
            # One-time incident at specific time
            elapsed = time.time() - start_time
            wait_time = incident.at_seconds - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            if not self.should_stop:
                await handler(ctx, **kwargs)

    async def _schedule_incident_group(
        self, group: IncidentGroup, ctx: IncidentContext, start_time: float
    ) -> None:
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

                # at_seconds is relative to cycle start
                if incident.at_seconds is not None:
                    incident_time = cycle_start + incident.at_seconds
                    now = time.time()
                    wait_time = incident_time - now
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

                if self.should_stop:
                    break

                handler = INCIDENT_HANDLERS.get(incident.type)
                if not handler:
                    console.print(f"[red]Unknown incident type: {incident.type}[/red]")
                    continue

                # Build kwargs
                kwargs: dict[str, Any] = {}
                if incident.delay_ms is not None:
                    kwargs["delay_ms"] = incident.delay_ms
                if incident.broker is not None:
                    kwargs["broker"] = incident.broker
                if incident.rate is not None:
                    kwargs["rate"] = incident.rate
                if incident.duration_seconds is not None:
                    kwargs["duration_seconds"] = incident.duration_seconds

                await handler(ctx, **kwargs)

    async def run(self, duration_seconds: int) -> ExecutionResult:
        result = ExecutionResult()
        start_time = time.time()
        tasks = []

        # Create producers and consumers for all topics
        for topic in self._all_topics:
            # Create message schema
            msg_schema = MessageSchema(
                min_size_bytes=topic.message_schema.min_size_bytes,
                max_size_bytes=topic.message_schema.max_size_bytes,
                key_distribution=self._to_key_distribution(topic.message_schema.key_distribution),
                key_cardinality=topic.message_schema.key_cardinality,
                fields=topic.message_schema.fields,
            )

            # Producers
            producers = self._create_producers_for_topic(topic)
            key_gen = create_key_generator(msg_schema)
            payload_gen = create_payload_generator(msg_schema)

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
                        result.add_error(f"Producer error: {e}")
                    finally:
                        p.flush()

                tasks.append(producer_task())

            # Consumers (skip if no_consumers mode)
            if not self.no_consumers:
                consumers = self._create_consumers_for_topic(topic)

                for _group_id, _name, consumer in consumers:

                    async def consumer_task(c=consumer):
                        try:
                            await c.consume_loop(duration_seconds=duration_seconds)
                        except Exception as e:
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
        ctx = IncidentContext(executor=self, bootstrap_servers=self.bootstrap_servers)
        for incident in self._all_incidents:
            tasks.append(self._schedule_incident(incident, ctx, start_time))

        # Schedule incident groups
        for group in self._all_incident_groups:
            tasks.append(self._schedule_incident_group(group, ctx, start_time))

        # Display update task
        async def update_display(live: Live):
            while not self.should_stop:
                live.update(self.generate_stats_table())
                await asyncio.sleep(0.5)
                if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                    break
            # Signal all tasks to stop when duration is reached
            self.request_stop()

        # Run with live display
        with Live(self.generate_stats_table(), refresh_per_second=4, console=console) as live:
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
