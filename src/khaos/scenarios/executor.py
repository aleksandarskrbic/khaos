"""Scenario executor - runs one or more scenarios with incident scheduling."""

import asyncio
import signal
import time
from dataclasses import dataclass, field

from rich.console import Console
from rich.live import Live
from rich.table import Table

from khaos.generators.key import create_key_generator
from khaos.generators.payload import create_payload_generator
from khaos.kafka.admin import KafkaAdmin
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.config import ProducerConfig
from khaos.models.message import KeyDistribution, MessageSchema
from khaos.models.topic import TopicConfig as KafkaTopicConfig
from khaos.runtime import shutdown_executor
from khaos.scenarios.incidents import INCIDENT_HANDLERS, IncidentContext
from khaos.scenarios.scenario import Incident, IncidentGroup, Scenario, TopicConfig

console = Console()


@dataclass
class ExecutionResult:
    """Result from executing scenarios."""

    messages_produced: int = 0
    messages_consumed: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        self.errors.append(error)


class ScenarioExecutor:
    """Executes one or more scenarios with incident scheduling."""

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
        self.rebalance_count = 0

        # Per-topic tracking for accurate stats
        self._producers_by_topic: dict[str, list[ProducerSimulator]] = {}
        # Nested structure: {topic_name: {group_id: [consumer1, consumer2, ...]}}
        self._consumers_by_topic: dict[str, dict[str, list[ConsumerSimulator]]] = {}
        # Map topic to scenario name for display
        self._topic_to_scenario: dict[str, str] = {}

        # Collect all topics, incidents, and incident groups from all scenarios
        self._all_topics: list[TopicConfig] = []
        self._all_incidents: list[Incident] = []
        self._all_incident_groups: list[IncidentGroup] = []
        for scenario in scenarios:
            for topic in scenario.topics:
                self._topic_to_scenario[topic.name] = scenario.name
            self._all_topics.extend(scenario.topics)
            self._all_incidents.extend(scenario.incidents)
            self._all_incident_groups.extend(scenario.incident_groups)

    def _get_display_title(self) -> str:
        """Get title for the live display."""
        names = [s.name for s in self.scenarios]
        if len(names) == 1:
            return f"Scenario: {names[0]}"
        return f"Scenarios: {', '.join(names)}"

    async def setup(self) -> None:
        """Create all topics for all scenarios."""
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

    async def teardown(self) -> None:
        """Clean up all resources."""
        for producer in self.producers:
            producer.stop()
            producer.flush(timeout=5)

        for consumer in self.consumers:
            consumer.stop()
            consumer.close()

        shutdown_executor()

    def request_stop(self) -> None:
        """Signal all tasks to stop."""
        self._stop_event.set()
        for producer in self.producers:
            producer.stop()
        for consumer in self.consumers:
            consumer.stop()

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

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
        """Create producers for a topic."""
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
        """Create consumers for a topic."""
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

    def generate_stats_table(self) -> Table:
        """Generate a Rich table with current stats."""
        table = Table(title=self._get_display_title())
        table.add_column("Topic/Consumer", style="cyan")
        table.add_column("Scenario", style="magenta")
        table.add_column("Produced", style="green")
        table.add_column("Consumed", style="yellow")
        table.add_column("Lag", style="red")

        for topic in self._all_topics:
            # Get per-topic stats
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
            scenario_name = self._topic_to_scenario.get(topic.name, "")

            # Topic row (bold)
            table.add_row(
                f"[bold]{topic.name}[/bold]",
                f"[dim]{scenario_name}[/dim]",
                f"[bold]{produced:,}[/bold]",
                f"[bold]{total_consumed:,}[/bold]",
                lag_display,
            )

            # Consumer groups and individual consumers
            group_names = list(topic_groups.keys())
            for g_idx, group_id in enumerate(group_names):
                consumers = topic_groups[group_id]
                group_consumed = sum(c.get_stats().messages_consumed for c in consumers)
                is_last_group = g_idx == len(group_names) - 1
                group_prefix = "  └─ " if is_last_group else "  ├─ "

                # Group row
                table.add_row(
                    f"[dim]{group_prefix}{group_id}[/dim]",
                    "",
                    "",
                    f"[dim]{group_consumed:,}[/dim]",
                    "",
                )

                # Individual consumers
                for c_idx, consumer in enumerate(consumers):
                    consumed = consumer.get_stats().messages_consumed
                    is_last_consumer = c_idx == len(consumers) - 1
                    if is_last_group:
                        consumer_prefix = "      └─ " if is_last_consumer else "      ├─ "
                    else:
                        consumer_prefix = "  │   └─ " if is_last_consumer else "  │   ├─ "

                    table.add_row(
                        f"[dim]{consumer_prefix}consumer-{c_idx + 1}[/dim]",
                        "",
                        "",
                        f"[dim]{consumed:,}[/dim]",
                        "",
                    )

        # Summary row
        table.add_section()
        total_produced = sum(p.get_stats().messages_sent for p in self.producers)
        total_consumed = sum(c.get_stats().messages_consumed for c in self.consumers)
        total_lag = total_produced - total_consumed

        table.add_row(
            "[bold]TOTAL[/bold]",
            "",
            f"[bold]{total_produced:,}[/bold]",
            f"[bold]{total_consumed:,}[/bold]",
            f"[bold red]{total_lag:,}[/bold red]"
            if total_lag > 100
            else f"[bold green]{total_lag:,}[/bold green]",
        )

        return table

    async def _schedule_incident(
        self, incident: Incident, ctx: IncidentContext, start_time: float
    ) -> None:
        """Schedule and execute a single incident."""
        handler = INCIDENT_HANDLERS.get(incident.type)
        if not handler:
            console.print(f"[red]Unknown incident type: {incident.type}[/red]")
            return

        # Build kwargs from incident fields
        kwargs = {}
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
        """Schedule and execute an incident group with repeats."""
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
                kwargs = {}
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
        """Run all scenarios."""
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
        result.duration_seconds = time.time() - start_time

        return result

    async def execute(self, duration_seconds: int) -> ExecutionResult:
        """Execute the full scenario lifecycle with signal handling."""
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
