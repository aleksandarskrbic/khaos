"""Stats display for scenario execution."""

from __future__ import annotations

from rich.table import Table

from khaos.generators.flow import FlowProducer
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.scenarios.scenario import TopicConfig


class StatsDisplay:
    """Generates stats tables for scenario execution display."""

    def __init__(
        self,
        scenario_names: list[str],
        topics: list[TopicConfig],
        producers: list[ProducerSimulator],
        consumers: list[ConsumerSimulator],
        flow_producers: list[FlowProducer],
        producers_by_topic: dict[str, list[ProducerSimulator]],
        consumers_by_topic: dict[str, dict[str, list[ConsumerSimulator]]],
    ):
        """Initialize stats display.

        Args:
            scenario_names: Names of scenarios being executed
            topics: All topic configurations
            producers: All producer instances
            consumers: All consumer instances
            flow_producers: All flow producer instances
            producers_by_topic: Producers grouped by topic
            consumers_by_topic: Consumers grouped by topic and group
        """
        self._scenario_names = scenario_names
        self._topics = topics
        self._producers = producers
        self._consumers = consumers
        self._flow_producers = flow_producers
        self._producers_by_topic = producers_by_topic
        self._consumers_by_topic = consumers_by_topic

    def get_display_title(self) -> str:
        """Get the display title for the stats table."""
        if len(self._scenario_names) == 1:
            return f"Scenario: {self._scenario_names[0]}"
        return f"Scenarios: {', '.join(self._scenario_names)}"

    def generate_stats_table(self) -> Table:
        """Generate a Rich table with current execution stats."""
        table = Table(title=self.get_display_title())
        table.add_column("Name", style="cyan")
        table.add_column("Produced", style="green")
        table.add_column("Consumed", style="yellow")
        table.add_column("Lag", style="red")

        for topic in self._topics:
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

        if self._flow_producers:
            table.add_section()
            for flow_producer in self._flow_producers:
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
        total_produced = sum(p.get_stats().messages_sent for p in self._producers)
        total_flow_messages = sum(fp.get_stats().messages_sent for fp in self._flow_producers)
        total_consumed = sum(c.get_stats().messages_consumed for c in self._consumers)
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
