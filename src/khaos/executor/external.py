"""External executor for connecting to external Kafka clusters."""

from __future__ import annotations

from rich.console import Console

from khaos.executor.base import BaseExecutor
from khaos.kafka.admin import KafkaAdmin
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ProducerConfig
from khaos.scenarios.incidents import IncidentGroup, StartBrokerIncident, StopBrokerIncident
from khaos.scenarios.scenario import Scenario, TopicConfig

console = Console()


def _is_infrastructure_incident(incident: object) -> bool:
    """Check if an incident requires infrastructure control."""
    return isinstance(incident, (StopBrokerIncident, StartBrokerIncident))


def _get_incident_type_name(incident: object) -> str:
    """Get the type name of an incident."""
    return type(incident).__name__


class ExternalExecutor(BaseExecutor):
    """Executor for external Kafka clusters.

    This executor:
    - Uses ClusterConfig for security settings (SASL, SSL, etc.)
    - Filters out infrastructure incidents (StopBroker/StartBroker)
    - Optionally skips topic creation
    - Passes cluster_config to producers/consumers
    """

    def __init__(
        self,
        cluster_config: ClusterConfig,
        scenarios: list[Scenario],
        skip_topic_creation: bool = False,
        no_consumers: bool = False,
    ):
        self.cluster_config = cluster_config
        self.skip_topic_creation = skip_topic_creation

        filtered_scenarios = self._filter_infrastructure_incidents(scenarios)

        super().__init__(
            bootstrap_servers=cluster_config.bootstrap_servers,
            scenarios=filtered_scenarios,
            no_consumers=no_consumers,
        )

        # Override admin with cluster config
        self.admin = KafkaAdmin(
            cluster_config.bootstrap_servers,
            cluster_config=cluster_config,
        )

    def _is_schema_registry_running(self) -> bool:
        """External clusters may have Schema Registry configured separately."""
        # For external clusters, assume Schema Registry is available if configured
        return any(s.schema_registry for s in self.scenarios)

    def _filter_infrastructure_incidents(
        self,
        scenarios: list[Scenario],
    ) -> list[Scenario]:
        """Filter out infrastructure incidents not supported on external clusters."""
        filtered = []
        skipped_count = 0

        for scenario in scenarios:
            new_incidents = []
            for incident in scenario.incidents:
                if _is_infrastructure_incident(incident):
                    console.print(
                        f"[yellow]Skipping '{_get_incident_type_name(incident)}' incident "
                        f"(not supported on external clusters)[/yellow]"
                    )
                    skipped_count += 1
                else:
                    new_incidents.append(incident)

            new_groups = []
            for group in scenario.incident_groups:
                new_group_incidents = []
                for incident in group.incidents:
                    if _is_infrastructure_incident(incident):
                        console.print(
                            f"[yellow]Skipping '{_get_incident_type_name(incident)}' in group "
                            f"(not supported on external clusters)[/yellow]"
                        )
                        skipped_count += 1
                    else:
                        new_group_incidents.append(incident)

                if new_group_incidents:
                    new_groups.append(
                        IncidentGroup(
                            repeat=group.repeat,
                            interval_seconds=group.interval_seconds,
                            incidents=new_group_incidents,
                        )
                    )

            filtered.append(
                Scenario(
                    name=scenario.name,
                    description=scenario.description,
                    topics=scenario.topics,
                    incidents=new_incidents,
                    incident_groups=new_groups,
                    flows=scenario.flows,
                    schema_registry=scenario.schema_registry,
                )
            )

        if skipped_count > 0:
            console.print(
                f"[yellow]Note: {skipped_count} infrastructure incident(s) "
                f"will be skipped[/yellow]\n"
            )

        return filtered

    def _create_single_consumer(
        self,
        group_id: str,
        topics: list[str],
        processing_delay_ms: int,
    ) -> ConsumerSimulator:
        """Create a consumer with cluster config for security."""
        return ConsumerSimulator(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            topics=topics,
            processing_delay_ms=processing_delay_ms,
            cluster_config=self.cluster_config,
        )

    def _create_producers_for_topic(
        self, topic: TopicConfig
    ) -> list[tuple[str, ProducerSimulator]]:
        """Create producers with cluster config for security."""
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
                cluster_config=self.cluster_config,
            )
            self.producers.append(producer)
            self._producers_by_topic[topic.name].append(producer)
            producers.append((f"{topic.name}-producer-{i + 1}", producer))

        return producers

    async def setup(self) -> None:
        """Set up topics (unless skipped)."""
        if self.skip_topic_creation:
            console.print("[dim]Skipping topic creation (--skip-topic-creation)[/dim]")
            return

        await super().setup()
