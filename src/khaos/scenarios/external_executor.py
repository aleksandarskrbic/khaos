from __future__ import annotations

from rich.console import Console

from khaos.kafka.admin import KafkaAdmin
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ProducerConfig
from khaos.scenarios.executor import ScenarioExecutor
from khaos.scenarios.incidents import INFRASTRUCTURE_INCIDENTS
from khaos.scenarios.scenario import IncidentGroup, Scenario

console = Console()


class ExternalScenarioExecutor(ScenarioExecutor):
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

        self.admin = KafkaAdmin(
            cluster_config.bootstrap_servers,
            cluster_config=cluster_config,
        )

    def _filter_infrastructure_incidents(
        self,
        scenarios: list[Scenario],
    ) -> list[Scenario]:
        filtered = []
        skipped_count = 0

        for scenario in scenarios:
            new_incidents = []
            for incident in scenario.incidents:
                if incident.type in INFRASTRUCTURE_INCIDENTS:
                    console.print(
                        f"[yellow]Skipping '{incident.type}' incident "
                        f"(not supported on external clusters)[/yellow]"
                    )
                    skipped_count += 1
                else:
                    new_incidents.append(incident)

            new_groups = []
            for group in scenario.incident_groups:
                new_group_incidents = []
                for incident in group.incidents:
                    if incident.type in INFRASTRUCTURE_INCIDENTS:
                        console.print(
                            f"[yellow]Skipping '{incident.type}' in group "
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
                )
            )

        if skipped_count > 0:
            console.print(
                f"[yellow]Note: {skipped_count} infrastructure incident(s) "
                f"will be skipped[/yellow]\n"
            )

        return filtered

    def _create_producers_for_topic(self, topic):
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

    def _create_consumers_for_topic(self, topic):
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
                    cluster_config=self.cluster_config,
                )
                self.consumers.append(consumer)
                self._consumers_by_topic[topic.name][group_id].append(consumer)
                consumers.append((group_id, f"{group_id}-consumer-{c + 1}", consumer))

        return consumers

    async def setup(self) -> None:
        if self.skip_topic_creation:
            console.print("[dim]Skipping topic creation (--skip-topic-creation)[/dim]")
            return

        await super().setup()
