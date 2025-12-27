"""External executor for connecting to external Kafka clusters."""

from __future__ import annotations

from rich.console import Console

from khaos.executor.base import BaseExecutor
from khaos.executor.topic_manager import TopicManager
from khaos.kafka.admin import KafkaAdmin
from khaos.models.cluster import ClusterConfig
from khaos.scenarios.incidents import IncidentGroup, StartBrokerIncident, StopBrokerIncident
from khaos.scenarios.scenario import Scenario

console = Console()


def _is_infrastructure_incident(incident: object) -> bool:
    return isinstance(incident, (StopBrokerIncident, StartBrokerIncident))


def _get_incident_type_name(incident: object) -> str:
    return type(incident).__name__


class ExternalExecutor(BaseExecutor):
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
        self.topic_manager = TopicManager(cluster_config.bootstrap_servers)

    def _is_schema_registry_running(self) -> bool:
        return any(s.schema_registry for s in self.scenarios)

    async def _handle_stop_broker(self, broker: str) -> None:
        """Broker incidents are filtered out for external clusters"""
        pass

    async def _handle_start_broker(self, broker: str) -> None:
        """Broker incidents are filtered out for external clusters"""
        pass

    def _filter_infrastructure_incidents(
        self,
        scenarios: list[Scenario],
    ) -> list[Scenario]:
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

    async def setup(self) -> None:
        if self.skip_topic_creation:
            console.print("[dim]Skipping topic creation (--skip-topic-creation)[/dim]")
            return

        await super().setup()
