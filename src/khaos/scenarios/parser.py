"""Scenario parsing from YAML/dict data."""

from __future__ import annotations

from typing import Any

from khaos.models.flow import FlowConfig
from khaos.models.schema import FieldSchema
from khaos.scenarios.incidents import (
    ChangeProducerRate,
    ConsumerTarget,
    Incident,
    IncidentGroup,
    IncreaseConsumerDelay,
    PauseConsumers,
    ProducerTarget,
    RebalanceConsumer,
    Schedule,
    StartBrokerIncident,
    StopBrokerIncident,
)


class ScenarioParser:
    def parse_schedule(self, data: dict[str, Any]) -> Schedule:
        return Schedule(
            at_seconds=data.get("at_seconds"),
            every_seconds=data.get("every_seconds"),
            initial_delay_seconds=data.get("initial_delay_seconds", 0),
        )

    def parse_consumer_target(self, data: dict[str, Any] | None) -> ConsumerTarget:
        if not data:
            return ConsumerTarget()
        return ConsumerTarget(
            topic=data.get("topic"),
            group=data.get("group"),
            percentage=data.get("percentage"),
            count=data.get("count"),
        )

    def parse_producer_target(self, data: dict[str, Any] | None) -> ProducerTarget:
        if not data:
            return ProducerTarget()
        return ProducerTarget(
            topic=data.get("topic"),
            percentage=data.get("percentage"),
            count=data.get("count"),
        )

    def parse_incident(self, data: dict[str, Any]) -> Incident:
        incident_type = data["type"]
        schedule = self.parse_schedule(data)

        match incident_type:
            case "stop_broker":
                return StopBrokerIncident(broker=data["broker"], schedule=schedule)

            case "start_broker":
                return StartBrokerIncident(broker=data["broker"], schedule=schedule)

            case "pause_consumer":
                return PauseConsumers(
                    duration_seconds=data["duration_seconds"],
                    target=self.parse_consumer_target(data.get("target")),
                    schedule=schedule,
                )

            case "rebalance_consumer":
                return RebalanceConsumer(
                    target=self.parse_consumer_target(data.get("target")),
                    schedule=schedule,
                )

            case "increase_consumer_delay":
                return IncreaseConsumerDelay(
                    delay_ms=data["delay_ms"],
                    target=self.parse_consumer_target(data.get("target")),
                    schedule=schedule,
                )

            case "change_producer_rate":
                return ChangeProducerRate(
                    rate=data["rate"],
                    target=self.parse_producer_target(data.get("target")),
                    schedule=schedule,
                )

            case _:
                raise ValueError(f"Unknown incident type: {incident_type}")

    def parse_incident_group(self, data: dict[str, Any]) -> IncidentGroup:
        group_incidents = [self.parse_incident(inc) for inc in data.get("incidents", [])]
        return IncidentGroup(
            repeat=data.get("repeat", 1),
            interval_seconds=data.get("interval_seconds", 60),
            incidents=group_incidents,
        )

    def parse_incidents(
        self, incidents_data: list[dict[str, Any]]
    ) -> tuple[list[Incident], list[IncidentGroup]]:
        incidents: list[Incident] = []
        incident_groups: list[IncidentGroup] = []

        for incident_data in incidents_data:
            if "group" in incident_data:
                group = self.parse_incident_group(incident_data["group"])
                incident_groups.append(group)
            else:
                incident = self.parse_incident(incident_data)
                incidents.append(incident)

        return incidents, incident_groups

    def parse_message_schema(
        self, data: dict[str, Any]
    ) -> tuple[dict[str, Any], list[FieldSchema] | None]:
        schema_data = dict(data)
        fields_data = schema_data.pop("fields", None)

        field_schemas = None
        if fields_data:
            field_schemas = [FieldSchema.from_dict(f) for f in fields_data]

        return schema_data, field_schemas

    def parse_flows(self, flows_data: list[dict[str, Any]]) -> list[FlowConfig]:
        return [FlowConfig.from_dict(f) for f in flows_data]


_parser = ScenarioParser()


def parse_schedule(data: dict[str, Any]) -> Schedule:
    return _parser.parse_schedule(data)


def parse_consumer_target(data: dict[str, Any] | None) -> ConsumerTarget:
    return _parser.parse_consumer_target(data)


def parse_producer_target(data: dict[str, Any] | None) -> ProducerTarget:
    return _parser.parse_producer_target(data)


def parse_incident(data: dict[str, Any]) -> Incident:
    return _parser.parse_incident(data)
