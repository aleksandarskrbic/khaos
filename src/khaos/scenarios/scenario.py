from dataclasses import dataclass, field
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


@dataclass
class MessageSchemaConfig:
    key_distribution: str = "uniform"
    key_cardinality: int = 50
    min_size_bytes: int = 200
    max_size_bytes: int = 500
    fields: list[FieldSchema] | None = None


@dataclass
class ProducerConfigData:
    batch_size: int = 16384
    linger_ms: int = 5
    acks: str = "1"
    compression_type: str = "lz4"


@dataclass
class TopicConfig:
    name: str
    partitions: int = 12
    replication_factor: int = 3
    num_producers: int = 2
    num_consumer_groups: int = 1
    consumers_per_group: int = 2
    producer_rate: float = 1000.0
    consumer_delay_ms: int = 0
    message_schema: MessageSchemaConfig = field(default_factory=MessageSchemaConfig)
    producer_config: ProducerConfigData = field(default_factory=ProducerConfigData)


def _parse_schedule(data: dict[str, Any]) -> Schedule:
    return Schedule(
        at_seconds=data.get("at_seconds"),
        every_seconds=data.get("every_seconds"),
        initial_delay_seconds=data.get("initial_delay_seconds", 0),
    )


def _parse_consumer_target(data: dict[str, Any] | None) -> ConsumerTarget:
    if not data:
        return ConsumerTarget()
    return ConsumerTarget(
        topic=data.get("topic"),
        group=data.get("group"),
        percentage=data.get("percentage"),
        count=data.get("count"),
    )


def _parse_producer_target(data: dict[str, Any] | None) -> ProducerTarget:
    if not data:
        return ProducerTarget()
    return ProducerTarget(
        topic=data.get("topic"),
        percentage=data.get("percentage"),
        count=data.get("count"),
    )


def _parse_incident(data: dict[str, Any]) -> Incident:
    incident_type = data["type"]
    schedule = _parse_schedule(data)

    match incident_type:
        case "stop_broker":
            return StopBrokerIncident(broker=data["broker"], schedule=schedule)

        case "start_broker":
            return StartBrokerIncident(broker=data["broker"], schedule=schedule)

        case "pause_consumer":
            return PauseConsumers(
                duration_seconds=data["duration_seconds"],
                target=_parse_consumer_target(data.get("target")),
                schedule=schedule,
            )

        case "rebalance_consumer":
            return RebalanceConsumer(
                target=_parse_consumer_target(data.get("target")),
                schedule=schedule,
            )

        case "increase_consumer_delay":
            return IncreaseConsumerDelay(
                delay_ms=data["delay_ms"],
                target=_parse_consumer_target(data.get("target")),
                schedule=schedule,
            )

        case "change_producer_rate":
            return ChangeProducerRate(
                rate=data["rate"],
                target=_parse_producer_target(data.get("target")),
                schedule=schedule,
            )

        case _:
            raise ValueError(f"Unknown incident type: {incident_type}")


@dataclass
class Scenario:
    name: str
    description: str
    topics: list[TopicConfig] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    incident_groups: list[IncidentGroup] = field(default_factory=list)
    flows: list[FlowConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Scenario":
        topics = []
        for topic_data in data.get("topics", []):
            msg_schema_data = topic_data.pop("message_schema", {})

            fields_data = msg_schema_data.pop("fields", None)
            field_schemas = None
            if fields_data:
                field_schemas = [FieldSchema.from_dict(f) for f in fields_data]

            msg_schema = MessageSchemaConfig(**msg_schema_data, fields=field_schemas)

            prod_config_data = topic_data.pop("producer_config", {})
            prod_config = ProducerConfigData(**prod_config_data)

            topic = TopicConfig(
                **topic_data,
                message_schema=msg_schema,
                producer_config=prod_config,
            )
            topics.append(topic)

        incidents: list[Incident] = []
        incident_groups: list[IncidentGroup] = []
        for incident_data in data.get("incidents", []):
            if "group" in incident_data:
                group_data = incident_data["group"]
                group_incidents = [_parse_incident(inc) for inc in group_data.get("incidents", [])]
                group = IncidentGroup(
                    repeat=group_data.get("repeat", 1),
                    interval_seconds=group_data.get("interval_seconds", 60),
                    incidents=group_incidents,
                )
                incident_groups.append(group)
            else:
                incident = _parse_incident(incident_data)
                incidents.append(incident)

        flows = [FlowConfig.from_dict(f) for f in data.get("flows", [])]

        return cls(
            name=data["name"],
            description=data.get("description", ""),
            topics=topics,
            incidents=incidents,
            incident_groups=incident_groups,
            flows=flows,
        )
