"""Scenario data class - represents a scenario loaded from YAML."""

from dataclasses import dataclass, field
from typing import Any

from khaos.models.schema import FieldSchema


@dataclass
class MessageSchemaConfig:
    """Message schema configuration."""

    key_distribution: str = "uniform"
    key_cardinality: int = 50
    min_size_bytes: int = 200
    max_size_bytes: int = 500
    # Structured field schemas (optional - if not set, use random bytes)
    fields: list[FieldSchema] | None = None


@dataclass
class ProducerConfigData:
    """Producer configuration."""

    batch_size: int = 16384
    linger_ms: int = 5
    acks: str = "1"
    compression_type: str = "lz4"


@dataclass
class TopicConfig:
    """Topic configuration for a scenario."""

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


@dataclass
class Incident:
    """Incident trigger configuration."""

    type: str
    at_seconds: int | None = None
    every_seconds: int | None = None
    initial_delay_seconds: int = 0
    # Type-specific params
    delay_ms: int | None = None
    broker: str | None = None
    rate: float | None = None
    duration_seconds: int | None = None


@dataclass
class IncidentGroup:
    """A group of incidents that repeat together."""

    repeat: int  # Number of times to repeat
    interval_seconds: int  # Time between each cycle start
    incidents: list[Incident] = field(default_factory=list)


@dataclass
class Scenario:
    """A scenario loaded from YAML.

    Pure data class - no execution logic.
    """

    name: str
    description: str
    topics: list[TopicConfig] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    incident_groups: list[IncidentGroup] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Scenario":
        """Create a Scenario from a dictionary (parsed YAML)."""
        topics = []
        for topic_data in data.get("topics", []):
            # Parse nested configs
            msg_schema_data = topic_data.pop("message_schema", {})

            # Parse field schemas if present
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

        incidents = []
        incident_groups = []
        for incident_data in data.get("incidents", []):
            # Check if this is a group
            if "group" in incident_data:
                group_data = incident_data["group"]
                group_incidents = [Incident(**inc) for inc in group_data.get("incidents", [])]
                group = IncidentGroup(
                    repeat=group_data.get("repeat", 1),
                    interval_seconds=group_data.get("interval_seconds", 60),
                    incidents=group_incidents,
                )
                incident_groups.append(group)
            else:
                incident = Incident(**incident_data)
                incidents.append(incident)

        return cls(
            name=data["name"],
            description=data.get("description", ""),
            topics=topics,
            incidents=incidents,
            incident_groups=incident_groups,
        )
