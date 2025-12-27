"""Scenario data models and parsing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from khaos.models.flow import FlowConfig
from khaos.models.schema import FieldSchema
from khaos.scenarios.incidents import Incident, IncidentGroup
from khaos.scenarios.parser import ScenarioParser


@dataclass
class SchemaRegistryConfig:
    url: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaRegistryConfig:
        return cls(url=data["url"])


@dataclass
class MessageSchemaConfig:
    key_distribution: str = "uniform"
    key_cardinality: int = 50
    min_size_bytes: int = 200
    max_size_bytes: int = 500
    data_format: str = "json"  # "json" or "avro"
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
    schema_provider: str = "inline"  # "inline" or "registry"
    subject_name: str | None = None  # Required when schema_provider is "registry"


@dataclass
class Scenario:
    name: str
    description: str
    topics: list[TopicConfig] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    incident_groups: list[IncidentGroup] = field(default_factory=list)
    flows: list[FlowConfig] = field(default_factory=list)
    schema_registry: SchemaRegistryConfig | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Scenario:
        parser = ScenarioParser()

        # Parse topics
        topics = []
        for raw_topic in data.get("topics", []):
            topic_data = dict(raw_topic)  # Copy to avoid mutation

            msg_schema_data = topic_data.pop("message_schema", {})
            schema_data, field_schemas = parser.parse_message_schema(msg_schema_data)
            msg_schema = MessageSchemaConfig(**schema_data, fields=field_schemas)

            prod_config_data = topic_data.pop("producer_config", {})
            prod_config = ProducerConfigData(**prod_config_data)

            schema_provider = topic_data.pop("schema_provider", "inline")
            subject_name = topic_data.pop("subject_name", None)

            topic = TopicConfig(
                **topic_data,
                message_schema=msg_schema,
                producer_config=prod_config,
                schema_provider=schema_provider,
                subject_name=subject_name,
            )
            topics.append(topic)

        # Parse incidents
        incidents, incident_groups = parser.parse_incidents(data.get("incidents", []))

        # Parse flows
        flows = parser.parse_flows(data.get("flows", []))

        # Parse schema registry
        schema_registry = None
        if data.get("schema_registry"):
            schema_registry = SchemaRegistryConfig.from_dict(data["schema_registry"])

        return cls(
            name=data["name"],
            description=data.get("description", ""),
            topics=topics,
            incidents=incidents,
            incident_groups=incident_groups,
            flows=flows,
            schema_registry=schema_registry,
        )
