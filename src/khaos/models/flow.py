from __future__ import annotations

from dataclasses import dataclass, field

from khaos.models.schema import FieldSchema


@dataclass
class CorrelationConfig:
    type: str  # "uuid" or "field_ref"
    field: str | None = None  # field name if type is "field_ref"

    @classmethod
    def from_dict(cls, data: dict) -> CorrelationConfig:
        return cls(
            type=data.get("type", "uuid"),
            field=data.get("field"),
        )


@dataclass
class StepConsumerConfig:
    groups: int = 1  # number of consumer groups
    per_group: int = 1  # consumers per group
    delay_ms: int = 0  # processing delay per message

    @classmethod
    def from_dict(cls, data: dict) -> StepConsumerConfig:
        return cls(
            groups=data.get("groups", 1),
            per_group=data.get("per_group", 1),
            delay_ms=data.get("delay_ms", 0),
        )


@dataclass
class FlowStep:
    topic: str
    event_type: str
    delay_ms: int = 0  # delay after previous step
    fields: list[FieldSchema] | None = None
    consumers: StepConsumerConfig | None = None  # optional consumers

    @classmethod
    def from_dict(cls, data: dict) -> FlowStep:
        fields_data = data.get("fields")
        field_schemas = None
        if fields_data:
            field_schemas = [FieldSchema.from_dict(f) for f in fields_data]

        consumers_data = data.get("consumers")
        consumers = None
        if consumers_data:
            consumers = StepConsumerConfig.from_dict(consumers_data)

        return cls(
            topic=data["topic"],
            event_type=data["event_type"],
            delay_ms=data.get("delay_ms", 0),
            fields=field_schemas,
            consumers=consumers,
        )


@dataclass
class FlowConfig:
    name: str
    rate: float  # flow instances per second
    steps: list[FlowStep] = field(default_factory=list)
    correlation: CorrelationConfig = field(default_factory=lambda: CorrelationConfig(type="uuid"))

    @classmethod
    def from_dict(cls, data: dict) -> FlowConfig:
        steps = [FlowStep.from_dict(s) for s in data.get("steps", [])]

        correlation_data = data.get("correlation", {"type": "uuid"})
        correlation = CorrelationConfig.from_dict(correlation_data)

        return cls(
            name=data["name"],
            rate=data.get("rate", 10.0),
            steps=steps,
            correlation=correlation,
        )

    def get_all_topics(self) -> list[str]:
        return list({step.topic for step in self.steps})
