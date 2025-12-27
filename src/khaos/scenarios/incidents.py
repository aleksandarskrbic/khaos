from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import ClassVar

from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator


@dataclass
class SetConsumerDelay:
    index: int
    delay_ms: int


@dataclass
class SetProducerRate:
    index: int
    rate: float


@dataclass
class IncrementRebalanceCount:
    pass


@dataclass
class StopConsumers:
    indices: list[int]


@dataclass
class ResumeConsumers:
    indices: list[int]


@dataclass
class StopConsumer:
    index: int


@dataclass
class CreateConsumer:
    index: int
    group_id: str
    topics: list[str]
    processing_delay_ms: int


@dataclass
class StopBroker:
    broker: str


@dataclass
class StartBroker:
    broker: str


@dataclass
class Delay:
    seconds: float


@dataclass
class PrintMessage:
    message: str
    style: str = "bold red"


Command = (
    SetConsumerDelay
    | SetProducerRate
    | IncrementRebalanceCount
    | StopConsumers
    | ResumeConsumers
    | StopConsumer
    | CreateConsumer
    | StopBroker
    | StartBroker
    | Delay
    | PrintMessage
)


@dataclass
class ConsumerTarget:
    topic: str | None = None
    group: str | None = None
    percentage: int | None = None
    count: int | None = None


@dataclass
class ProducerTarget:
    topic: str | None = None
    percentage: int | None = None
    count: int | None = None


@dataclass
class Schedule:
    at_seconds: int | None = None
    every_seconds: int | None = None
    initial_delay_seconds: int = 0


@dataclass
class IncidentContext:
    consumers: list[ConsumerSimulator]
    producers: list[ProducerSimulator]
    bootstrap_servers: str
    rebalance_count: int
    consumers_by_topic: dict[str, list[ConsumerSimulator]]
    consumers_by_group: dict[str, list[ConsumerSimulator]]
    producers_by_topic: dict[str, list[ProducerSimulator]]


def select_consumers(
    ctx: IncidentContext, target: ConsumerTarget
) -> list[tuple[int, ConsumerSimulator]]:
    candidates = list(enumerate(ctx.consumers))

    if target.topic:
        topic_consumers = set(ctx.consumers_by_topic.get(target.topic, []))
        candidates = [(i, c) for i, c in candidates if c in topic_consumers]

    if target.group:
        group_consumers = set(ctx.consumers_by_group.get(target.group, []))
        candidates = [(i, c) for i, c in candidates if c in group_consumers]

    if target.count is not None:
        return random.sample(candidates, min(target.count, len(candidates)))

    if target.percentage is not None:
        n = max(1, len(candidates) * target.percentage // 100)
        return random.sample(candidates, min(n, len(candidates)))

    return candidates


def select_producers(
    ctx: IncidentContext, target: ProducerTarget
) -> list[tuple[int, ProducerSimulator]]:
    candidates = list(enumerate(ctx.producers))

    if target.topic:
        topic_producers = set(ctx.producers_by_topic.get(target.topic, []))
        candidates = [(i, p) for i, p in candidates if p in topic_producers]

    if target.count is not None:
        return random.sample(candidates, min(target.count, len(candidates)))

    if target.percentage is not None:
        n = max(1, len(candidates) * target.percentage // 100)
        return random.sample(candidates, min(n, len(candidates)))

    return candidates


@dataclass
class Incident:
    type_name: ClassVar[str]
    schedule: Schedule = field(default_factory=Schedule)

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        raise NotImplementedError


@dataclass
class IncidentGroup:
    repeat: int
    interval_seconds: int
    incidents: list[Incident] = field(default_factory=list)


@dataclass
class StopBrokerIncident(Incident):
    type_name: ClassVar[str] = "stop_broker"
    broker: str = ""

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        return [
            PrintMessage(f">>> INCIDENT: Stopping {self.broker}"),
            PrintMessage("ISR will shrink, leadership will change", style="yellow"),
            StopBroker(broker=self.broker),
        ]


@dataclass
class StartBrokerIncident(Incident):
    type_name: ClassVar[str] = "start_broker"
    broker: str = ""

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        return [
            PrintMessage(f">>> RECOVERY: Starting {self.broker}", style="bold green"),
            PrintMessage("ISR will expand back", style="yellow"),
            StartBroker(broker=self.broker),
        ]


@dataclass
class PauseConsumers(Incident):
    type_name: ClassVar[str] = "pause_consumer"
    duration_seconds: int = 0
    target: ConsumerTarget = field(default_factory=ConsumerTarget)

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        selected = select_consumers(ctx, self.target)
        if not selected:
            return [PrintMessage(">>> No consumers matched target", style="yellow")]
        indices = [i for i, _ in selected]
        msg = f">>> INCIDENT: Pausing {len(indices)} consumers for {self.duration_seconds}s"
        return [
            PrintMessage(msg),
            StopConsumers(indices=indices),
            Delay(seconds=self.duration_seconds),
            PrintMessage(">>> Consumers resuming", style="bold green"),
            ResumeConsumers(indices=indices),
        ]


@dataclass
class RebalanceConsumer(Incident):
    type_name: ClassVar[str] = "rebalance_consumer"
    target: ConsumerTarget = field(default_factory=ConsumerTarget)

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        selected = select_consumers(ctx, self.target)
        if not selected:
            return [PrintMessage(">>> No consumers matched target", style="yellow")]
        idx, consumer = random.choice(selected)
        rebalance_num = ctx.rebalance_count + 1
        return [
            PrintMessage(f">>> REBALANCE #{rebalance_num}: Closing consumer {idx + 1}"),
            IncrementRebalanceCount(),
            StopConsumer(index=idx),
            Delay(seconds=3),
            CreateConsumer(
                index=idx,
                group_id=consumer.config.group_id,
                topics=consumer.topics,
                processing_delay_ms=consumer.config.processing_delay_ms,
            ),
            PrintMessage(f">>> Consumer {idx + 1} rejoined group", style="yellow"),
        ]


@dataclass
class IncreaseConsumerDelay(Incident):
    type_name: ClassVar[str] = "increase_consumer_delay"
    delay_ms: int = 0
    target: ConsumerTarget = field(default_factory=ConsumerTarget)

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        selected = select_consumers(ctx, self.target)
        if not selected:
            return [PrintMessage(">>> No consumers matched target", style="yellow")]
        msg = f">>> INCIDENT: Setting delay to {self.delay_ms}ms for {len(selected)} consumers"
        return [
            PrintMessage(msg),
            *[SetConsumerDelay(index=i, delay_ms=self.delay_ms) for i, _ in selected],
        ]


@dataclass
class ChangeProducerRate(Incident):
    type_name: ClassVar[str] = "change_producer_rate"
    rate: float = 0.0
    target: ProducerTarget = field(default_factory=ProducerTarget)

    def get_commands(self, ctx: IncidentContext) -> list[Command]:
        selected = select_producers(ctx, self.target)
        if not selected:
            return [PrintMessage(">>> No producers matched target", style="yellow")]
        return [
            PrintMessage(
                f">>> INCIDENT: Changing rate to {self.rate} msg/s for {len(selected)} producers",
                style="bold yellow",
            ),
            *[SetProducerRate(index=i, rate=self.rate) for i, _ in selected],
        ]


def get_incident_names() -> set[str]:
    return {cls.type_name for cls in Incident.__subclasses__()}
