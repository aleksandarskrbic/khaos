from unittest.mock import Mock

from khaos.scenarios.incidents import (
    CreateConsumer,
    Delay,
    IncidentContext,
    IncrementRebalanceCount,
    PrintMessage,
    ResumeConsumers,
    SetConsumerDelay,
    SetProducerRate,
    StartBroker,
    StopBroker,
    StopConsumer,
    StopConsumers,
)
from khaos.scenarios.parser import parse_incident


def make_consumer(group_id: str, topics: list[str], delay_ms: int = 0):
    mock = Mock()
    mock.config.group_id = group_id
    mock.topics = topics
    mock.config.processing_delay_ms = delay_ms
    return mock


def make_context(
    consumers=None,
    producers=None,
    consumers_by_topic=None,
    consumers_by_group=None,
    producers_by_topic=None,
    rebalance_count=0,
):
    return IncidentContext(
        consumers=consumers or [],
        producers=producers or [],
        bootstrap_servers="localhost:9092",
        rebalance_count=rebalance_count,
        consumers_by_topic=consumers_by_topic or {},
        consumers_by_group=consumers_by_group or {},
        producers_by_topic=producers_by_topic or {},
    )


class TestStopBrokerIncident:
    def test_returns_commands(self):
        incident = parse_incident({"type": "stop_broker", "broker": "kafka-1", "at_seconds": 10})
        ctx = make_context()

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> INCIDENT: Stopping kafka-1"),
            PrintMessage("ISR will shrink, leadership will change", style="yellow"),
            StopBroker(broker="kafka-1"),
        ]


class TestStartBrokerIncident:
    def test_returns_commands(self):
        incident = parse_incident({"type": "start_broker", "broker": "kafka-2", "at_seconds": 30})
        ctx = make_context()

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> RECOVERY: Starting kafka-2", style="bold green"),
            PrintMessage("ISR will expand back", style="yellow"),
            StartBroker(broker="kafka-2"),
        ]


class TestPauseConsumers:
    def test_pauses_all_consumers(self):
        incident = parse_incident(
            {
                "type": "pause_consumer",
                "duration_seconds": 10,
                "at_seconds": 5,
            }
        )
        consumers = [make_consumer("g", ["t"]) for _ in range(3)]
        ctx = make_context(consumers=consumers)

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> INCIDENT: Pausing 3 consumers for 10s"),
            StopConsumers(indices=[0, 1, 2]),
            Delay(seconds=10),
            PrintMessage(">>> Consumers resuming", style="bold green"),
            ResumeConsumers(indices=[0, 1, 2]),
        ]

    def test_no_consumers_matched(self):
        incident = parse_incident(
            {
                "type": "pause_consumer",
                "duration_seconds": 10,
                "at_seconds": 5,
            }
        )
        ctx = make_context(consumers=[])

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> No consumers matched target", style="yellow"),
        ]

    def test_by_topic(self):
        incident = parse_incident(
            {
                "type": "pause_consumer",
                "duration_seconds": 5,
                "at_seconds": 10,
                "target": {"topic": "events"},
            }
        )
        c1 = make_consumer("g", ["events"])
        c2 = make_consumer("g", ["orders"])
        c3 = make_consumer("g", ["events"])
        ctx = make_context(
            consumers=[c1, c2, c3],
            consumers_by_topic={"events": [c1, c3], "orders": [c2]},
        )

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> INCIDENT: Pausing 2 consumers for 5s"),
            StopConsumers(indices=[0, 2]),
            Delay(seconds=5),
            PrintMessage(">>> Consumers resuming", style="bold green"),
            ResumeConsumers(indices=[0, 2]),
        ]

    def test_by_group(self):
        incident = parse_incident(
            {
                "type": "pause_consumer",
                "duration_seconds": 5,
                "at_seconds": 10,
                "target": {"group": "group-a"},
            }
        )
        c1 = make_consumer("group-a", ["t"])
        c2 = make_consumer("group-b", ["t"])
        c3 = make_consumer("group-a", ["t"])
        ctx = make_context(
            consumers=[c1, c2, c3],
            consumers_by_group={"group-a": [c1, c3], "group-b": [c2]},
        )

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> INCIDENT: Pausing 2 consumers for 5s"),
            StopConsumers(indices=[0, 2]),
            Delay(seconds=5),
            PrintMessage(">>> Consumers resuming", style="bold green"),
            ResumeConsumers(indices=[0, 2]),
        ]

    def test_by_count(self):
        incident = parse_incident(
            {
                "type": "pause_consumer",
                "duration_seconds": 5,
                "at_seconds": 10,
                "target": {"count": 2},
            }
        )
        consumers = [make_consumer("g", ["t"]) for _ in range(5)]
        ctx = make_context(consumers=consumers)

        commands = incident.get_commands(ctx)
        assert len(commands) == 5
        assert len(commands[1].indices) == 2

    def test_by_percentage(self):
        incident = parse_incident(
            {
                "type": "pause_consumer",
                "duration_seconds": 5,
                "at_seconds": 10,
                "target": {"percentage": 50},
            }
        )
        consumers = [make_consumer("g", ["t"]) for _ in range(10)]
        ctx = make_context(consumers=consumers)

        commands = incident.get_commands(ctx)
        assert len(commands[1].indices) == 5


class TestRebalanceConsumer:
    def test_returns_commands(self):
        incident = parse_incident({"type": "rebalance_consumer", "every_seconds": 20})
        consumer = make_consumer("my-group", ["my-topic"], delay_ms=50)
        ctx = make_context(consumers=[consumer], rebalance_count=3)

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> REBALANCE #4: Closing consumer 1"),
            IncrementRebalanceCount(),
            StopConsumer(index=0),
            Delay(seconds=3),
            CreateConsumer(
                index=0,
                group_id="my-group",
                topics=["my-topic"],
                processing_delay_ms=50,
            ),
            PrintMessage(">>> Consumer 1 rejoined group", style="yellow"),
        ]

    def test_no_consumers(self):
        incident = parse_incident({"type": "rebalance_consumer", "every_seconds": 20})
        ctx = make_context(consumers=[])

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> No consumers matched target", style="yellow"),
        ]


class TestIncreaseConsumerDelay:
    def test_sets_delay_for_all(self):
        incident = parse_incident(
            {
                "type": "increase_consumer_delay",
                "delay_ms": 100,
                "at_seconds": 30,
            }
        )
        consumers = [make_consumer("g", ["t"]) for _ in range(3)]
        ctx = make_context(consumers=consumers)

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> INCIDENT: Setting delay to 100ms for 3 consumers"),
            SetConsumerDelay(index=0, delay_ms=100),
            SetConsumerDelay(index=1, delay_ms=100),
            SetConsumerDelay(index=2, delay_ms=100),
        ]

    def test_by_topic(self):
        incident = parse_incident(
            {
                "type": "increase_consumer_delay",
                "delay_ms": 200,
                "at_seconds": 30,
                "target": {"topic": "slow-topic"},
            }
        )
        c1 = make_consumer("g", ["slow-topic"])
        c2 = make_consumer("g", ["fast-topic"])
        ctx = make_context(
            consumers=[c1, c2],
            consumers_by_topic={"slow-topic": [c1], "fast-topic": [c2]},
        )

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> INCIDENT: Setting delay to 200ms for 1 consumers"),
            SetConsumerDelay(index=0, delay_ms=200),
        ]


class TestChangeProducerRate:
    def test_changes_rate_for_all(self):
        incident = parse_incident(
            {
                "type": "change_producer_rate",
                "rate": 500.0,
                "at_seconds": 20,
            }
        )
        producers = [Mock(), Mock()]
        ctx = make_context(producers=producers)

        assert incident.get_commands(ctx) == [
            PrintMessage(
                ">>> INCIDENT: Changing rate to 500.0 msg/s for 2 producers",
                style="bold yellow",
            ),
            SetProducerRate(index=0, rate=500.0),
            SetProducerRate(index=1, rate=500.0),
        ]

    def test_by_topic(self):
        incident = parse_incident(
            {
                "type": "change_producer_rate",
                "rate": 100.0,
                "at_seconds": 20,
                "target": {"topic": "orders"},
            }
        )
        p1, p2, p3 = Mock(), Mock(), Mock()
        ctx = make_context(
            producers=[p1, p2, p3],
            producers_by_topic={"orders": [p1, p3], "events": [p2]},
        )

        assert incident.get_commands(ctx) == [
            PrintMessage(
                ">>> INCIDENT: Changing rate to 100.0 msg/s for 2 producers",
                style="bold yellow",
            ),
            SetProducerRate(index=0, rate=100.0),
            SetProducerRate(index=2, rate=100.0),
        ]

    def test_no_producers(self):
        incident = parse_incident(
            {
                "type": "change_producer_rate",
                "rate": 100.0,
                "at_seconds": 20,
            }
        )
        ctx = make_context(producers=[])

        assert incident.get_commands(ctx) == [
            PrintMessage(">>> No producers matched target", style="yellow"),
        ]
