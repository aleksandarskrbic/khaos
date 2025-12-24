from unittest.mock import Mock

from khaos.scenarios.incidents import (
    ConsumerTarget,
    IncidentContext,
    ProducerTarget,
    select_consumers,
    select_producers,
)


def make_consumer(group_id: str, topics: list[str]):
    mock = Mock()
    mock.group_id = group_id
    mock.topics = topics
    return mock


def make_producer(topic: str):
    mock = Mock()
    mock.topic = topic
    return mock


def make_context(
    consumers=None,
    producers=None,
    consumers_by_topic=None,
    consumers_by_group=None,
    producers_by_topic=None,
):
    return IncidentContext(
        consumers=consumers or [],
        producers=producers or [],
        bootstrap_servers="localhost:9092",
        rebalance_count=0,
        consumers_by_topic=consumers_by_topic or {},
        consumers_by_group=consumers_by_group or {},
        producers_by_topic=producers_by_topic or {},
    )


class TestSelectConsumers:
    def test_no_filters_returns_all(self):
        c1 = make_consumer("g1", ["t1"])
        c2 = make_consumer("g2", ["t2"])
        ctx = make_context(consumers=[c1, c2])

        result = select_consumers(ctx, ConsumerTarget())

        assert result == [(0, c1), (1, c2)]

    def test_empty_consumers(self):
        ctx = make_context(consumers=[])

        result = select_consumers(ctx, ConsumerTarget())

        assert result == []

    def test_filter_by_topic(self):
        c1 = make_consumer("g1", ["events"])
        c2 = make_consumer("g1", ["orders"])
        c3 = make_consumer("g1", ["events"])
        ctx = make_context(
            consumers=[c1, c2, c3],
            consumers_by_topic={"events": [c1, c3], "orders": [c2]},
        )

        result = select_consumers(ctx, ConsumerTarget(topic="events"))

        assert result == [(0, c1), (2, c3)]

    def test_filter_by_topic_not_found(self):
        c1 = make_consumer("g1", ["events"])
        ctx = make_context(
            consumers=[c1],
            consumers_by_topic={"events": [c1]},
        )

        result = select_consumers(ctx, ConsumerTarget(topic="nonexistent"))

        assert result == []

    def test_filter_by_group(self):
        c1 = make_consumer("group-a", ["t"])
        c2 = make_consumer("group-b", ["t"])
        c3 = make_consumer("group-a", ["t"])
        ctx = make_context(
            consumers=[c1, c2, c3],
            consumers_by_group={"group-a": [c1, c3], "group-b": [c2]},
        )

        result = select_consumers(ctx, ConsumerTarget(group="group-a"))

        assert result == [(0, c1), (2, c3)]

    def test_filter_by_group_not_found(self):
        c1 = make_consumer("group-a", ["t"])
        ctx = make_context(
            consumers=[c1],
            consumers_by_group={"group-a": [c1]},
        )

        result = select_consumers(ctx, ConsumerTarget(group="nonexistent"))

        assert result == []

    def test_filter_by_topic_and_group(self):
        c1 = make_consumer("group-a", ["events"])
        c2 = make_consumer("group-b", ["events"])
        c3 = make_consumer("group-a", ["orders"])
        c4 = make_consumer("group-a", ["events"])
        ctx = make_context(
            consumers=[c1, c2, c3, c4],
            consumers_by_topic={"events": [c1, c2, c4], "orders": [c3]},
            consumers_by_group={"group-a": [c1, c3, c4], "group-b": [c2]},
        )

        result = select_consumers(ctx, ConsumerTarget(topic="events", group="group-a"))

        assert result == [(0, c1), (3, c4)]

    def test_filter_by_count(self):
        consumers = [make_consumer("g", ["t"]) for _ in range(5)]
        ctx = make_context(consumers=consumers)

        result = select_consumers(ctx, ConsumerTarget(count=2))

        assert len(result) == 2
        assert all(idx in range(5) for idx, _ in result)

    def test_filter_by_count_larger_than_available(self):
        c1 = make_consumer("g", ["t"])
        c2 = make_consumer("g", ["t"])
        ctx = make_context(consumers=[c1, c2])

        result = select_consumers(ctx, ConsumerTarget(count=10))

        assert len(result) == 2

    def test_filter_by_percentage(self):
        consumers = [make_consumer("g", ["t"]) for _ in range(10)]
        ctx = make_context(consumers=consumers)

        result = select_consumers(ctx, ConsumerTarget(percentage=50))

        assert len(result) == 5

    def test_filter_by_percentage_rounds_up_to_at_least_one(self):
        consumers = [make_consumer("g", ["t"]) for _ in range(10)]
        ctx = make_context(consumers=consumers)

        result = select_consumers(ctx, ConsumerTarget(percentage=5))

        assert len(result) >= 1

    def test_filter_by_topic_then_count(self):
        c1 = make_consumer("g", ["events"])
        c2 = make_consumer("g", ["orders"])
        c3 = make_consumer("g", ["events"])
        c4 = make_consumer("g", ["events"])
        ctx = make_context(
            consumers=[c1, c2, c3, c4],
            consumers_by_topic={"events": [c1, c3, c4], "orders": [c2]},
        )

        result = select_consumers(ctx, ConsumerTarget(topic="events", count=2))

        assert len(result) == 2
        assert all(c in [c1, c3, c4] for _, c in result)

    def test_filter_by_topic_then_percentage(self):
        events_consumers = [make_consumer("g", ["events"]) for _ in range(10)]
        other_consumer = make_consumer("g", ["orders"])
        ctx = make_context(
            consumers=[*events_consumers, other_consumer],
            consumers_by_topic={"events": events_consumers, "orders": [other_consumer]},
        )

        result = select_consumers(ctx, ConsumerTarget(topic="events", percentage=30))

        assert len(result) == 3
        assert all(c in events_consumers for _, c in result)


class TestSelectProducers:
    def test_no_filters_returns_all(self):
        p1 = make_producer("t1")
        p2 = make_producer("t2")
        ctx = make_context(producers=[p1, p2])

        result = select_producers(ctx, ProducerTarget())

        assert result == [(0, p1), (1, p2)]

    def test_empty_producers(self):
        ctx = make_context(producers=[])

        result = select_producers(ctx, ProducerTarget())

        assert result == []

    def test_filter_by_topic(self):
        p1 = make_producer("events")
        p2 = make_producer("orders")
        p3 = make_producer("events")
        ctx = make_context(
            producers=[p1, p2, p3],
            producers_by_topic={"events": [p1, p3], "orders": [p2]},
        )

        result = select_producers(ctx, ProducerTarget(topic="events"))

        assert result == [(0, p1), (2, p3)]

    def test_filter_by_topic_not_found(self):
        p1 = make_producer("events")
        ctx = make_context(
            producers=[p1],
            producers_by_topic={"events": [p1]},
        )

        result = select_producers(ctx, ProducerTarget(topic="nonexistent"))

        assert result == []

    def test_filter_by_count(self):
        producers = [make_producer("t") for _ in range(5)]
        ctx = make_context(producers=producers)

        result = select_producers(ctx, ProducerTarget(count=2))

        assert len(result) == 2
        assert all(idx in range(5) for idx, _ in result)

    def test_filter_by_count_larger_than_available(self):
        p1 = make_producer("t")
        p2 = make_producer("t")
        ctx = make_context(producers=[p1, p2])

        result = select_producers(ctx, ProducerTarget(count=10))

        assert len(result) == 2

    def test_filter_by_percentage(self):
        producers = [make_producer("t") for _ in range(10)]
        ctx = make_context(producers=producers)

        result = select_producers(ctx, ProducerTarget(percentage=50))

        assert len(result) == 5

    def test_filter_by_percentage_rounds_up_to_at_least_one(self):
        producers = [make_producer("t") for _ in range(10)]
        ctx = make_context(producers=producers)

        result = select_producers(ctx, ProducerTarget(percentage=5))

        assert len(result) >= 1

    def test_filter_by_topic_then_count(self):
        p1 = make_producer("events")
        p2 = make_producer("orders")
        p3 = make_producer("events")
        p4 = make_producer("events")
        ctx = make_context(
            producers=[p1, p2, p3, p4],
            producers_by_topic={"events": [p1, p3, p4], "orders": [p2]},
        )

        result = select_producers(ctx, ProducerTarget(topic="events", count=2))

        assert len(result) == 2
        assert all(p in [p1, p3, p4] for _, p in result)

    def test_filter_by_topic_then_percentage(self):
        events_producers = [make_producer("events") for _ in range(10)]
        other_producer = make_producer("orders")
        ctx = make_context(
            producers=[*events_producers, other_producer],
            producers_by_topic={"events": events_producers, "orders": [other_producer]},
        )

        result = select_producers(ctx, ProducerTarget(topic="events", percentage=30))

        assert len(result) == 3
        assert all(p in events_producers for _, p in result)
