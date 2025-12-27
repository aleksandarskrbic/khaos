"""Factory for creating producers, consumers, and flow producers."""

from __future__ import annotations

from khaos.generators.flow import FlowProducer
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ProducerConfig
from khaos.models.flow import FlowConfig
from khaos.scenarios.scenario import TopicConfig


class SimulatorFactory:
    def __init__(
        self,
        bootstrap_servers: str,
        cluster_config: ClusterConfig | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.cluster_config = cluster_config

    def create_producer(self, config: ProducerConfig) -> ProducerSimulator:
        return ProducerSimulator(
            bootstrap_servers=self.bootstrap_servers,
            config=config,
            cluster_config=self.cluster_config,
        )

    def create_consumer(
        self,
        group_id: str,
        topics: list[str],
        processing_delay_ms: int = 0,
    ) -> ConsumerSimulator:
        return ConsumerSimulator(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            topics=topics,
            processing_delay_ms=processing_delay_ms,
            cluster_config=self.cluster_config,
        )

    def create_flow_producer(self, flow: FlowConfig) -> FlowProducer:
        return FlowProducer(
            flow=flow,
            bootstrap_servers=self.bootstrap_servers,
            cluster_config=self.cluster_config,
        )

    def create_producers_for_topic(
        self,
        topic: TopicConfig,
    ) -> list[tuple[str, ProducerSimulator]]:
        config = ProducerConfig(
            messages_per_second=topic.producer_rate,
            batch_size=topic.producer_config.batch_size,
            linger_ms=topic.producer_config.linger_ms,
            acks=topic.producer_config.acks,
            compression_type=topic.producer_config.compression_type,
        )

        producers = []
        for i in range(topic.num_producers):
            producer = self.create_producer(config)
            producers.append((f"{topic.name}-producer-{i + 1}", producer))

        return producers

    def create_consumers_for_topic(
        self,
        topic: TopicConfig,
    ) -> list[tuple[str, str, ConsumerSimulator]]:
        consumers = []

        for g in range(topic.num_consumer_groups):
            group_id = f"{topic.name}-group-{g + 1}"

            for c in range(topic.consumers_per_group):
                consumer = self.create_consumer(
                    group_id=group_id,
                    topics=[topic.name],
                    processing_delay_ms=topic.consumer_delay_ms,
                )
                consumers.append((group_id, f"{group_id}-consumer-{c + 1}", consumer))

        return consumers
