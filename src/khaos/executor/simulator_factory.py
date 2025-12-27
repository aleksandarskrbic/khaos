"""Factory for creating producers, consumers, and flow producers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from khaos.generators.flow import FlowProducer
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.models.cluster import ClusterConfig
from khaos.models.config import ConsumerConfig, ProducerConfig
from khaos.models.flow import FlowConfig
from khaos.scenarios.scenario import TopicConfig


class SimulatorFactory:
    def __init__(
        self,
        bootstrap_servers: str,
        executor: ThreadPoolExecutor,
        cluster_config: ClusterConfig | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.executor = executor
        self.cluster_config = cluster_config

    def create_producer(self, config: ProducerConfig) -> ProducerSimulator:
        return ProducerSimulator(
            bootstrap_servers=self.bootstrap_servers,
            executor=self.executor,
            config=config,
            cluster_config=self.cluster_config,
        )

    def create_consumer(self, topics: list[str], config: ConsumerConfig) -> ConsumerSimulator:
        return ConsumerSimulator(
            bootstrap_servers=self.bootstrap_servers,
            topics=topics,
            config=config,
            executor=self.executor,
            cluster_config=self.cluster_config,
        )

    def create_flow_producer(self, flow: FlowConfig) -> FlowProducer:
        return FlowProducer(
            flow=flow,
            bootstrap_servers=self.bootstrap_servers,
            executor=self.executor,
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
            config = ConsumerConfig(
                group_id=group_id,
                processing_delay_ms=topic.consumer_delay_ms,
            )

            for c in range(topic.consumers_per_group):
                consumer = self.create_consumer(topics=[topic.name], config=config)
                consumers.append((group_id, f"{group_id}-consumer-{c + 1}", consumer))

        return consumers
