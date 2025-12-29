from __future__ import annotations

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError

from khaos.errors import KhaosConnectionError, format_kafka_error
from khaos.kafka.config import build_kafka_config
from khaos.models.cluster import ClusterConfig
from khaos.models.topic import TopicConfig

logger = logging.getLogger(__name__)


class KafkaAdmin:
    def __init__(
        self,
        bootstrap_servers: str,
        cluster_config: ClusterConfig | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.cluster_config = cluster_config

        config = build_kafka_config(bootstrap_servers, cluster_config)

        try:
            self._client = AdminClient(config)
            # Test connection immediately to fail fast
            self._client.list_topics(timeout=10)
        except Exception as e:
            raise KhaosConnectionError(
                f"Cannot connect to Kafka at {bootstrap_servers}: {format_kafka_error(e)}"
            )

    def create_topic(self, config: TopicConfig) -> None:
        topic = NewTopic(
            config.name,
            num_partitions=config.partitions,
            replication_factor=config.replication_factor,
            config={
                "retention.ms": str(config.retention_ms),
            },
        )

        futures = self._client.create_topics([topic])

        for _, future in futures.items():
            try:
                future.result()
            except KafkaException as e:
                # Ignore "already exists" errors
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise

    def delete_topic(self, topic_name: str) -> None:
        futures = self._client.delete_topics([topic_name])

        for _, future in futures.items():
            try:
                future.result()
            except KafkaException as e:
                # Ignore "unknown topic" errors
                if e.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise

    def topic_exists(self, topic_name: str) -> bool:
        metadata = self._client.list_topics(timeout=10)
        return topic_name in metadata.topics

    def get_topic_partitions(self, topic_name: str) -> int:
        metadata = self._client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            raise ValueError(f"Topic {topic_name} does not exist")
        return len(metadata.topics[topic_name].partitions)

    def list_topics(self) -> list[str]:
        metadata = self._client.list_topics(timeout=10)
        return [name for name in metadata.topics if not name.startswith("_")]
