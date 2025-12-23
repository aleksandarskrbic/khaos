from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka.admin import AdminClient, NewTopic

from khaos.errors import KhaosConnectionError, format_kafka_error

if TYPE_CHECKING:
    from khaos.models.cluster import ClusterConfig
    from khaos.models.topic import TopicConfig


class KafkaAdmin:
    def __init__(
        self,
        bootstrap_servers: str,
        cluster_config: ClusterConfig | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.cluster_config = cluster_config

        config = {
            "bootstrap.servers": bootstrap_servers,
            "log_level": 0,  # Disable librdkafka logging to stderr
            "logger": lambda *args: None,  # Silent logger callback
        }

        # Merge security configuration if provided
        if cluster_config:
            security_config = cluster_config.to_kafka_config()
            security_config.pop("bootstrap.servers", None)
            config.update(security_config)

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

        for _topic_name, future in futures.items():
            try:
                future.result()
            except Exception as e:
                # Ignore "already exists" errors
                if "already exists" not in str(e).lower():
                    raise

    def delete_topic(self, topic_name: str) -> None:
        futures = self._client.delete_topics([topic_name])

        for _name, future in futures.items():
            try:
                future.result()
            except Exception as e:
                if "unknown topic" not in str(e).lower():
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
