"""Topic management for Kafka scenarios."""

from __future__ import annotations

import asyncio

from rich.console import Console

from khaos.defaults import (
    DEFAULT_FLOW_PARTITIONS,
    DEFAULT_REPLICATION_FACTOR,
    TOPIC_CREATION_WAIT_SECONDS,
)
from khaos.kafka.admin import KafkaAdmin
from khaos.models.flow import FlowConfig
from khaos.models.topic import TopicConfig as KafkaTopicConfig
from khaos.scenarios.scenario import TopicConfig

console = Console()


class TopicManager:
    def __init__(self, bootstrap_servers: str):
        self.admin = KafkaAdmin(bootstrap_servers)

    def create_topic(self, name: str, partitions: int, replication_factor: int) -> None:
        topic_config = KafkaTopicConfig(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
        )
        self.admin.delete_topic(name)
        self.admin.create_topic(topic_config)

    def delete_topic(self, name: str) -> None:
        self.admin.delete_topic(name)

    async def setup_topics(
        self,
        topics: list[TopicConfig],
        flows: list[FlowConfig],
    ) -> set[str]:
        created_topics: set[str] = set()

        for topic in topics:
            console.print(
                f"[dim]Creating topic: {topic.name} ({topic.partitions} partitions)[/dim]"
            )
            self.create_topic(
                name=topic.name,
                partitions=topic.partitions,
                replication_factor=topic.replication_factor,
            )
            created_topics.add(topic.name)

        for flow in flows:
            for topic_name in flow.get_all_topics():
                if topic_name not in created_topics:
                    console.print(f"[dim]Creating topic for flow: {topic_name}[/dim]")
                    self.create_topic(
                        name=topic_name,
                        partitions=DEFAULT_FLOW_PARTITIONS,
                        replication_factor=DEFAULT_REPLICATION_FACTOR,
                    )
                    created_topics.add(topic_name)

        if created_topics:
            await asyncio.sleep(TOPIC_CREATION_WAIT_SECONDS)

        return created_topics
