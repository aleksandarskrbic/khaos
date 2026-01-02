"""Dead Letter Queue (DLQ) producer for failed message handling."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from confluent_kafka import Producer

from khaos.defaults import FLUSH_TIMEOUT_SECONDS
from khaos.kafka.config import build_kafka_config
from khaos.models.cluster import ClusterConfig

logger = logging.getLogger(__name__)


@dataclass
class DLQMessage:
    """Represents a message being sent to the Dead Letter Queue."""

    original_topic: str
    original_partition: int
    original_offset: int
    error: str
    timestamp: str
    payload: bytes | None

    @classmethod
    def from_kafka_message(cls, msg, error: str) -> DLQMessage:
        """Create a DLQMessage from a Kafka message."""
        return cls(
            original_topic=msg.topic(),
            original_partition=msg.partition(),
            original_offset=msg.offset(),
            error=error,
            timestamp=datetime.now(UTC).isoformat(),
            payload=msg.value(),
        )

    def to_bytes(self) -> bytes:
        """Serialize the DLQ message to bytes."""
        data = asdict(self)
        # Handle bytes payload - decode to string or base64 encode
        if data["payload"] is not None:
            try:
                data["payload"] = data["payload"].decode("utf-8")
            except UnicodeDecodeError:
                import base64

                data["payload"] = base64.b64encode(data["payload"]).decode("ascii")
                data["payload_encoding"] = "base64"
        return json.dumps(data).encode("utf-8")


class DLQProducer:
    """Producer for sending failed messages to a Dead Letter Queue topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        cluster_config: ClusterConfig | None = None,
    ):
        """Initialize the DLQ producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            cluster_config: Optional cluster configuration for authentication
        """
        self._bootstrap_servers = bootstrap_servers
        self._cluster_config = cluster_config
        self._producer: Producer | None = None

    def _get_producer(self) -> Producer:
        """Lazily initialize the Kafka producer."""
        if self._producer is None:
            config = build_kafka_config(
                self._bootstrap_servers,
                self._cluster_config,
                **{
                    "acks": "all",
                    "retries": 3,
                },
            )
            self._producer = Producer(config)
        return self._producer

    def _get_dlq_topic(self, original_topic: str) -> str:
        """Get the DLQ topic name for a given original topic."""
        return f"{original_topic}-dlq"

    def send_to_dlq(self, msg, error: str) -> None:
        """Send a failed message to the DLQ.

        Args:
            msg: The original Kafka message that failed processing
            error: Description of the error that occurred
        """
        dlq_message = DLQMessage.from_kafka_message(msg, error)
        dlq_topic = self._get_dlq_topic(dlq_message.original_topic)

        producer = self._get_producer()
        producer.produce(
            topic=dlq_topic,
            value=dlq_message.to_bytes(),
            callback=self._delivery_callback,
        )
        producer.poll(0)

    def _delivery_callback(self, err, msg) -> None:
        """Handle delivery confirmation for DLQ messages."""
        if err:
            logger.error(f"Failed to send message to DLQ: {err}")

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> int:
        """Flush any pending DLQ messages."""
        if self._producer is not None:
            return self._producer.flush(timeout)
        return 0

    def close(self) -> None:
        """Close the DLQ producer."""
        if self._producer is not None:
            self.flush()
            self._producer = None
