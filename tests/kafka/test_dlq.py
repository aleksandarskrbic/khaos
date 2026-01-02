"""Tests for the DLQ (Dead Letter Queue) module."""

import json
from unittest.mock import Mock

from khaos.kafka.dlq import DLQMessage


class TestDLQMessage:
    def test_from_kafka_message(self):
        # Create mock Kafka message
        mock_msg = Mock()
        mock_msg.topic.return_value = "orders"
        mock_msg.partition.return_value = 2
        mock_msg.offset.return_value = 12345
        mock_msg.value.return_value = b'{"order_id": "123"}'

        dlq_message = DLQMessage.from_kafka_message(mock_msg, "simulated_processing_failure")

        assert dlq_message.original_topic == "orders"
        assert dlq_message.original_partition == 2
        assert dlq_message.original_offset == 12345
        assert dlq_message.error == "simulated_processing_failure"
        assert dlq_message.payload == b'{"order_id": "123"}'
        # Timestamp should be an ISO format string
        assert "T" in dlq_message.timestamp

    def test_to_bytes_with_utf8_payload(self):
        dlq_message = DLQMessage(
            original_topic="orders",
            original_partition=1,
            original_offset=100,
            error="test_error",
            timestamp="2025-01-02T10:00:00Z",
            payload=b'{"key": "value"}',
        )

        result = dlq_message.to_bytes()
        data = json.loads(result)

        assert data["original_topic"] == "orders"
        assert data["original_partition"] == 1
        assert data["original_offset"] == 100
        assert data["error"] == "test_error"
        assert data["timestamp"] == "2025-01-02T10:00:00Z"
        assert data["payload"] == '{"key": "value"}'
        assert "payload_encoding" not in data

    def test_to_bytes_with_binary_payload(self):
        # Non-UTF8 bytes that can't be decoded
        binary_payload = bytes([0x80, 0x81, 0x82, 0xFF])

        dlq_message = DLQMessage(
            original_topic="images",
            original_partition=0,
            original_offset=50,
            error="processing_failed",
            timestamp="2025-01-02T11:00:00Z",
            payload=binary_payload,
        )

        result = dlq_message.to_bytes()
        data = json.loads(result)

        assert data["original_topic"] == "images"
        assert data["payload_encoding"] == "base64"
        # Verify it's valid base64
        import base64

        decoded = base64.b64decode(data["payload"])
        assert decoded == binary_payload

    def test_to_bytes_with_none_payload(self):
        dlq_message = DLQMessage(
            original_topic="events",
            original_partition=3,
            original_offset=999,
            error="null_payload",
            timestamp="2025-01-02T12:00:00Z",
            payload=None,
        )

        result = dlq_message.to_bytes()
        data = json.loads(result)

        assert data["payload"] is None
        assert "payload_encoding" not in data

    def test_dlq_message_fields(self):
        """Test that DLQMessage has all expected fields."""
        dlq_message = DLQMessage(
            original_topic="test-topic",
            original_partition=5,
            original_offset=42,
            error="test_error",
            timestamp="2025-01-02T00:00:00Z",
            payload=b"test",
        )

        # Verify dataclass fields
        assert hasattr(dlq_message, "original_topic")
        assert hasattr(dlq_message, "original_partition")
        assert hasattr(dlq_message, "original_offset")
        assert hasattr(dlq_message, "error")
        assert hasattr(dlq_message, "timestamp")
        assert hasattr(dlq_message, "payload")
