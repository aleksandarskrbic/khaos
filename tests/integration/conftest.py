"""Fixtures for integration tests using testcontainers."""

import time

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="module")
def docker_network(request):
    """Create a shared Docker network for containers."""
    network = Network()
    network.create()

    def cleanup():
        try:
            network.remove()
        except Exception:
            pass

    request.addfinalizer(cleanup)
    return network


@pytest.fixture(scope="module")
def kafka_container(docker_network, request):
    """Start Kafka container for integration tests."""
    kafka = (
        KafkaContainer("confluentinc/cp-kafka:7.5.0")
        .with_network(docker_network)
        .with_network_aliases("kafka")
    )
    kafka.start()

    def cleanup():
        try:
            kafka.stop()
        except Exception:
            pass

    request.addfinalizer(cleanup)
    return kafka


@pytest.fixture(scope="module")
def schema_registry_container(kafka_container, docker_network, request):
    """Start Schema Registry connected to Kafka."""
    registry = (
        DockerContainer("confluentinc/cp-schema-registry:7.5.0")
        .with_network(docker_network)
        .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_env(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
            "PLAINTEXT://kafka:9092",
        )
        .with_exposed_ports(8081)
    )
    registry.start()

    # Wait for Schema Registry to be ready
    wait_for_logs(registry, "Server started", timeout=60)
    time.sleep(2)  # Extra buffer for full initialization

    def cleanup():
        try:
            registry.stop()
        except Exception:
            pass

    request.addfinalizer(cleanup)
    return registry


@pytest.fixture(scope="module")
def schema_registry_url(schema_registry_container):
    """Get Schema Registry URL for tests."""
    host = schema_registry_container.get_container_host_ip()
    port = schema_registry_container.get_exposed_port(8081)
    return f"http://{host}:{port}"


@pytest.fixture
def avro_schema():
    """Sample Avro schema for testing."""
    return {
        "type": "record",
        "name": "TestRecord",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "count", "type": "long"},
            {"name": "amount", "type": "double"},
            {"name": "active", "type": "boolean"},
            {
                "name": "status",
                "type": {"type": "enum", "name": "Status", "symbols": ["PENDING", "ACTIVE"]},
            },
        ],
    }


@pytest.fixture
def complex_avro_schema():
    """Complex Avro schema with nested types."""
    return {
        "type": "record",
        "name": "Order",
        "namespace": "com.example",
        "fields": [
            {"name": "order_id", "type": {"type": "string", "logicalType": "uuid"}},
            {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "amount", "type": "double"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {
                "name": "customer",
                "type": {
                    "type": "record",
                    "name": "Customer",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": ["null", "string"]},
                    ],
                },
            },
        ],
    }


@pytest.fixture
def protobuf_schema():
    """Sample Protobuf schema for testing."""
    return """
syntax = "proto3";

message TestRecord {
    string id = 1;
    int64 count = 2;
    double amount = 3;
    bool active = 4;
    repeated string tags = 5;
}
"""
