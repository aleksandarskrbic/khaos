from __future__ import annotations

from typing import Any

from khaos.models.cluster import ClusterConfig


def build_kafka_config(
    bootstrap_servers: str,
    cluster_config: ClusterConfig | None = None,
    **extra: Any,
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "log_level": 0,
        "logger": lambda *_: None,
        **extra,
    }
    if cluster_config:
        security = cluster_config.to_kafka_config()
        security.pop("bootstrap.servers", None)
        config.update(security)
    return config
