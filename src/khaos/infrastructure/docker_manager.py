"""Docker-based Kafka cluster management."""

from __future__ import annotations

import time
from enum import Enum
from pathlib import Path

from confluent_kafka.admin import AdminClient
from rich.console import Console

from khaos.defaults import KAFKA_READY_TIMEOUT_SECONDS
from khaos.infrastructure.compose_runner import DockerComposeRunner
from khaos.infrastructure.schema_registry_manager import SchemaRegistryManager

DOCKER_DIR = Path(__file__).parent.parent.parent.parent / "docker"


class ClusterMode(str, Enum):
    KRAFT = "kraft"
    ZOOKEEPER = "zookeeper"


class DockerManager:
    """Manages Docker-based Kafka cluster lifecycle."""

    def __init__(self, console: Console | None = None):
        self._console = console or Console()
        self._active_compose_file: Path | None = None
        self._schema_registry = SchemaRegistryManager(self._console)

    @staticmethod
    def _get_compose_file(mode: ClusterMode) -> Path:
        if mode == ClusterMode.KRAFT:
            return DOCKER_DIR / "docker-compose.kraft.yml"
        return DOCKER_DIR / "docker-compose.zk.yml"

    @staticmethod
    def _get_schema_registry_compose_file(mode: ClusterMode) -> Path:
        if mode == ClusterMode.KRAFT:
            return DOCKER_DIR / "docker-compose.schema-registry.kraft.yml"
        return DOCKER_DIR / "docker-compose.schema-registry.zk.yml"

    def _get_active_compose_file(self) -> Path | None:
        if self._active_compose_file is not None:
            return self._active_compose_file

        if DockerComposeRunner.is_container_running("zookeeper"):
            self._active_compose_file = self._get_compose_file(ClusterMode.ZOOKEEPER)
            return self._active_compose_file

        if DockerComposeRunner.is_container_running("kafka-1"):
            self._active_compose_file = self._get_compose_file(ClusterMode.KRAFT)
            return self._active_compose_file

        return None

    def cluster_up(self, mode: ClusterMode = ClusterMode.KRAFT) -> None:
        """Start Kafka cluster."""
        compose_file = self._get_compose_file(mode)
        mode_label = "KRaft" if mode == ClusterMode.KRAFT else "ZooKeeper"

        self._console.print(f"[bold blue]Starting Kafka cluster ({mode_label} mode)...[/bold blue]")
        DockerComposeRunner.up(compose_file)
        self._active_compose_file = compose_file
        self._console.print(
            f"[bold green]Kafka containers started ({mode_label} mode)![/bold green]"
        )
        self.wait_for_kafka()

    def cluster_down(self, remove_volumes: bool = True) -> None:
        """Stop Kafka cluster."""
        if self._schema_registry.is_running():
            mode = self.get_active_mode()
            if mode:
                self._schema_registry.stop_with_volumes(
                    self._get_schema_registry_compose_file(mode)
                )
            else:
                for m in ClusterMode:
                    self._schema_registry.stop_with_volumes(
                        self._get_schema_registry_compose_file(m)
                    )

        compose_file = self._get_active_compose_file()

        if compose_file is None:
            self._console.print(
                "[yellow]No active cluster detected, checking both modes...[/yellow]"
            )
            for mode in ClusterMode:
                DockerComposeRunner.down(self._get_compose_file(mode), remove_volumes, silent=True)
            self._console.print("[bold green]Kafka cluster stopped![/bold green]")
            return

        self._console.print("[bold blue]Stopping Kafka cluster...[/bold blue]")
        DockerComposeRunner.down(compose_file, remove_volumes, silent=False)
        self._active_compose_file = None
        self._console.print("[bold green]Kafka cluster stopped![/bold green]")

    def cluster_status(self) -> dict[str, dict[str, str]]:
        compose_file = self._get_active_compose_file()
        if compose_file is None:
            return {}
        return DockerComposeRunner.ps(compose_file)

    def get_active_mode(self) -> ClusterMode | None:
        compose_file = self._get_active_compose_file()
        if compose_file is None:
            return None
        if "kraft" in compose_file.name:
            return ClusterMode.KRAFT
        return ClusterMode.ZOOKEEPER

    def get_bootstrap_servers(self) -> str:
        status = self.cluster_status()
        brokers = []
        for service, info in sorted(status.items()):
            if service.startswith("kafka-") and service != "kafka-ui":
                url = info.get("url", "")
                if url and url != "-":
                    brokers.append(url.replace("localhost", "127.0.0.1"))
        return ",".join(brokers) if brokers else "127.0.0.1:9092"

    def wait_for_kafka(
        self,
        bootstrap_servers: str | None = None,
        timeout: int = KAFKA_READY_TIMEOUT_SECONDS,
    ) -> None:
        if bootstrap_servers is None:
            bootstrap_servers = self.get_bootstrap_servers()

        self._console.print("[bold yellow]Waiting for Kafka to be ready...[/bold yellow]")

        admin = AdminClient(
            {
                "bootstrap.servers": bootstrap_servers,
                "log_level": 0,
                "logger": lambda *args: None,
            }
        )
        start = time.time()

        while time.time() - start < timeout:
            try:
                admin.list_topics(timeout=5)
                self._console.print("[bold green]Kafka cluster is ready![/bold green]")
                self._console.print(f"[dim]Bootstrap servers: {bootstrap_servers}[/dim]")
                return
            except Exception:
                elapsed = int(time.time() - start)
                self._console.print(f"[dim]Waiting for Kafka... ({elapsed}s)[/dim]")
                time.sleep(3)

        raise TimeoutError(
            f"Kafka cluster did not become ready within {timeout} seconds.\n"
            "Try: docker compose logs kafka-1"
        )

    def is_cluster_running(self) -> bool:
        status = self.cluster_status()
        if not status:
            return False
        return all("running" in info["state"].lower() for info in status.values())

    def stop_broker(self, broker_name: str) -> None:
        compose_file = self._get_active_compose_file()
        if compose_file is None:
            raise RuntimeError("No active Kafka cluster found")

        self._console.print(f"[bold red]Stopping broker: {broker_name}[/bold red]")
        DockerComposeRunner.stop_service(compose_file, broker_name)

    def start_broker(self, broker_name: str) -> None:
        compose_file = self._get_active_compose_file()
        if compose_file is None:
            raise RuntimeError("No active Kafka cluster found")

        self._console.print(f"[bold green]Starting broker: {broker_name}[/bold green]")
        DockerComposeRunner.start_service(compose_file, broker_name)

    def is_schema_registry_running(self) -> bool:
        return self._schema_registry.is_running()

    def start_schema_registry(self) -> None:
        mode = self.get_active_mode()
        if mode is None:
            raise RuntimeError("No active Kafka cluster found. Start cluster first.")

        compose_file = self._get_schema_registry_compose_file(mode)
        self._schema_registry.start(compose_file)

    def stop_schema_registry(self) -> None:
        """Stop Schema Registry."""
        if not self._schema_registry.is_running():
            return

        mode = self.get_active_mode()
        if mode is None:
            for m in ClusterMode:
                compose_file = self._get_schema_registry_compose_file(m)
                DockerComposeRunner.down(compose_file, remove_volumes=False, silent=True)
            return

        compose_file = self._get_schema_registry_compose_file(mode)
        self._schema_registry.stop(compose_file)
