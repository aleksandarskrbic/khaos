from __future__ import annotations

import json
import subprocess
import time
from enum import Enum
from pathlib import Path

from rich.console import Console

DOCKER_DIR = Path(__file__).parent.parent.parent.parent / "docker"


class ClusterMode(str, Enum):
    KRAFT = "kraft"
    ZOOKEEPER = "zookeeper"


def get_compose_file(mode: ClusterMode) -> Path:
    if mode == ClusterMode.KRAFT:
        return DOCKER_DIR / "docker-compose.kraft.yml"
    return DOCKER_DIR / "docker-compose.zk.yml"


class DockerManager:
    def __init__(self, console: Console | None = None):
        self._console = console or Console()
        self._active_compose_file: Path | None = None

    def _get_active_compose_file(self) -> Path | None:
        if self._active_compose_file is not None:
            return self._active_compose_file

        result = subprocess.run(
            ["docker", "ps", "--filter", "name=zookeeper", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if "zookeeper" in result.stdout:
            self._active_compose_file = get_compose_file(ClusterMode.ZOOKEEPER)
            return self._active_compose_file

        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka-1", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if "kafka-1" in result.stdout:
            self._active_compose_file = get_compose_file(ClusterMode.KRAFT)
            return self._active_compose_file

        return None

    def cluster_up(self, mode: ClusterMode = ClusterMode.KRAFT) -> None:
        compose_file = get_compose_file(mode)
        mode_label = "KRaft" if mode == ClusterMode.KRAFT else "ZooKeeper"

        self._console.print(f"[bold blue]Starting Kafka cluster ({mode_label} mode)...[/bold blue]")

        try:
            subprocess.run(
                ["docker", "compose", "-f", str(compose_file), "up", "-d"],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            stderr = e.stderr or ""
            if (
                "Cannot connect to the Docker daemon" in stderr
                or "Is the docker daemon running" in stderr
            ):
                raise RuntimeError(
                    "Docker is not running. Please start Docker Desktop and try again."
                )
            if "port is already allocated" in stderr:
                raise RuntimeError(
                    "Ports 9092-9094 already in use. Stop other Kafka instances or free the ports."
                )
            if "no such file or directory" in stderr.lower() or "not found" in stderr.lower():
                raise RuntimeError(f"Docker compose file not found: {compose_file}")
            raise RuntimeError(f"Failed to start Kafka cluster: {stderr or e}")

        self._active_compose_file = compose_file
        self._console.print(
            f"[bold green]Kafka containers started ({mode_label} mode)![/bold green]"
        )
        self.wait_for_kafka()

    def cluster_down(self, remove_volumes: bool = False) -> None:
        compose_file = self._get_active_compose_file()

        if compose_file is None:
            self._console.print(
                "[yellow]No active cluster detected, checking both modes...[/yellow]"
            )
            for mode in ClusterMode:
                self._stop_compose(get_compose_file(mode), remove_volumes, silent=True)
            self._console.print("[bold green]Kafka cluster stopped![/bold green]")
            return

        self._console.print("[bold blue]Stopping Kafka cluster...[/bold blue]")
        self._stop_compose(compose_file, remove_volumes, silent=False)
        self._active_compose_file = None
        self._console.print("[bold green]Kafka cluster stopped![/bold green]")

    def _stop_compose(self, compose_file: Path, remove_volumes: bool, silent: bool) -> None:
        cmd = ["docker", "compose", "-f", str(compose_file), "down"]
        if remove_volumes:
            cmd.append("-v")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            if not silent:
                stderr = e.stderr or ""
                if "Cannot connect to the Docker daemon" in stderr:
                    raise RuntimeError(
                        "Docker is not running. Please start Docker Desktop and try again."
                    )
                raise RuntimeError(f"Failed to stop Kafka cluster: {stderr or e}")

    def cluster_status(self) -> dict[str, dict[str, str]]:
        compose_file = self._get_active_compose_file()

        if compose_file is None:
            return {}

        result = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "ps", "--format", "json"],
            check=False,
            capture_output=True,
            text=True,
        )
        if not result.stdout.strip():
            return {}

        try:
            lines = result.stdout.strip().split("\n")
            services = {}
            for line in lines:
                if line.strip():
                    data = json.loads(line)
                    service_name = data.get("Service", data.get("Name", "unknown"))
                    state = data.get("State", "unknown")

                    url = "-"
                    publishers = data.get("Publishers", [])
                    if publishers:
                        for pub in publishers:
                            published_port = pub.get("PublishedPort")
                            if published_port:
                                if service_name == "kafka-ui":
                                    url = f"http://localhost:{published_port}"
                                else:
                                    url = f"localhost:{published_port}"
                                break

                    services[service_name] = {"state": state, "url": url}
            return services
        except json.JSONDecodeError:
            return {}

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
        timeout: int = 120,
    ) -> None:
        from confluent_kafka.admin import AdminClient

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
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "stop", broker_name],
            check=True,
        )

    def start_broker(self, broker_name: str) -> None:
        compose_file = self._get_active_compose_file()
        if compose_file is None:
            raise RuntimeError("No active Kafka cluster found")

        self._console.print(f"[bold green]Starting broker: {broker_name}[/bold green]")
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "start", broker_name],
            check=True,
        )


# Default instance for module-level API compatibility
_default_manager = DockerManager()


def cluster_up(mode: ClusterMode = ClusterMode.KRAFT) -> None:
    _default_manager.cluster_up(mode)


def cluster_down(remove_volumes: bool = False) -> None:
    _default_manager.cluster_down(remove_volumes)


def cluster_status() -> dict[str, dict[str, str]]:
    return _default_manager.cluster_status()


def get_active_mode() -> ClusterMode | None:
    return _default_manager.get_active_mode()


def get_bootstrap_servers() -> str:
    return _default_manager.get_bootstrap_servers()


def wait_for_kafka(bootstrap_servers: str | None = None, timeout: int = 120) -> None:
    _default_manager.wait_for_kafka(bootstrap_servers, timeout)


def is_cluster_running() -> bool:
    return _default_manager.is_cluster_running()


def stop_broker(broker_name: str) -> None:
    _default_manager.stop_broker(broker_name)


def start_broker(broker_name: str) -> None:
    _default_manager.start_broker(broker_name)
