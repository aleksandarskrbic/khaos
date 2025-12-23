import subprocess
import time
from enum import Enum
from pathlib import Path

from rich.console import Console

console = Console()

DOCKER_DIR = Path(__file__).parent.parent.parent.parent / "docker"

_active_compose_file: Path | None = None


class ClusterMode(str, Enum):
    KRAFT = "kraft"
    ZOOKEEPER = "zookeeper"


def get_compose_file(mode: ClusterMode) -> Path:
    if mode == ClusterMode.KRAFT:
        return DOCKER_DIR / "docker-compose.kraft.yml"
    return DOCKER_DIR / "docker-compose.zk.yml"


def _get_active_compose_file() -> Path | None:
    global _active_compose_file

    if _active_compose_file is not None:
        return _active_compose_file

    result = subprocess.run(
        ["docker", "ps", "--filter", "name=zookeeper", "--format", "{{.Names}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if "zookeeper" in result.stdout:
        _active_compose_file = get_compose_file(ClusterMode.ZOOKEEPER)
        return _active_compose_file

    result = subprocess.run(
        ["docker", "ps", "--filter", "name=kafka-1", "--format", "{{.Names}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if "kafka-1" in result.stdout:
        _active_compose_file = get_compose_file(ClusterMode.KRAFT)
        return _active_compose_file

    return None


def _set_active_compose_file(compose_file: Path) -> None:
    global _active_compose_file
    _active_compose_file = compose_file


def _clear_active_compose_file() -> None:
    global _active_compose_file
    _active_compose_file = None


def cluster_up(mode: ClusterMode = ClusterMode.KRAFT) -> None:
    compose_file = get_compose_file(mode)
    mode_label = "KRaft" if mode == ClusterMode.KRAFT else "ZooKeeper"

    console.print(f"[bold blue]Starting Kafka cluster ({mode_label} mode)...[/bold blue]")

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
            raise RuntimeError("Docker is not running. Please start Docker Desktop and try again.")
        if "port is already allocated" in stderr:
            raise RuntimeError(
                "Ports 9092-9094 already in use. Stop other Kafka instances or free the ports."
            )
        if "no such file or directory" in stderr.lower() or "not found" in stderr.lower():
            raise RuntimeError(f"Docker compose file not found: {compose_file}")
        raise RuntimeError(f"Failed to start Kafka cluster: {stderr or e}")

    _set_active_compose_file(compose_file)
    console.print(f"[bold green]Kafka containers started ({mode_label} mode)![/bold green]")
    wait_for_kafka()


def cluster_down(remove_volumes: bool = False) -> None:
    compose_file = _get_active_compose_file()

    if compose_file is None:
        console.print("[yellow]No active cluster detected, checking both modes...[/yellow]")
        for mode in ClusterMode:
            _stop_compose(get_compose_file(mode), remove_volumes, silent=True)
        console.print("[bold green]Kafka cluster stopped![/bold green]")
        return

    console.print("[bold blue]Stopping Kafka cluster...[/bold blue]")
    _stop_compose(compose_file, remove_volumes, silent=False)
    _clear_active_compose_file()
    console.print("[bold green]Kafka cluster stopped![/bold green]")


def _stop_compose(compose_file: Path, remove_volumes: bool, silent: bool) -> None:
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


def cluster_status() -> dict[str, dict[str, str]]:
    compose_file = _get_active_compose_file()

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

    import json

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


def get_active_mode() -> ClusterMode | None:
    compose_file = _get_active_compose_file()
    if compose_file is None:
        return None
    if "kraft" in compose_file.name:
        return ClusterMode.KRAFT
    return ClusterMode.ZOOKEEPER


def get_bootstrap_servers() -> str:
    status = cluster_status()
    brokers = []
    for service, info in sorted(status.items()):
        if service.startswith("kafka-") and service != "kafka-ui":
            url = info.get("url", "")
            if url and url != "-":
                brokers.append(url.replace("localhost", "127.0.0.1"))
    return ",".join(brokers) if brokers else "127.0.0.1:9092"


def wait_for_kafka(
    bootstrap_servers: str | None = None,
    timeout: int = 120,
) -> None:
    from confluent_kafka.admin import AdminClient

    if bootstrap_servers is None:
        bootstrap_servers = get_bootstrap_servers()

    console.print("[bold yellow]Waiting for Kafka to be ready...[/bold yellow]")

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
            console.print("[bold green]Kafka cluster is ready![/bold green]")
            console.print(f"[dim]Bootstrap servers: {bootstrap_servers}[/dim]")
            return
        except Exception:
            elapsed = int(time.time() - start)
            console.print(f"[dim]Waiting for Kafka... ({elapsed}s)[/dim]")
            time.sleep(3)

    raise TimeoutError(
        f"Kafka cluster did not become ready within {timeout} seconds.\n"
        "Try: docker compose logs kafka-1"
    )


def is_cluster_running() -> bool:
    status = cluster_status()
    if not status:
        return False
    return all("running" in info["state"].lower() for info in status.values())


def stop_broker(broker_name: str) -> None:
    compose_file = _get_active_compose_file()
    if compose_file is None:
        raise RuntimeError("No active Kafka cluster found")

    console.print(f"[bold red]Stopping broker: {broker_name}[/bold red]")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "stop", broker_name],
        check=True,
    )


def start_broker(broker_name: str) -> None:
    compose_file = _get_active_compose_file()
    if compose_file is None:
        raise RuntimeError("No active Kafka cluster found")

    console.print(f"[bold green]Starting broker: {broker_name}[/bold green]")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "start", broker_name],
        check=True,
    )
