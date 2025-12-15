"""Docker management for Kafka cluster."""

import subprocess
import time
from pathlib import Path

from rich.console import Console

console = Console()

COMPOSE_FILE = Path(__file__).parent.parent.parent.parent / "docker" / "docker-compose.yml"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def cluster_up() -> None:
    """Start the 3-broker Kafka cluster."""
    console.print("[bold blue]Starting Kafka cluster...[/bold blue]")
    try:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"],
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
            raise RuntimeError(f"Docker compose file not found: {COMPOSE_FILE}")
        raise RuntimeError(f"Failed to start Kafka cluster: {stderr or e}")
    console.print("[bold green]Kafka containers started![/bold green]")
    wait_for_kafka()


def cluster_down(remove_volumes: bool = False) -> None:
    """Stop the Kafka cluster."""
    console.print("[bold blue]Stopping Kafka cluster...[/bold blue]")
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), "down"]
    if remove_volumes:
        cmd.append("-v")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr or ""
        if "Cannot connect to the Docker daemon" in stderr:
            raise RuntimeError("Docker is not running. Please start Docker Desktop and try again.")
        raise RuntimeError(f"Failed to stop Kafka cluster: {stderr or e}")
    console.print("[bold green]Kafka cluster stopped![/bold green]")


def cluster_status() -> dict[str, str]:
    """Get status of Kafka containers."""
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        return {}

    import json

    try:
        # docker compose ps --format json returns one JSON object per line
        lines = result.stdout.strip().split("\n")
        services = {}
        for line in lines:
            if line.strip():
                data = json.loads(line)
                services[data.get("Service", data.get("Name", "unknown"))] = data.get(
                    "State", "unknown"
                )
        return services
    except json.JSONDecodeError:
        return {}


def wait_for_kafka(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    timeout: int = 120,
) -> None:
    """Wait until Kafka cluster is ready to accept connections."""
    from confluent_kafka.admin import AdminClient

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
            # Try to list topics - this will fail if Kafka isn't ready
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
        "Try: docker compose -f docker/docker-compose.yml logs kafka-1"
    )


def is_cluster_running() -> bool:
    """Check if the Kafka cluster is running."""
    status = cluster_status()
    if not status:
        return False
    # Check if all kafka services are running
    return all("running" in state.lower() for state in status.values())


def stop_broker(broker_name: str) -> None:
    """Stop a specific broker container.

    Args:
        broker_name: Name of the broker service (e.g., 'kafka-1', 'kafka-2', 'kafka-3')
    """
    console.print(f"[bold red]Stopping broker: {broker_name}[/bold red]")
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "stop", broker_name],
        check=True,
    )


def start_broker(broker_name: str) -> None:
    """Start a specific broker container.

    Args:
        broker_name: Name of the broker service (e.g., 'kafka-1', 'kafka-2', 'kafka-3')
    """
    console.print(f"[bold green]Starting broker: {broker_name}[/bold green]")
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "start", broker_name],
        check=True,
    )
