"""Low-level Docker Compose operations."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path


class DockerComposeRunner:
    """Runs Docker Compose commands."""

    @staticmethod
    def up(compose_file: Path) -> None:
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
            raise RuntimeError(f"Failed to start containers: {stderr or e}")

    @staticmethod
    def down(compose_file: Path, remove_volumes: bool = True, silent: bool = False) -> None:
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
                raise RuntimeError(f"Failed to stop containers: {stderr or e}")

    @staticmethod
    def stop_service(compose_file: Path, service_name: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "stop", service_name],
            check=True,
        )

    @staticmethod
    def start_service(compose_file: Path, service_name: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "start", service_name],
            check=True,
        )

    @staticmethod
    def ps(compose_file: Path) -> dict[str, dict[str, str]]:
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

    @staticmethod
    def is_container_running(name_filter: str) -> bool:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={name_filter}", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
        )
        return name_filter in result.stdout
