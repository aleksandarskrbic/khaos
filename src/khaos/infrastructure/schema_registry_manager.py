"""Schema Registry lifecycle management."""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from pathlib import Path

from rich.console import Console

from khaos.defaults import SCHEMA_REGISTRY_READY_TIMEOUT_SECONDS
from khaos.infrastructure.compose_runner import DockerComposeRunner

DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081"


class SchemaRegistryManager:
    def __init__(self, console: Console | None = None):
        self._console = console or Console()

    def is_running(self) -> bool:
        return DockerComposeRunner.is_container_running("schema-registry")

    def start(self, compose_file: Path) -> None:
        if self.is_running():
            self._console.print("[dim]Schema Registry already running[/dim]")
            return

        self._console.print("[bold blue]Starting Schema Registry...[/bold blue]")
        DockerComposeRunner.up(compose_file)
        self._wait_for_ready()
        self._console.print("[bold green]Schema Registry is ready![/bold green]")

    def stop(self, compose_file: Path) -> None:
        if not self.is_running():
            return

        self._console.print("[bold blue]Stopping Schema Registry...[/bold blue]")
        DockerComposeRunner.down(compose_file, remove_volumes=False, silent=False)
        self._console.print("[bold green]Schema Registry stopped![/bold green]")

    def stop_with_volumes(self, compose_file: Path) -> None:
        self._console.print("[dim]Stopping Schema Registry...[/dim]")
        DockerComposeRunner.down(compose_file, remove_volumes=True, silent=True)

    def _wait_for_ready(self, timeout: int = SCHEMA_REGISTRY_READY_TIMEOUT_SECONDS) -> None:
        start = time.time()
        while time.time() - start < timeout:
            try:
                req = urllib.request.urlopen(f"{DEFAULT_SCHEMA_REGISTRY_URL}/subjects", timeout=5)
                if req.status == 200:
                    return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(2)

        raise TimeoutError(f"Schema Registry did not become ready within {timeout} seconds")
