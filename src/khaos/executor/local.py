"""Local executor for Docker-based Kafka clusters."""

from __future__ import annotations

import asyncio

from rich.console import Console

from khaos.executor.base import BaseExecutor
from khaos.infrastructure.docker_manager import DockerManager
from khaos.scenarios.scenario import Scenario

console = Console()


class LocalExecutor(BaseExecutor):
    """Executor for local Docker-based Kafka clusters.

    This executor:
    - Auto-starts Schema Registry when Avro/Protobuf schemas are used
    - Supports StopBroker/StartBroker incident commands
    - Creates topics using local KafkaAdmin
    """

    def __init__(
        self,
        bootstrap_servers: str,
        scenarios: list[Scenario],
        docker_manager: DockerManager,
        no_consumers: bool = False,
    ):
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            scenarios=scenarios,
            no_consumers=no_consumers,
        )
        self._docker_manager = docker_manager

    def _is_schema_registry_running(self) -> bool:
        """Check if Schema Registry is running via Docker."""
        return self._docker_manager.is_schema_registry_running()

    async def setup(self) -> None:
        """Set up topics and auto-start Schema Registry if needed."""
        serializer_factory = self._create_serializer_factory()
        if serializer_factory.needs_schema_registry(self._all_topics):
            if not self._docker_manager.is_schema_registry_running():
                console.print("[bold blue]Schema format detected, starting Schema Registry...[/]")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._docker_manager.start_schema_registry)

        await super().setup()

    async def _handle_stop_broker(self, broker: str) -> None:
        """Handle StopBroker command by stopping Docker container."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._docker_manager.stop_broker, broker)

    async def _handle_start_broker(self, broker: str) -> None:
        """Handle StartBroker command by starting Docker container."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._docker_manager.start_broker, broker)
