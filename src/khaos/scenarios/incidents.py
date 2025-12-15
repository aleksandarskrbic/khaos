"""Incident primitives - executable actions triggered during scenarios."""

import asyncio
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich.console import Console

from khaos.infrastructure import docker_manager
from khaos.kafka.consumer import ConsumerSimulator

if TYPE_CHECKING:
    from khaos.scenarios.executor import ScenarioExecutor

console = Console()

# Incident categories for external cluster support
# Infrastructure incidents require Docker control - not available on external clusters
INFRASTRUCTURE_INCIDENTS = {"stop_broker", "start_broker"}

# Client incidents control local simulators - work on any cluster
CLIENT_INCIDENTS = {
    "increase_consumer_delay",
    "rebalance_consumer",
    "change_producer_rate",
    "pause_consumer",
}


@dataclass
class IncidentContext:
    """Context passed to incident handlers."""

    executor: "ScenarioExecutor"
    bootstrap_servers: str


async def increase_consumer_delay(
    ctx: IncidentContext,
    delay_ms: int,
    **kwargs,
) -> None:
    """Increase processing delay on all consumers (simulate backpressure)."""
    console.print(f"\n[bold red]>>> INCIDENT: Increasing consumer delay to {delay_ms}ms[/bold red]")
    for consumer in ctx.executor.consumers:
        consumer.processing_delay_ms = delay_ms


async def rebalance_consumer(
    ctx: IncidentContext,
    **kwargs,
) -> None:
    """Trigger a consumer rebalance by closing and recreating a random consumer."""
    if not ctx.executor.consumers:
        return

    idx = random.randint(0, len(ctx.executor.consumers) - 1)
    old_consumer = ctx.executor.consumers[idx]

    ctx.executor.rebalance_count += 1
    n = ctx.executor.rebalance_count
    console.print(f"\n[bold red]>>> REBALANCE #{n}: Closing consumer {idx + 1}[/bold red]")

    # Get consumer info before closing
    group_id = old_consumer.group_id
    topics = old_consumer.topics
    delay_ms = old_consumer.processing_delay_ms

    # Close old consumer
    old_consumer.stop()
    old_consumer.close()

    await asyncio.sleep(3)

    # Create new consumer
    new_consumer = ConsumerSimulator(
        bootstrap_servers=ctx.bootstrap_servers,
        group_id=group_id,
        topics=topics,
        processing_delay_ms=delay_ms,
    )
    ctx.executor.consumers[idx] = new_consumer

    console.print(f"[yellow]>>> Consumer {idx + 1} rejoined group[/yellow]\n")

    # Start consuming in background - use duration_seconds=0 but consumer respects stop() signal
    asyncio.create_task(new_consumer.consume_loop(duration_seconds=0))


async def stop_broker(
    ctx: IncidentContext,
    broker: str,
    **kwargs,
) -> None:
    """Stop a Kafka broker."""
    console.print(f"\n[bold red]>>> INCIDENT: Stopping {broker}[/bold red]")
    console.print("[yellow]ISR will shrink, leadership will change[/yellow]\n")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, docker_manager.stop_broker, broker)


async def start_broker(
    ctx: IncidentContext,
    broker: str,
    **kwargs,
) -> None:
    """Start a Kafka broker."""
    console.print(f"\n[bold green]>>> RECOVERY: Starting {broker}[/bold green]")
    console.print("[yellow]ISR will expand back[/yellow]\n")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, docker_manager.start_broker, broker)


async def change_producer_rate(
    ctx: IncidentContext,
    rate: float,
    **kwargs,
) -> None:
    """Change producer rate (traffic spike/drop)."""
    console.print(
        f"\n[bold yellow]>>> INCIDENT: Changing producer rate to {rate} msg/s[/bold yellow]"
    )
    for producer in ctx.executor.producers:
        producer.messages_per_second = rate


async def pause_consumer(
    ctx: IncidentContext,
    duration_seconds: int,
    **kwargs,
) -> None:
    """Pause all consumers for a duration (simulate GC pause)."""
    console.print(f"\n[bold red]>>> INCIDENT: Pausing consumers {duration_seconds}s[/bold red]")

    # Stop all consumers
    for consumer in ctx.executor.consumers:
        consumer.stop()

    await asyncio.sleep(duration_seconds)

    console.print("[bold green]>>> Consumers resuming[/bold green]\n")

    # Restart consumers
    for consumer in ctx.executor.consumers:
        consumer._stop_event.clear()
        asyncio.create_task(consumer.consume_loop(duration_seconds=0))


# Registry of incident handlers
INCIDENT_HANDLERS = {
    "increase_consumer_delay": increase_consumer_delay,
    "rebalance_consumer": rebalance_consumer,
    "stop_broker": stop_broker,
    "start_broker": start_broker,
    "change_producer_rate": change_producer_rate,
    "pause_consumer": pause_consumer,
}
