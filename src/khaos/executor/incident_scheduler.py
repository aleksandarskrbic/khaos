"""Incident scheduling and command execution."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

from rich.console import Console

from khaos.defaults import CONSUMER_CLOSE_WAIT_SECONDS
from khaos.kafka.consumer import ConsumerSimulator
from khaos.kafka.producer import ProducerSimulator
from khaos.scenarios.incidents import (
    Command,
    CreateConsumer,
    Delay,
    Incident,
    IncidentContext,
    IncidentGroup,
    IncrementRebalanceCount,
    PrintMessage,
    ResumeConsumers,
    SetConsumerDelay,
    SetProducerRate,
    StartBroker,
    StopBroker,
    StopConsumer,
    StopConsumers,
)


class IncidentScheduler:
    def __init__(
        self,
        consumers: list[ConsumerSimulator],
        producers: list[ProducerSimulator],
        bootstrap_servers: str,
        consumers_by_topic: dict[str, list[ConsumerSimulator]],
        consumers_by_group: dict[str, list[ConsumerSimulator]],
        producers_by_topic: dict[str, list[ProducerSimulator]],
        create_consumer_fn: Callable[[str, list[str], int], ConsumerSimulator],
        handle_stop_broker_fn: Callable[[str], Coroutine[Any, Any, None]],
        handle_start_broker_fn: Callable[[str], Coroutine[Any, Any, None]],
        should_stop_fn: Callable[[], bool],
        console: Console,
    ):
        self.consumers = consumers
        self.console = console
        self.producers = producers
        self.bootstrap_servers = bootstrap_servers
        self.consumers_by_topic = consumers_by_topic
        self.consumers_by_group = consumers_by_group
        self.producers_by_topic = producers_by_topic
        self._create_consumer_fn = create_consumer_fn
        self._handle_stop_broker_fn = handle_stop_broker_fn
        self._handle_start_broker_fn = handle_start_broker_fn
        self._should_stop_fn = should_stop_fn
        self.rebalance_count = 0

    @property
    def should_stop(self) -> bool:
        return self._should_stop_fn()

    def build_context(self) -> IncidentContext:
        return IncidentContext(
            consumers=self.consumers,
            producers=self.producers,
            bootstrap_servers=self.bootstrap_servers,
            rebalance_count=self.rebalance_count,
            consumers_by_topic=self.consumers_by_topic,
            consumers_by_group=self.consumers_by_group,
            producers_by_topic=self.producers_by_topic,
        )

    async def execute_commands(self, commands: list[Command]) -> None:
        for cmd in commands:
            if self.should_stop:
                break

            match cmd:
                case Delay(seconds=s):
                    await asyncio.sleep(s)

                case PrintMessage(message=msg, style=style):
                    self.console.print(f"\n[{style}]{msg}[/]")

                case SetConsumerDelay(index=idx, delay_ms=delay):
                    if idx < len(self.consumers):
                        self.consumers[idx].config.processing_delay_ms = delay

                case SetProducerRate(index=idx, rate=r):
                    if idx < len(self.producers):
                        self.producers[idx].messages_per_second = r

                case IncrementRebalanceCount():
                    self.rebalance_count += 1

                case StopConsumers(indices=indices):
                    for idx in indices:
                        if idx < len(self.consumers):
                            self.consumers[idx].stop()

                case ResumeConsumers(indices=indices):
                    for idx in indices:
                        if idx < len(self.consumers):
                            self.consumers[idx].resume()
                            asyncio.create_task(
                                self.consumers[idx].consume_loop(duration_seconds=0)
                            )

                case StopConsumer(index=idx):
                    if idx < len(self.consumers):
                        self.consumers[idx].stop()
                        await asyncio.sleep(CONSUMER_CLOSE_WAIT_SECONDS)
                        self.consumers[idx].close()

                case CreateConsumer(index=idx, group_id=gid, topics=t, processing_delay_ms=d):
                    new_consumer = self._create_consumer_fn(gid, t, d)
                    if idx < len(self.consumers):
                        self.consumers[idx] = new_consumer
                    asyncio.create_task(new_consumer.consume_loop(duration_seconds=0))

                case StopBroker(broker=b):
                    await self._handle_stop_broker_fn(b)

                case StartBroker(broker=b):
                    await self._handle_start_broker_fn(b)

    async def schedule_incident(self, incident: Incident, start_time: float) -> None:
        schedule = incident.schedule

        if schedule.every_seconds:
            await asyncio.sleep(schedule.initial_delay_seconds)
            while not self.should_stop:
                ctx = self.build_context()
                commands = incident.get_commands(ctx)
                await self.execute_commands(commands)
                await asyncio.sleep(schedule.every_seconds)
        elif schedule.at_seconds is not None:
            elapsed = time.time() - start_time
            wait_time = schedule.at_seconds - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            if not self.should_stop:
                ctx = self.build_context()
                commands = incident.get_commands(ctx)
                await self.execute_commands(commands)

    async def schedule_incident_group(self, group: IncidentGroup, start_time: float) -> None:
        for cycle in range(group.repeat):
            if self.should_stop:
                break

            cycle_start = start_time + (cycle * group.interval_seconds)

            now = time.time()
            wait_time = cycle_start - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)

            if self.should_stop:
                break

            self.console.print(f"\n[bold magenta]>>> GROUP: Cycle {cycle + 1}/{group.repeat}[/]")

            for incident in group.incidents:
                if self.should_stop:
                    break

                schedule = incident.schedule

                if schedule.at_seconds is not None:
                    incident_time = cycle_start + schedule.at_seconds
                    now = time.time()
                    wait_time = incident_time - now
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

                if self.should_stop:
                    break

                ctx = self.build_context()
                commands = incident.get_commands(ctx)
                await self.execute_commands(commands)
