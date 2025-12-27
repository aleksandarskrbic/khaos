"""Base class for Kafka simulators (producers and consumers)."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Generic, TypeVar


@dataclass
class SimulatorStats:
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


StatsT = TypeVar("StatsT", bound=SimulatorStats)


class Simulator(ABC, Generic[StatsT]):
    def __init__(self, executor: ThreadPoolExecutor) -> None:
        self._stop_event = threading.Event()
        self._executor = executor

    def stop(self) -> None:
        self._stop_event.set()

    def resume(self) -> None:
        self._stop_event.clear()

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    @abstractmethod
    def get_stats(self) -> StatsT: ...
