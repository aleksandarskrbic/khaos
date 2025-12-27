"""Base class for Kafka simulators (producers and consumers)."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, TypeVar


@dataclass
class SimulatorStats:
    """Base class for simulator statistics."""

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


StatsT = TypeVar("StatsT", bound=SimulatorStats)


class Simulator(ABC, Generic[StatsT]):
    """Base class for all simulators with common stop/resume functionality."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """Signal the simulator to stop."""
        self._stop_event.set()

    def resume(self) -> None:
        """Resume the simulator after being stopped."""
        self._stop_event.clear()

    @property
    def should_stop(self) -> bool:
        """Check if the simulator should stop."""
        return self._stop_event.is_set()

    @abstractmethod
    def get_stats(self) -> StatsT:
        """Get simulator statistics."""
        ...
