"""Execution result dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ExecutionResult:
    """Result of scenario execution."""

    messages_produced: int = 0
    messages_consumed: int = 0
    flows_completed: int = 0
    flow_messages_sent: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        """Add an error message to the result."""
        self.errors.append(error)
