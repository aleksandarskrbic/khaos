from __future__ import annotations

from typing import Any, Protocol


class Serializer(Protocol):
    def serialize(self, data: dict[str, Any]) -> bytes: ...

    def deserialize(self, data: bytes) -> dict[str, Any]: ...
