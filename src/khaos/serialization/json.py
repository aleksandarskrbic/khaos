from __future__ import annotations

import json
from typing import Any


class JsonSerializer:
    def serialize(self, data: dict[str, Any]) -> bytes:
        return json.dumps(data).encode("utf-8")

    def deserialize(self, data: bytes) -> dict[str, Any]:
        result: dict[str, Any] = json.loads(data.decode("utf-8"))
        return result
