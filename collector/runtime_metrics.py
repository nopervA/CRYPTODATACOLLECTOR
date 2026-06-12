from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RuntimeMetrics:
    """In-process counters for daily quality reporting."""

    started_at_monotonic: float = field(default_factory=time.monotonic)
    started_at_wall: float = field(default_factory=time.time)
    rest_failures: int = 0
    storage_failures: int = 0
    websocket_disconnects: int = 0
    websocket_reconnects: int = 0
    queue_peak: dict[str, int] = field(default_factory=dict)

    def record_rest_failure(self) -> None:
        self.rest_failures += 1

    def record_storage_failure(self) -> None:
        self.storage_failures += 1

    def record_queue_sizes(self, sizes: dict[str, int]) -> None:
        for name, value in sizes.items():
            self.queue_peak[name] = max(self.queue_peak.get(name, 0), value)

    def uptime_seconds(self) -> float:
        return max(0.0, time.monotonic() - self.started_at_monotonic)

    def snapshot(
        self,
        *,
        websocket_disconnects: int | None = None,
        websocket_reconnects: int | None = None,
    ) -> dict[str, Any]:
        return {
            "uptime_seconds": round(self.uptime_seconds(), 1),
            "started_at_unix": int(self.started_at_wall),
            "websocket_disconnects": (
                self.websocket_disconnects
                if websocket_disconnects is None
                else websocket_disconnects
            ),
            "websocket_reconnects": (
                self.websocket_reconnects
                if websocket_reconnects is None
                else websocket_reconnects
            ),
            "rest_failures": self.rest_failures,
            "storage_failures": self.storage_failures,
            "queue_peak": dict(self.queue_peak),
        }

    def reset_daily(self) -> None:
        self.rest_failures = 0
        self.storage_failures = 0
        self.websocket_disconnects = 0
        self.websocket_reconnects = 0
        self.queue_peak.clear()
