from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from collector.storage import StorageManager


@dataclass(slots=True)
class _OiSample:
    timestamp: int
    open_interest: float


class OiChangeTracker:
    """Derive open-interest change metrics from live OI polls."""

    def __init__(self, storage: StorageManager) -> None:
        self._storage = storage
        self._history: dict[str, deque[_OiSample]] = {}
        self._previous_oi: dict[str, float] = {}

    async def on_open_interest(self, record: dict[str, Any]) -> None:
        symbol = str(record["symbol"])
        timestamp = int(record["timestamp"])
        oi = float(record["open_interest"])
        previous = self._previous_oi.get(symbol)
        oi_delta = 0.0 if previous is None else oi - previous
        oi_delta_pct = 0.0 if not previous else oi_delta / previous

        history = self._history.setdefault(symbol, deque(maxlen=120))
        history.append(_OiSample(timestamp=timestamp, open_interest=oi))
        self._previous_oi[symbol] = oi

        await self._storage.write(
            "oi_change",
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "oi": oi,
                "oi_delta": oi_delta,
                "oi_delta_pct": oi_delta_pct,
                "rolling_5m_delta": self._rolling_delta(history, 5 * 60_000),
                "rolling_15m_delta": self._rolling_delta(history, 15 * 60_000),
                "rolling_60m_delta": self._rolling_delta(history, 60 * 60_000),
            },
        )

    @staticmethod
    def _rolling_delta(history: deque[_OiSample], window_ms: int) -> float:
        if not history:
            return 0.0
        latest = history[-1]
        cutoff = latest.timestamp - window_ms
        baseline = latest.open_interest
        for sample in history:
            if sample.timestamp >= cutoff:
                baseline = sample.open_interest
                break
        return latest.open_interest - baseline
