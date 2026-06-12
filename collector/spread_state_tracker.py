from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from collector.storage import StorageManager


@dataclass(slots=True)
class _SpreadSample:
    timestamp: int
    spread_bps: float


class SpreadStateTracker:
    """Track spread regime statistics from top-of-book samples."""

    def __init__(
        self,
        storage: StorageManager,
        *,
        window: int = 300,
        zscore_threshold: float = 2.0,
    ) -> None:
        self._storage = storage
        self._window = window
        self._zscore_threshold = zscore_threshold
        self._history: dict[str, deque[_SpreadSample]] = {}

    async def on_top_of_book(self, record: dict[str, Any]) -> bool:
        symbol = str(record["symbol"])
        timestamp = int(record["event_time"])
        spread_bps = float(record["spread_bps"])
        history = self._history.setdefault(symbol, deque(maxlen=self._window))
        history.append(_SpreadSample(timestamp=timestamp, spread_bps=spread_bps))

        values = [sample.spread_bps for sample in history]
        mean = sum(values) / len(values)
        if len(values) > 1:
            variance = sum((value - mean) ** 2 for value in values) / len(values)
            std = variance**0.5
        else:
            std = 0.0
        spread_zscore = (spread_bps - mean) / std if std > 0.0 else 0.0
        is_wide_spread = spread_zscore >= self._zscore_threshold

        await self._storage.write(
            "spread_state",
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "spread_bps": spread_bps,
                "spread_zscore": spread_zscore,
                "is_wide_spread": is_wide_spread,
                "rolling_spread_mean": mean,
                "rolling_spread_std": std,
            },
        )
        return is_wide_spread
