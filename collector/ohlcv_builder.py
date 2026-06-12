from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from collector.storage import StorageManager


@dataclass(slots=True)
class _MinuteBar:
    minute_start: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int


class OhlcvBuilder:
    """Build one-minute OHLCV bars from live aggregate trades."""

    def __init__(self, storage: StorageManager) -> None:
        self._storage = storage
        self._bars: dict[str, _MinuteBar] = {}

    async def on_trade(self, record: dict[str, Any]) -> None:
        symbol = str(record["symbol"])
        timestamp_ms = int(record["timestamp"])
        price = float(record["price"])
        quantity = float(record["quantity"])
        minute_start = (timestamp_ms // 60_000) * 60_000

        bar = self._bars.get(symbol)
        if bar is None or bar.minute_start != minute_start:
            if bar is not None:
                await self._emit(symbol, bar)
            self._bars[symbol] = _MinuteBar(
                minute_start=minute_start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=quantity,
                trade_count=1,
            )
            return

        bar.high = max(bar.high, price)
        bar.low = min(bar.low, price)
        bar.close = price
        bar.volume += quantity
        bar.trade_count += 1

    async def flush(self) -> None:
        for symbol, bar in list(self._bars.items()):
            await self._emit(symbol, bar)
        self._bars.clear()

    async def _emit(self, symbol: str, bar: _MinuteBar) -> None:
        await self._storage.write(
            "ohlcv_1m",
            {
                "symbol": symbol,
                "minute_start": bar.minute_start,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "trade_count": bar.trade_count,
            },
        )
