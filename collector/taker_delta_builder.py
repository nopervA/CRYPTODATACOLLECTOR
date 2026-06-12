from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from collector.storage import StorageManager


@dataclass(slots=True)
class _TakerMinute:
    minute: int
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    trade_count: int = 0


class TakerDeltaBuilder:
    """Build one-minute taker buy/sell flow from aggregate trades."""

    def __init__(self, storage: StorageManager) -> None:
        self._storage = storage
        self._minutes: dict[str, _TakerMinute] = {}

    async def on_trade(self, record: dict[str, Any]) -> None:
        symbol = str(record["symbol"])
        timestamp_ms = int(record["timestamp"])
        quantity = float(record["quantity"])
        minute = (timestamp_ms // 60_000) * 60_000
        is_buyer_maker = bool(record["is_buyer_maker"])

        current = self._minutes.get(symbol)
        if current is None or current.minute != minute:
            if current is not None:
                await self._emit(symbol, current)
            current = _TakerMinute(minute=minute)
            self._minutes[symbol] = current

        if is_buyer_maker:
            current.sell_volume += quantity
        else:
            current.buy_volume += quantity
        current.trade_count += 1

    async def flush(self) -> None:
        for symbol, minute in list(self._minutes.items()):
            await self._emit(symbol, minute)
        self._minutes.clear()

    async def _emit(self, symbol: str, minute: _TakerMinute) -> None:
        total = minute.buy_volume + minute.sell_volume
        delta_volume = minute.buy_volume - minute.sell_volume
        delta_ratio = delta_volume / total if total > 0.0 else 0.0
        await self._storage.write(
            "taker_delta_1m",
            {
                "symbol": symbol,
                "minute": minute.minute,
                "buy_volume": minute.buy_volume,
                "sell_volume": minute.sell_volume,
                "delta_volume": delta_volume,
                "delta_ratio": delta_ratio,
                "trade_count": minute.trade_count,
            },
        )
