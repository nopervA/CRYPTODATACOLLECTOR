from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from collector.config import Settings
from collector.health import HealthState, utc_iso_now
from collector.funding_event_tracker import FundingEventTracker
from collector.rest_client import BinanceRestClient
from collector.storage import StorageManager

logger = logging.getLogger(__name__)


def parse_funding(
    payload: dict[str, Any], received_at: int
) -> dict[str, Any]:
    return {
        "timestamp": int(payload.get("time", received_at)),
        "symbol": str(payload["symbol"]).upper(),
        "funding_rate": float(payload["lastFundingRate"]),
        "next_funding_time": int(payload["nextFundingTime"]),
        "mark_price": float(payload["markPrice"]),
        "received_at": received_at,
    }


class FundingCollector:
    def __init__(
        self,
        settings: Settings,
        client: BinanceRestClient,
        storage: StorageManager,
        health: HealthState,
        funding_event_tracker: FundingEventTracker | None = None,
    ) -> None:
        self._symbols = settings.symbols
        self._interval = settings.funding_interval_seconds
        self._client = client
        self._storage = storage
        self._health = health
        self._funding_event_tracker = funding_event_tracker

    async def run(self) -> None:
        while True:
            started = time.monotonic()
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Funding poll failed")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self._interval - elapsed))

    async def _poll_once(self) -> None:
        results = await asyncio.gather(
            *(self._fetch_symbol(symbol) for symbol in self._symbols),
            return_exceptions=True,
        )
        success_count = 0
        for symbol, result in zip(self._symbols, results, strict=True):
            if isinstance(result, BaseException):
                logger.error(
                    "Funding request failed for %s: %s", symbol, result
                )
            else:
                success_count += 1

        if success_count:
            self._health.last_funding_update = utc_iso_now()

    async def _fetch_symbol(self, symbol: str) -> None:
        payload = await self._client.get_json(
            "/fapi/v1/premiumIndex", {"symbol": symbol}
        )
        received_at = time.time_ns() // 1_000_000
        record = parse_funding(payload, received_at)
        await self._storage.write("funding", record)
        if self._funding_event_tracker is not None:
            await self._funding_event_tracker.on_funding(record)
