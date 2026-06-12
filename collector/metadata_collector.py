from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from collector.config import Settings
from collector.health import HealthState, utc_iso_now
from collector.rest_client import BinanceRestClient
from collector.storage import StorageManager

logger = logging.getLogger(__name__)


def parse_exchange_metadata(
    symbols: tuple[str, ...],
    payload: dict[str, Any],
    received_at: int,
) -> list[dict[str, Any]]:
    symbol_info = {
        str(item["symbol"]).upper(): item
        for item in payload.get("symbols", [])
    }
    records: list[dict[str, Any]] = []

    for symbol in symbols:
        info = symbol_info.get(symbol)
        if info is None:
            continue

        filters = {
            str(item["filterType"]): item for item in info.get("filters", [])
        }
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        notional_filter = filters.get("MIN_NOTIONAL", filters.get("NOTIONAL", {}))
        min_notional_raw = notional_filter.get(
            "notional", notional_filter.get("minNotional")
        )

        records.append(
            {
                "symbol": symbol,
                "timestamp": received_at,
                "tick_size": float(price_filter.get("tickSize", 0.0)),
                "step_size": float(lot_filter.get("stepSize", 0.0)),
                "min_qty": float(lot_filter.get("minQty", 0.0)),
                "min_notional": float(min_notional_raw or 0.0),
                "maker_fee": None,
                "taker_fee": None,
            }
        )

    return records


class MetadataCollector:
    def __init__(
        self,
        settings: Settings,
        client: BinanceRestClient,
        storage: StorageManager,
        health: HealthState,
    ) -> None:
        self._symbols = settings.symbols
        self._client = client
        self._storage = storage
        self._health = health
        self._last_collected_day: str | None = None

    async def run(self) -> None:
        while True:
            try:
                await self._collect_if_needed()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Exchange metadata collection failed")

            sleep_seconds = self._seconds_until_next_utc_day() + 1.0
            await asyncio.sleep(sleep_seconds)

    async def _collect_if_needed(self) -> None:
        today = datetime.now(UTC).date().isoformat()
        if self._last_collected_day == today:
            return
        if self._metadata_exists_for_day(today):
            self._last_collected_day = today
            return
        await self._collect(today)
        self._last_collected_day = today

    def _metadata_exists_for_day(self, day: str) -> bool:
        final_path = (
            self._storage.data_dir / "metadata" / f"date={day}" / "metadata.parquet"
        )
        if final_path.exists():
            return True
        partition_dir = self._storage.data_dir / "metadata" / f"date={day}"
        return partition_dir.exists() and any(
            partition_dir.glob(".segment.*.parquet")
        )

    async def _collect(self, day: str) -> None:
        payload = await self._client.get_json("/fapi/v1/exchangeInfo")
        received_at = time.time_ns() // 1_000_000
        records = parse_exchange_metadata(self._symbols, payload, received_at)
        for record in records:
            await self._storage.write("metadata", record)

        self._health.last_metadata_update = utc_iso_now()
        logger.info(
            "Collected exchange metadata snapshot for %d symbols on %s",
            len(records),
            day,
        )

    @staticmethod
    def _seconds_until_next_utc_day() -> float:
        now = datetime.now(UTC)
        next_day = datetime.combine(
            now.date() + timedelta(days=1), datetime.min.time(), UTC
        )
        return max(0.0, (next_day - now).total_seconds())
