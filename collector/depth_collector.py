from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from collector.config import Settings
from collector.depth_metrics import (
    build_book_imbalance_record,
    build_liquidity_stress_record,
    merge_depth50,
    parse_depth_ws,
)
from collector.health import HealthState
from collector.rest_client import BinanceRestClient
from collector.spread_state_tracker import SpreadStateTracker
from collector.storage import StorageManager
from collector.top_of_book import build_top_of_book_record
from collector.telegram_alerts import TelegramAlerter
from collector.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)

# Backward-compatible alias for tests and callers expecting depth20 parsing.
parse_depth20 = parse_depth_ws


class DepthCollector:
    def __init__(
        self,
        settings: Settings,
        rest_client: BinanceRestClient,
        storage: StorageManager,
        health: HealthState,
        spread_tracker: SpreadStateTracker | None = None,
        alerter: TelegramAlerter | None = None,
    ) -> None:
        self._settings = settings
        self._rest_client = rest_client
        self._storage = storage
        self._health = health
        self._spread_tracker = spread_tracker
        self._last_stored_second: dict[str, int] = {}
        self._last_update_id: dict[str, int] = {}
        self._rest_depth: dict[str, dict[str, Any]] = {}
        self._previous_total_notional: dict[str, float] = {}
        streams = [
            f"{symbol.lower()}@depth20@500ms" for symbol in settings.symbols
        ]
        self._websocket = WebSocketManager(
            "depth50",
            settings.websocket_base_url,
            "public",
            streams,
            self._handle_message,
            settings.reconnect_min_seconds,
            settings.reconnect_max_seconds,
            alerter,
        )

    async def run(self) -> None:
        await asyncio.gather(
            self._websocket.run(),
            self._rest_refresh_loop(),
        )

    async def _rest_refresh_loop(self) -> None:
        interval = self._settings.depth50_rest_refresh_seconds
        while True:
            started = time.monotonic()
            try:
                results = await asyncio.gather(
                    *(self._refresh_symbol(symbol) for symbol in self._settings.symbols),
                    return_exceptions=True,
                )
                for symbol, result in zip(self._settings.symbols, results, strict=True):
                    if isinstance(result, BaseException):
                        logger.warning(
                            "Depth REST refresh failed for %s: %s", symbol, result
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Depth REST refresh loop failed")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, interval - elapsed))

    async def _refresh_symbol(self, symbol: str) -> None:
        payload = await self._rest_client.get_json(
            "/fapi/v1/depth",
            {"symbol": symbol, "limit": "50"},
        )
        self._rest_depth[symbol] = payload

    async def _handle_message(
        self, _: str, payload: dict[str, Any], received_at: int
    ) -> None:
        try:
            ws_record = parse_depth_ws(payload, received_at, levels=20)
        except (KeyError, IndexError, TypeError, ValueError):
            logger.exception("Could not parse depth snapshot")
            return

        symbol = str(ws_record["symbol"])
        previous_update_id = self._last_update_id.get(symbol)
        previous_from_exchange = ws_record.get("prev_final_update_id")
        if (
            previous_update_id is not None
            and previous_from_exchange is not None
            and int(previous_from_exchange) != previous_update_id
        ):
            logger.warning(
                "Depth update chain break for %s: expected pu=%d, received pu=%d",
                symbol,
                previous_update_id,
                int(previous_from_exchange),
            )
        final_update_id = ws_record.get("final_update_id")
        if final_update_id is not None:
            self._last_update_id[symbol] = int(final_update_id)

        event_second = int(ws_record["timestamp"]) // 1_000
        if event_second <= self._last_stored_second.get(symbol, -1):
            return
        self._last_stored_second[symbol] = event_second

        depth50 = merge_depth50(ws_record, self._rest_depth.get(symbol))
        top_of_book = build_top_of_book_record(depth50)
        is_wide_spread = False
        if self._spread_tracker is not None:
            is_wide_spread = await self._spread_tracker.on_top_of_book(top_of_book)

        previous_total = self._previous_total_notional.get(symbol)
        liquidity_stress = build_liquidity_stress_record(
            depth50,
            previous_total_notional=previous_total,
            is_wide_spread=is_wide_spread,
        )
        self._previous_total_notional[symbol] = (
            liquidity_stress["bid_depth_notional"]
            + liquidity_stress["ask_depth_notional"]
        )

        await self._storage.write("depth50", depth50)
        await self._storage.write("top_of_book", top_of_book)
        await self._storage.write(
            "book_imbalance", build_book_imbalance_record(depth50)
        )
        await self._storage.write("liquidity_stress", liquidity_stress)

        self._health.depth_snapshots_received += 1
        self._health.top_of_book_updates_received += 1
