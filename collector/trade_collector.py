from __future__ import annotations

import logging
import time
from typing import Any

from collector.config import Settings
from collector.health import HealthState, utc_iso_now
from collector.ohlcv_builder import OhlcvBuilder
from collector.taker_delta_builder import TakerDeltaBuilder
from collector.rest_client import BinanceRestClient
from collector.storage import StorageManager
from collector.telegram_alerts import TelegramAlerter
from collector.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)


def parse_agg_trade(
    payload: dict[str, Any], received_at: int
) -> dict[str, Any]:
    return {
        "timestamp": int(payload["T"]),
        "symbol": str(payload["s"]).upper(),
        "price": float(payload["p"]),
        "quantity": float(payload["q"]),
        "is_buyer_maker": bool(payload["m"]),
        "trade_id": int(payload["a"]),
        "is_recovered": False,
        "received_at": received_at,
    }


def parse_rest_agg_trade(
    payload: dict[str, Any], symbol: str, received_at: int
) -> dict[str, Any]:
    record = parse_agg_trade({**payload, "s": symbol}, received_at)
    record["is_recovered"] = True
    return record


class TradeCollector:
    def __init__(
        self,
        settings: Settings,
        rest_client: BinanceRestClient,
        storage: StorageManager,
        health: HealthState,
        ohlcv_builder: OhlcvBuilder | None = None,
        taker_delta_builder: TakerDeltaBuilder | None = None,
        alerter: TelegramAlerter | None = None,
    ) -> None:
        self._rest_client = rest_client
        self._storage = storage
        self._health = health
        self._ohlcv_builder = ohlcv_builder
        self._taker_delta_builder = taker_delta_builder
        self._last_trade_id: dict[str, int] = {}
        streams = [f"{symbol.lower()}@aggTrade" for symbol in settings.symbols]
        self._websocket = WebSocketManager(
            "trades",
            settings.websocket_base_url,
            "market",
            streams,
            self._handle_message,
            settings.reconnect_min_seconds,
            settings.reconnect_max_seconds,
            alerter,
        )

    async def run(self) -> None:
        await self._websocket.run()

    async def _handle_message(
        self, _: str, payload: dict[str, Any], received_at: int
    ) -> None:
        try:
            record = parse_agg_trade(payload, received_at)
        except (KeyError, TypeError, ValueError):
            logger.exception("Could not parse aggregate trade")
            return

        symbol = str(record["symbol"])
        trade_id = int(record["trade_id"])
        previous_id = self._last_trade_id.get(symbol)
        if previous_id is not None and trade_id <= previous_id:
            return
        if previous_id is not None and trade_id > previous_id + 1:
            recovered = await self._recover_gap(
                symbol, previous_id + 1, trade_id - 1
            )
            self._health.trades_received += recovered

        await self._storage.write("trades", record)
        if self._ohlcv_builder is not None:
            await self._ohlcv_builder.on_trade(record)
        if self._taker_delta_builder is not None:
            await self._taker_delta_builder.on_trade(record)
        self._last_trade_id[symbol] = trade_id
        self._health.trades_received += 1
        self._health.last_trade_update = utc_iso_now()

    async def _recover_gap(
        self, symbol: str, first_id: int, last_id: int
    ) -> int:
        logger.warning(
            "Aggregate-trade gap detected for %s: %d-%d; starting REST recovery",
            symbol,
            first_id,
            last_id,
        )
        next_id = first_id
        recovered = 0
        recovery_complete = True
        first_unresolved_id: int | None = None

        try:
            while next_id <= last_id:
                payload = await self._rest_client.get_json(
                    "/fapi/v1/aggTrades",
                    {
                        "symbol": symbol,
                        "fromId": str(next_id),
                        "limit": "1000",
                    },
                )
                if not isinstance(payload, list) or not payload:
                    break

                batch = sorted(
                    (
                        item
                        for item in payload
                        if next_id <= int(item["a"]) <= last_id
                    ),
                    key=lambda item: int(item["a"]),
                )
                if not batch:
                    break

                received_at = time.time_ns() // 1_000_000
                for item in batch:
                    item_id = int(item["a"])
                    if item_id < next_id:
                        continue
                    if item_id > next_id:
                        recovery_complete = False
                        if first_unresolved_id is None:
                            first_unresolved_id = next_id
                        logger.error(
                            "REST trade recovery remained discontinuous for "
                            "%s: expected %d, received %d",
                            symbol,
                            next_id,
                            item_id,
                        )
                    record = parse_rest_agg_trade(item, symbol, received_at)
                    await self._storage.write("trades", record)
                    if self._ohlcv_builder is not None:
                        await self._ohlcv_builder.on_trade(record)
                    if self._taker_delta_builder is not None:
                        await self._taker_delta_builder.on_trade(record)
                    self._last_trade_id[symbol] = item_id
                    next_id = item_id + 1
                    recovered += 1

                if len(payload) < 1000:
                    break
        except Exception:
            logger.exception(
                "Aggregate-trade REST recovery failed for %s: %d-%d",
                symbol,
                next_id,
                last_id,
            )

        if next_id <= last_id or not recovery_complete:
            logger.error(
                "Aggregate-trade recovery incomplete for %s; requested %d-%d, "
                "next unresolved ID is %d",
                symbol,
                first_id,
                last_id,
                first_unresolved_id or next_id,
            )
        else:
            logger.info("Recovered %d aggregate trades for %s", recovered, symbol)
        return recovered
