from __future__ import annotations

import logging
from typing import Any

from collector.config import Settings
from collector.health import HealthState
from collector.storage import StorageManager
from collector.telegram_alerts import TelegramAlerter
from collector.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)


def parse_liquidation(
    payload: dict[str, Any], received_at: int
) -> dict[str, Any]:
    order = payload["o"]
    average_price = float(order.get("ap") or 0.0)
    order_price = float(order.get("p") or 0.0)
    filled_quantity = float(order.get("z") or 0.0)
    original_quantity = float(order.get("q") or 0.0)
    price = average_price if average_price > 0.0 else order_price
    quantity = filled_quantity if filled_quantity > 0.0 else original_quantity

    return {
        "timestamp": int(payload["E"]),
        "symbol": str(order["s"]).upper(),
        "side": str(order["S"]).upper(),
        "price": price,
        "quantity": quantity,
        "notional": price * quantity,
        "order_timestamp": int(order["T"]) if order.get("T") is not None else None,
        "received_at": received_at,
    }


class LiquidationCollector:
    def __init__(
        self,
        settings: Settings,
        storage: StorageManager,
        health: HealthState,
        alerter: TelegramAlerter | None = None,
    ) -> None:
        self._storage = storage
        self._health = health
        streams = [f"{symbol.lower()}@forceOrder" for symbol in settings.symbols]
        self._websocket = WebSocketManager(
            "liquidations",
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
            record = parse_liquidation(payload, received_at)
        except (KeyError, TypeError, ValueError):
            logger.exception("Could not parse liquidation event")
            return
        await self._storage.write("liquidations", record)
        self._health.liquidations_received += 1
