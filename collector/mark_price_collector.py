from __future__ import annotations

import logging
from typing import Any

from collector.config import Settings
from collector.health import HealthState
from collector.storage import StorageManager
from collector.telegram_alerts import TelegramAlerter
from collector.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)


def parse_mark_price(
    payload: dict[str, Any], received_at: int
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "event_time": int(payload["E"]),
        "symbol": str(payload["s"]).upper(),
        "received_at": received_at,
        "mark_price": float(payload["p"]),
        "index_price": float(payload["i"]),
        "predicted_funding_rate": float(payload["r"]),
        "next_funding_time": int(payload["T"]),
    }
    settle_price = payload.get("P")
    record["estimated_settle_price"] = (
        float(settle_price) if settle_price is not None else None
    )
    return record


class MarkPriceCollector:
    _DEBUG_MESSAGE_LIMIT = 5

    def __init__(
        self,
        settings: Settings,
        storage: StorageManager,
        health: HealthState,
        alerter: TelegramAlerter | None = None,
    ) -> None:
        self._storage = storage
        self._health = health
        self._debug_messages_logged = 0
        streams = [
            f"{symbol.lower()}@markPrice@1s" for symbol in settings.symbols
        ]
        self._websocket = WebSocketManager(
            "mark_price",
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
        self, stream: str, payload: dict[str, Any], received_at: int
    ) -> None:
        if self._debug_messages_logged < self._DEBUG_MESSAGE_LIMIT:
            logger.info(
                "Mark price sample %d stream=%s payload=%s",
                self._debug_messages_logged + 1,
                stream,
                payload,
            )
            self._debug_messages_logged += 1

        try:
            record = parse_mark_price(payload, received_at)
        except (KeyError, TypeError, ValueError):
            logger.exception("Could not parse mark price update")
            return

        await self._storage.write("mark_price", record)
        self._health.mark_price_updates_received += 1
