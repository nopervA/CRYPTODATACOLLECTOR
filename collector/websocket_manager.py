from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

if TYPE_CHECKING:
    from collector.telegram_alerts import TelegramAlerter

logger = logging.getLogger(__name__)

MessageHandler = Callable[[str, dict[str, Any], int], Awaitable[None]]


class WebSocketManager:
    """Maintain one reconnecting Binance combined-stream connection."""

    def __init__(
        self,
        name: str,
        base_url: str,
        route: str,
        streams: Sequence[str],
        handler: MessageHandler,
        reconnect_min_seconds: float,
        reconnect_max_seconds: float,
        alerter: TelegramAlerter | None = None,
    ) -> None:
        if not streams:
            raise ValueError("At least one websocket stream is required")
        if route not in {"public", "market", "private"}:
            raise ValueError(f"Unsupported Binance websocket route: {route}")
        self._name = name
        self._url = f"{base_url}/{route}/stream?streams={'/'.join(streams)}"
        self._handler = handler
        self._reconnect_min_seconds = reconnect_min_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._alerter = alerter

    async def run(self) -> None:
        delay = self._reconnect_min_seconds

        while True:
            connected_at = 0.0
            try:
                logger.info(
                    "Connecting websocket %s: %s", self._name, self._url
                )
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    open_timeout=20,
                    max_queue=2_048,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    connected_at = time.monotonic()
                    logger.info("Websocket connected: %s", self._name)
                    async for raw_message in websocket:
                        received_at = time.time_ns() // 1_000_000
                        try:
                            message = json.loads(raw_message)
                            stream = str(message.get("stream", ""))
                            data = message.get("data", message)
                            if isinstance(data, dict):
                                await self._handler(stream, data, received_at)
                        except (json.JSONDecodeError, TypeError, ValueError):
                            logger.exception(
                                "Invalid websocket message on %s", self._name
                            )

            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                logger.warning(
                    "Websocket disconnected (%s): %s", self._name, exc
                )
                if self._alerter is not None:
                    self._alerter.notify_websocket_disconnected(
                        self._name, str(exc)
                    )
            except Exception:
                logger.exception("Unexpected websocket failure: %s", self._name)
                if self._alerter is not None:
                    self._alerter.notify_unexpected_exception(
                        f"websocket:{self._name}", "unexpected websocket failure"
                    )

            if connected_at and time.monotonic() - connected_at >= 60.0:
                delay = self._reconnect_min_seconds

            sleep_seconds = delay + random.uniform(0.0, min(1.0, delay / 4.0))
            logger.info(
                "Scheduling websocket reconnect for %s in %.2fs",
                self._name,
                sleep_seconds,
            )
            if self._alerter is not None:
                self._alerter.notify_websocket_reconnect_scheduled(
                    self._name, sleep_seconds
                )
            await asyncio.sleep(sleep_seconds)
            delay = min(delay * 2.0, self._reconnect_max_seconds)
