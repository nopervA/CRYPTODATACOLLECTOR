from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from aiohttp import web


def utc_iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class HealthState:
    symbol_count: int
    started_monotonic: float = 0.0
    trades_received: int = 0
    liquidations_received: int = 0
    depth_snapshots_received: int = 0
    mark_price_updates_received: int = 0
    top_of_book_updates_received: int = 0
    last_trade_update: str | None = None
    last_depth50_update: str | None = None
    last_funding_update: str | None = None
    last_oi_update: str | None = None
    last_metadata_update: str | None = None
    websocket_reconnects: int = 0
    integrity_error_count: int = 0

    def __post_init__(self) -> None:
        if self.started_monotonic == 0.0:
            self.started_monotonic = time.monotonic()

    def snapshot(self) -> dict[str, int | float | str | None]:
        return {
            "uptime_hours": round(
                (time.monotonic() - self.started_monotonic) / 3600.0, 2
            ),
            "symbols": self.symbol_count,
            "trades_received": self.trades_received,
            "liquidations_received": self.liquidations_received,
            "depth_snapshots_received": self.depth_snapshots_received,
            "mark_price_updates_received": self.mark_price_updates_received,
            "top_of_book_updates_received": self.top_of_book_updates_received,
            "last_trade_update": self.last_trade_update,
            "last_depth50_update": self.last_depth50_update,
            "last_funding_update": self.last_funding_update,
            "last_oi_update": self.last_oi_update,
            "last_metadata_update": self.last_metadata_update,
            "websocket_reconnects": self.websocket_reconnects,
            "integrity_error_count": self.integrity_error_count,
        }


async def start_health_server(
    state: HealthState,
    host: str,
    port: int,
    *,
    integrity_error_count: Callable[[], int] | None = None,
    websocket_reconnects: Callable[[], int] | None = None,
) -> web.AppRunner:
    async def status(_: web.Request) -> web.Response:
        payload = state.snapshot()
        if integrity_error_count is not None:
            payload["integrity_error_count"] = integrity_error_count()
        if websocket_reconnects is not None:
            payload["websocket_reconnects"] = websocket_reconnects()
        return web.json_response(payload)

    app = web.Application()
    app.router.add_get("/status", status)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
