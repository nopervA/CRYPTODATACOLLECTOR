from __future__ import annotations

import time
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
    last_funding_update: str | None = None
    last_oi_update: str | None = None
    last_metadata_update: str | None = None

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
            "last_funding_update": self.last_funding_update,
            "last_oi_update": self.last_oi_update,
            "last_metadata_update": self.last_metadata_update,
        }


async def start_health_server(
    state: HealthState, host: str, port: int
) -> web.AppRunner:
    async def status(_: web.Request) -> web.Response:
        return web.json_response(state.snapshot())

    app = web.Application()
    app.router.add_get("/status", status)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
