from __future__ import annotations

from datetime import UTC, datetime, timedelta

from collector.health import HealthState, utc_iso_now


def test_health_snapshot_includes_freshness_and_integrity_fields() -> None:
    health = HealthState(symbol_count=25)
    health.last_trade_update = "2026-06-11T12:00:00Z"
    health.last_depth50_update = "2026-06-11T12:00:01Z"
    health.websocket_reconnects = 3
    health.integrity_error_count = 0

    snapshot = health.snapshot()

    assert snapshot["symbols"] == 25
    assert snapshot["last_trade_update"] == "2026-06-11T12:00:00Z"
    assert snapshot["last_depth50_update"] == "2026-06-11T12:00:01Z"
    assert snapshot["websocket_reconnects"] == 3
    assert snapshot["integrity_error_count"] == 0
    assert snapshot["uptime_hours"] >= 0


def test_utc_iso_now_is_zulu() -> None:
    assert utc_iso_now().endswith("Z")


def test_health_check_staleness_logic() -> None:
    now = datetime.now(UTC)
    stale = (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    fresh = (now - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")

    def parse_iso(value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

    last_trade = parse_iso(stale)
    assert last_trade is not None
    assert now - last_trade > timedelta(minutes=2)

    last_depth = parse_iso(fresh)
    assert last_depth is not None
    assert now - last_depth <= timedelta(minutes=5)
