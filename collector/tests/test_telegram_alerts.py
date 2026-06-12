from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import aiohttp

from collector.config import Settings
from collector.health import HealthState
from collector.telegram_alerts import (
    AlertEventType,
    AlertSeverity,
    TelegramAlerter,
    _format_message,
)


def test_format_message_includes_severity_and_details() -> None:
    from collector.telegram_alerts import AlertEvent

    text = _format_message(
        AlertEvent(
            AlertEventType.WEBSOCKET_DISCONNECTED,
            AlertSeverity.WARNING,
            "WebSocket disconnected: trades",
            "connection reset",
        )
    )
    assert "[WARNING] websocket_disconnected" in text
    assert "connection reset" in text


def test_notify_disabled_without_credentials() -> None:
    settings = replace(Settings(), telegram_bot_token=None, telegram_chat_id=None)

    async def scenario() -> None:
        async with aiohttp.ClientSession() as session:
            alerter = TelegramAlerter(settings, session)
            assert alerter.enabled is False
            alerter.notify(
                AlertEventType.COLLECTOR_RESTARTED,
                AlertSeverity.INFO,
                "should not send",
            )
            await alerter.start()
            await alerter.close()

    asyncio.run(scenario())


def test_rate_limiting_blocks_duplicate_event_types() -> None:
    settings = replace(
        Settings(),
        telegram_bot_token="token",
        telegram_chat_id="123",
        telegram_rate_limit_seconds=900.0,
    )

    async def scenario() -> None:
        async with aiohttp.ClientSession() as session:
            alerter = TelegramAlerter(settings, session)
            alerter.notify(
                AlertEventType.DISK_USAGE_HIGH,
                AlertSeverity.WARNING,
                "first",
            )
            alerter.notify(
                AlertEventType.DISK_USAGE_HIGH,
                AlertSeverity.WARNING,
                "second",
            )
            assert alerter._queue.qsize() == 1

    asyncio.run(scenario())


def test_repeated_reconnects_alert_after_threshold() -> None:
    settings = replace(
        Settings(),
        telegram_bot_token="token",
        telegram_chat_id="123",
        telegram_repeated_reconnect_threshold=3,
        telegram_repeated_reconnect_window_seconds=900.0,
    )
    alerter = TelegramAlerter(settings, session=object())  # type: ignore[arg-type]

    for _ in range(3):
        alerter.notify_websocket_reconnect_scheduled("trades", 1.0)

    queued = []
    while not alerter._queue.empty():
        item = alerter._queue.get_nowait()
        if item is not None:
            queued.append(item)
    assert any(
        event.event_type == AlertEventType.REPEATED_RECONNECTS for event in queued
    )


def test_health_monitor_detects_stale_oi() -> None:
    settings = replace(
        Settings(),
        telegram_bot_token="token",
        telegram_chat_id="123",
        telegram_oi_stale_minutes=5.0,
    )
    health = HealthState(symbol_count=1)
    health.started_monotonic = time.monotonic() - 3600.0
    health.last_oi_update = (
        datetime.now(UTC) - timedelta(minutes=30)
    ).isoformat().replace("+00:00", "Z")

    async def scenario() -> None:
        async with aiohttp.ClientSession() as session:
            alerter = TelegramAlerter(settings, session)
            await alerter.run_health_monitor(
                health, free_gb=100.0, integrity_error_count=0
            )
            event = alerter._queue.get_nowait()
            assert event is not None
            assert event.event_type == AlertEventType.OI_UPDATES_MISSING

    asyncio.run(scenario())


def test_telegram_send_failure_does_not_raise() -> None:
    settings = replace(
        Settings(),
        telegram_bot_token="bad-token",
        telegram_chat_id="123",
    )

    async def scenario() -> None:
        async with aiohttp.ClientSession() as session:
            alerter = TelegramAlerter(settings, session)
            await alerter.start()
            alerter.notify(
                AlertEventType.UNEXPECTED_EXCEPTION,
                AlertSeverity.CRITICAL,
                "test",
            )
            await asyncio.sleep(0.2)
            await alerter.close()

    asyncio.run(scenario())


class _MockTelegramResponse:
    status = 200

    async def text(self) -> str:
        return '{"ok": true}'

    async def __aenter__(self) -> _MockTelegramResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _MockTelegramSession:
    def __init__(self) -> None:
        self.posts: list[dict[str, object]] = []

    def post(self, url: str, **kwargs: object) -> _MockTelegramResponse:
        self.posts.append({"url": url, **kwargs})
        return _MockTelegramResponse()


def test_worker_sends_first_queued_event() -> None:
    settings = replace(
        Settings(),
        telegram_bot_token="token",
        telegram_chat_id="123",
    )

    async def scenario() -> None:
        session = _MockTelegramSession()
        alerter = TelegramAlerter(settings, session)  # type: ignore[arg-type]
        await alerter.start()
        alerter.notify(
            AlertEventType.DISK_USAGE_HIGH,
            AlertSeverity.WARNING,
            "disk low",
        )
        await asyncio.sleep(0.05)
        await alerter.close()
        assert len(session.posts) >= 1
        texts = [str(post["json"]["text"]) for post in session.posts]
        assert any("disk_usage_high" in text for text in texts)

    asyncio.run(scenario())


def test_startup_and_shutdown_notifications_are_sent() -> None:
    settings = replace(
        Settings(),
        telegram_bot_token="token",
        telegram_chat_id="123",
    )

    async def scenario() -> None:
        session = _MockTelegramSession()
        alerter = TelegramAlerter(settings, session)  # type: ignore[arg-type]
        await alerter.start()
        alerter.notify_collector_restarted("symbols=25")
        await alerter.close()
        texts = [
            str(post["json"]["text"])
            for post in session.posts
            if isinstance(post.get("json"), dict)
        ]
        assert any("collector_restarted" in text for text in texts)
        assert any("collector_stopped" in text for text in texts)
        assert texts.index(
            next(t for t in texts if "collector_restarted" in t)
        ) < texts.index(next(t for t in texts if "collector_stopped" in t))

    asyncio.run(scenario())

