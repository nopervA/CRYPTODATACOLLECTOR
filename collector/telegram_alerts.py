from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import aiohttp

from collector.config import Settings
from collector.health import HealthState, utc_iso_now

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class AlertSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertEventType(StrEnum):
    COLLECTOR_STOPPED = "collector_stopped"
    COLLECTOR_RESTARTED = "collector_restarted"
    WEBSOCKET_DISCONNECTED = "websocket_disconnected"
    REPEATED_RECONNECTS = "repeated_reconnects"
    FUNDING_UPDATES_MISSING = "funding_updates_missing"
    OI_UPDATES_MISSING = "oi_updates_missing"
    DISK_USAGE_HIGH = "disk_usage_high"
    BACKUP_FAILURE = "backup_failure"
    DATA_INTEGRITY_FAILURE = "data_integrity_failure"
    UNEXPECTED_EXCEPTION = "unexpected_exception"
    DAILY_SUMMARY = "daily_summary"


@dataclass(slots=True)
class AlertEvent:
    event_type: AlertEventType
    severity: AlertSeverity
    message: str
    details: str | None = None


@dataclass
class TelegramAlerter:
    """Non-blocking Telegram notifier with per-event rate limiting."""

    settings: Settings
    session: aiohttp.ClientSession
    _queue: asyncio.Queue[AlertEvent | None] = field(init=False)
    _worker_task: asyncio.Task[None] | None = field(default=None, init=False)
    _last_sent: dict[str, float] = field(default_factory=dict, init=False)
    _reconnect_times: dict[str, deque[float]] = field(
        default_factory=lambda: defaultdict(deque), init=False
    )
    _integrity_errors_seen: int = field(default=0, init=False)
    _daily_summary_sent_for: str | None = field(default=None, init=False)
    websocket_disconnects: int = field(default=0, init=False)
    websocket_reconnects: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._queue = asyncio.Queue(maxsize=1_000)

    @property
    def enabled(self) -> bool:
        return bool(self.settings.telegram_bot_token and self.settings.telegram_chat_id)

    def consume_daily_websocket_counts(self) -> tuple[int, int]:
        disconnects = self.websocket_disconnects
        reconnects = self.websocket_reconnects
        self.websocket_disconnects = 0
        self.websocket_reconnects = 0
        return disconnects, reconnects

    async def start(self) -> None:
        if not self.enabled:
            logger.info("Telegram alerts disabled (token or chat id not set)")
            return
        self._worker_task = asyncio.create_task(
            self._worker(), name="telegram-alerts"
        )

    async def close(self) -> None:
        if not self.enabled:
            return
        self.notify_collector_stopped()
        await self._queue.join()
        if self._worker_task is not None:
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            try:
                await asyncio.wait_for(self._worker_task, timeout=10.0)
            except asyncio.TimeoutError:
                self._worker_task.cancel()
                await asyncio.gather(self._worker_task, return_exceptions=True)

    def notify_collector_restarted(self, details: str | None = None) -> None:
        self.notify(
            AlertEventType.COLLECTOR_RESTARTED,
            AlertSeverity.INFO,
            "Collector started",
            details=details,
            bypass_rate_limit=True,
        )

    def notify_collector_stopped(self) -> None:
        self.notify(
            AlertEventType.COLLECTOR_STOPPED,
            AlertSeverity.WARNING,
            "Collector shutting down",
            bypass_rate_limit=True,
        )

    def notify(
        self,
        event_type: AlertEventType,
        severity: AlertSeverity,
        message: str,
        details: str | None = None,
        *,
        bypass_rate_limit: bool = False,
    ) -> None:
        if not self.enabled:
            return
        event = AlertEvent(event_type, severity, message, details)
        if not bypass_rate_limit and self._is_rate_limited(event.event_type):
            return
        try:
            self._queue.put_nowait(event)
            if not bypass_rate_limit:
                self._last_sent[event.event_type.value] = time.monotonic()
            logger.info(
                "Telegram alert emitted: event=%s severity=%s message=%s",
                event.event_type.value,
                event.severity.value,
                message,
            )
        except asyncio.QueueFull:
            logger.warning("Telegram alert queue full; dropping %s", event_type.value)

    def notify_websocket_disconnected(self, stream_name: str, reason: str) -> None:
        self.websocket_disconnects += 1
        self.notify(
            AlertEventType.WEBSOCKET_DISCONNECTED,
            AlertSeverity.WARNING,
            f"WebSocket disconnected: {stream_name}",
            details=reason,
        )

    def notify_websocket_reconnect_scheduled(
        self, stream_name: str, delay_seconds: float
    ) -> None:
        self.websocket_reconnects += 1
        now = time.monotonic()
        window = self.settings.telegram_repeated_reconnect_window_seconds
        threshold = self.settings.telegram_repeated_reconnect_threshold
        history = self._reconnect_times[stream_name]
        history.append(now)
        while history and now - history[0] > window:
            history.popleft()
        if len(history) >= threshold:
            self.notify(
                AlertEventType.REPEATED_RECONNECTS,
                AlertSeverity.CRITICAL,
                f"Repeated reconnects on {stream_name}",
                details=f"{len(history)} reconnects in {int(window // 60)} minutes",
            )
            history.clear()

    def notify_unexpected_exception(self, source: str, error: str) -> None:
        self.notify(
            AlertEventType.UNEXPECTED_EXCEPTION,
            AlertSeverity.CRITICAL,
            f"Unexpected exception: {source}",
            details=error[:500],
        )

    async def run_health_monitor(
        self,
        health: HealthState,
        *,
        free_gb: float,
        integrity_error_count: int,
    ) -> None:
        if not self.enabled:
            return

        if free_gb < self.settings.telegram_disk_free_gb_warn:
            self.notify(
                AlertEventType.DISK_USAGE_HIGH,
                AlertSeverity.CRITICAL if free_gb < self.settings.telegram_disk_free_gb_warn / 2
                else AlertSeverity.WARNING,
                "Disk space low",
                details=f"free={free_gb:.2f} GB (threshold={self.settings.telegram_disk_free_gb_warn:.2f} GB)",
            )

        uptime_hours = (time.monotonic() - health.started_monotonic) / 3600.0
        now = datetime.now(UTC)

        if uptime_hours >= self.settings.telegram_oi_stale_minutes / 60.0:
            last_oi = _parse_iso(health.last_oi_update)
            if last_oi is None or now - last_oi > timedelta(
                minutes=self.settings.telegram_oi_stale_minutes
            ):
                self.notify(
                    AlertEventType.OI_UPDATES_MISSING,
                    AlertSeverity.WARNING,
                    "Open-interest updates missing",
                    details=f"last_oi_update={health.last_oi_update}",
                )

        if uptime_hours >= self.settings.telegram_funding_stale_minutes / 60.0:
            last_funding = _parse_iso(health.last_funding_update)
            if last_funding is None or now - last_funding > timedelta(
                minutes=self.settings.telegram_funding_stale_minutes
            ):
                self.notify(
                    AlertEventType.FUNDING_UPDATES_MISSING,
                    AlertSeverity.WARNING,
                    "Funding updates missing",
                    details=f"last_funding_update={health.last_funding_update}",
                )

        if integrity_error_count > self._integrity_errors_seen:
            new_errors = integrity_error_count - self._integrity_errors_seen
            self._integrity_errors_seen = integrity_error_count
            self.notify(
                AlertEventType.DATA_INTEGRITY_FAILURE,
                AlertSeverity.CRITICAL,
                "Parquet data integrity failure",
                details=f"{new_errors} new compaction error(s)",
            )

        self._check_backup_status()

    async def maybe_send_daily_summary(
        self, health: HealthState, *, free_gb: float, total_gb: float
    ) -> None:
        if not self.enabled:
            return
        now = datetime.now(UTC)
        if (
            now.hour != self.settings.telegram_daily_summary_hour_utc
            or now.minute < self.settings.telegram_daily_summary_minute_utc
        ):
            return
        day_key = now.date().isoformat()
        if self._daily_summary_sent_for == day_key:
            return
        self._daily_summary_sent_for = day_key

        backup_status = _read_backup_status(self.settings.backup_status_file)
        used_gb = max(0.0, total_gb - free_gb)
        message = (
            f"trades={health.trades_received:,}\n"
            f"liquidations={health.liquidations_received:,}\n"
            f"disk_used={used_gb:.1f} GB / {total_gb:.1f} GB\n"
            f"uptime={health.snapshot()['uptime_hours']} h\n"
            f"ws_disconnects={self.websocket_disconnects}\n"
            f"ws_reconnects={self.websocket_reconnects}\n"
            f"backup={backup_status}"
        )
        self.notify(
            AlertEventType.DAILY_SUMMARY,
            AlertSeverity.INFO,
            "Daily collector summary",
            details=message,
            bypass_rate_limit=True,
        )

    def _check_backup_status(self) -> None:
        payload = _read_backup_status_payload(self.settings.backup_status_file)
        if payload is None:
            return
        if payload.get("success") is False:
            self.notify(
                AlertEventType.BACKUP_FAILURE,
                AlertSeverity.CRITICAL,
                "Backup failed",
                details=str(
                    payload.get("error_message")
                    or payload.get("error", "unknown error")
                )[:500],
            )

    def _is_rate_limited(self, event_type: AlertEventType) -> bool:
        if event_type == AlertEventType.DAILY_SUMMARY:
            return False
        now = time.monotonic()
        last = self._last_sent.get(event_type.value)
        if last is not None and now - last < self.settings.telegram_rate_limit_seconds:
            return True
        return False

    async def _worker(self) -> None:
        while True:
            event = await self._queue.get()
            if event is None:
                self._queue.task_done()
                return
            try:
                await self._send(event)
                self._last_sent[event.event_type.value] = time.monotonic()
            except Exception:
                logger.warning(
                    "Telegram alert delivery failed for %s",
                    event.event_type.value,
                    exc_info=True,
                )
            finally:
                self._queue.task_done()

    async def _send(self, event: AlertEvent) -> None:
        token = self.settings.telegram_bot_token
        chat_id = self.settings.telegram_chat_id
        if not token or not chat_id:
            return
        text = _format_message(event)
        url = TELEGRAM_API.format(token=token)
        async with self.session.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=10.0),
        ) as response:
            if response.status >= 400:
                body = await response.text()
                raise RuntimeError(f"Telegram HTTP {response.status}: {body[:200]}")
        logger.info(
            "Telegram alert sent: event=%s severity=%s",
            event.event_type.value,
            event.severity.value,
        )


def _format_message(event: AlertEvent) -> str:
    lines = [
        f"[{event.severity}] {event.event_type.value}",
        event.message,
    ]
    if event.details:
        lines.append(event.details)
    lines.append(f"time={utc_iso_now()}")
    return "\n".join(lines)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _read_backup_status_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_backup_status(path: Path) -> str:
    payload = _read_backup_status_payload(path)
    if payload is None:
        return "unknown"
    if payload.get("success") is True:
        when = payload.get("timestamp") or payload.get("last_run", "n/a")
        return f"ok ({when})"
    if payload.get("success") is False:
        when = payload.get("timestamp") or payload.get("last_run", "n/a")
        return f"failed ({when})"
    return "unknown"
