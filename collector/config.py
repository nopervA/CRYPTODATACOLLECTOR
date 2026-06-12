from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from collector.symbols import DEFAULT_SYMBOLS, parse_symbols


def _env_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _env_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    data_dir: Path = Path("data")
    log_dir: Path = Path("logs")
    websocket_base_url: str = "wss://fstream.binance.com"
    rest_base_url: str = "https://fapi.binance.com"
    funding_interval_seconds: float = 300.0
    oi_interval_seconds: float = 60.0
    health_host: str = "127.0.0.1"
    health_port: int = 8080
    reconnect_min_seconds: float = 1.0
    reconnect_max_seconds: float = 60.0
    rest_max_attempts: int = 6
    rest_concurrency: int = 4
    trade_queue_size: int = 50_000
    liquidation_queue_size: int = 10_000
    funding_queue_size: int = 2_000
    oi_queue_size: int = 5_000
    depth_queue_size: int = 20_000
    depth50_queue_size: int = 10_000
    depth50_rest_refresh_seconds: float = 5.0
    mark_price_queue_size: int = 5_000
    top_of_book_queue_size: int = 10_000
    ohlcv_queue_size: int = 2_000
    metadata_queue_size: int = 500
    oi_change_queue_size: int = 2_000
    taker_delta_queue_size: int = 2_000
    book_imbalance_queue_size: int = 10_000
    spread_state_queue_size: int = 10_000
    liquidity_stress_queue_size: int = 10_000
    funding_event_queue_size: int = 2_000
    funding_neutral_threshold: float = 0.00001
    funding_period_hours: float = 8.0
    dedup_cache_size: int = 500_000
    log_level: str = "INFO"
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    telegram_rate_limit_seconds: float = 900.0
    telegram_disk_free_gb_warn: float = 10.0
    telegram_repeated_reconnect_threshold: int = 3
    telegram_repeated_reconnect_window_seconds: float = 900.0
    telegram_funding_stale_minutes: float = 15.0
    telegram_oi_stale_minutes: float = 5.0
    telegram_daily_summary_hour_utc: int = 0
    telegram_daily_summary_minute_utc: int = 5
    backup_status_file: Path = Path("/var/lib/binance-futures-collector/backup_status.json")
    backup_report_dir: Path = Path("backup_reports")
    backup_enabled: bool = False
    backup_gcs_uri: str | None = None
    backup_hour_utc: int = 3
    backup_minute_utc: int = 0
    report_dir: Path = Path("reports")
    quality_report_hour_utc: int = 0
    quality_report_minute_utc: int = 15

    @property
    def log_file(self) -> Path:
        return self.log_dir / "collector.log"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            symbols=parse_symbols(os.getenv("COLLECTOR_SYMBOLS")),
            data_dir=Path(os.getenv("COLLECTOR_DATA_DIR", "data")).resolve(),
            log_dir=Path(os.getenv("COLLECTOR_LOG_DIR", "logs")).resolve(),
            websocket_base_url=os.getenv(
                "BINANCE_WS_BASE_URL", "wss://fstream.binance.com"
            ).rstrip("/"),
            rest_base_url=os.getenv(
                "BINANCE_REST_BASE_URL", "https://fapi.binance.com"
            ).rstrip("/"),
            funding_interval_seconds=_env_float(
                "FUNDING_INTERVAL_SECONDS", 300.0
            ),
            oi_interval_seconds=_env_float("OI_INTERVAL_SECONDS", 60.0),
            health_host=os.getenv("HEALTH_HOST", "127.0.0.1"),
            health_port=_env_int("HEALTH_PORT", 8080),
            reconnect_min_seconds=_env_float("RECONNECT_MIN_SECONDS", 1.0),
            reconnect_max_seconds=_env_float("RECONNECT_MAX_SECONDS", 60.0),
            rest_max_attempts=_env_int("REST_MAX_ATTEMPTS", 6),
            rest_concurrency=_env_int("REST_CONCURRENCY", 4),
            trade_queue_size=_env_int("TRADE_QUEUE_SIZE", 50_000),
            liquidation_queue_size=_env_int(
                "LIQUIDATION_QUEUE_SIZE", 10_000
            ),
            funding_queue_size=_env_int("FUNDING_QUEUE_SIZE", 2_000),
            oi_queue_size=_env_int("OI_QUEUE_SIZE", 5_000),
            depth_queue_size=_env_int("DEPTH_QUEUE_SIZE", 20_000),
            depth50_queue_size=_env_int("DEPTH50_QUEUE_SIZE", 10_000),
            depth50_rest_refresh_seconds=_env_float(
                "DEPTH50_REST_REFRESH_SECONDS", 5.0
            ),
            mark_price_queue_size=_env_int("MARK_PRICE_QUEUE_SIZE", 5_000),
            top_of_book_queue_size=_env_int("TOP_OF_BOOK_QUEUE_SIZE", 10_000),
            ohlcv_queue_size=_env_int("OHLCV_QUEUE_SIZE", 2_000),
            metadata_queue_size=_env_int("METADATA_QUEUE_SIZE", 500),
            oi_change_queue_size=_env_int("OI_CHANGE_QUEUE_SIZE", 2_000),
            taker_delta_queue_size=_env_int("TAKER_DELTA_QUEUE_SIZE", 2_000),
            book_imbalance_queue_size=_env_int(
                "BOOK_IMBALANCE_QUEUE_SIZE", 10_000
            ),
            spread_state_queue_size=_env_int("SPREAD_STATE_QUEUE_SIZE", 10_000),
            liquidity_stress_queue_size=_env_int(
                "LIQUIDITY_STRESS_QUEUE_SIZE", 10_000
            ),
            funding_event_queue_size=_env_int("FUNDING_EVENT_QUEUE_SIZE", 2_000),
            funding_neutral_threshold=_env_float(
                "FUNDING_NEUTRAL_THRESHOLD", 0.00001
            ),
            funding_period_hours=_env_float("FUNDING_PERIOD_HOURS", 8.0),
            dedup_cache_size=_env_int("DEDUP_CACHE_SIZE", 500_000),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN") or None,
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or None,
            telegram_rate_limit_seconds=_env_float("TELEGRAM_RATE_LIMIT_SECONDS", 900.0),
            telegram_disk_free_gb_warn=_env_float("TELEGRAM_DISK_FREE_GB_WARN", 10.0),
            telegram_repeated_reconnect_threshold=_env_int(
                "TELEGRAM_REPEATED_RECONNECT_THRESHOLD", 3
            ),
            telegram_repeated_reconnect_window_seconds=_env_float(
                "TELEGRAM_REPEATED_RECONNECT_WINDOW_SECONDS", 900.0
            ),
            telegram_funding_stale_minutes=_env_float(
                "TELEGRAM_FUNDING_STALE_MINUTES", 15.0
            ),
            telegram_oi_stale_minutes=_env_float("TELEGRAM_OI_STALE_MINUTES", 5.0),
            telegram_daily_summary_hour_utc=max(
                0, min(23, int(os.getenv("TELEGRAM_DAILY_SUMMARY_HOUR_UTC", "0")))
            ),
            telegram_daily_summary_minute_utc=max(
                0, min(59, int(os.getenv("TELEGRAM_DAILY_SUMMARY_MINUTE_UTC", "5")))
            ),
            backup_status_file=Path(
                os.getenv(
                    "BACKUP_STATUS_FILE",
                    "/var/lib/binance-futures-collector/backup_status.json",
                )
            ).resolve(),
            backup_report_dir=Path(
                os.getenv("BACKUP_REPORT_DIR", "backup_reports")
            ).resolve(),
            backup_enabled=os.getenv("BACKUP_ENABLED", "0") == "1",
            backup_gcs_uri=os.getenv("BACKUP_GCS_URI") or None,
            backup_hour_utc=max(0, min(23, int(os.getenv("BACKUP_HOUR_UTC", "3")))),
            backup_minute_utc=max(0, min(59, int(os.getenv("BACKUP_MINUTE_UTC", "0")))),
            report_dir=Path(os.getenv("COLLECTOR_REPORT_DIR", "reports")).resolve(),
            quality_report_hour_utc=max(
                0, min(23, int(os.getenv("QUALITY_REPORT_HOUR_UTC", "0")))
            ),
            quality_report_minute_utc=max(
                0, min(59, int(os.getenv("QUALITY_REPORT_MINUTE_UTC", "15")))
            ),
        )
