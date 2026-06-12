from __future__ import annotations

import asyncio
import logging
import shutil
import signal
from datetime import UTC, datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

import aiohttp

from collector.config import Settings
from collector.daily_quality_report import generate_daily_quality_report
from collector.depth_collector import DepthCollector
from collector.funding_event_tracker import FundingEventTracker
from collector.funding_collector import FundingCollector
from collector.health import HealthState, start_health_server
from collector.liquidation_collector import LiquidationCollector
from collector.mark_price_collector import MarkPriceCollector
from collector.metadata_collector import MetadataCollector
from collector.oi_change_tracker import OiChangeTracker
from collector.oi_collector import OpenInterestCollector
from collector.ohlcv_builder import OhlcvBuilder
from collector.rest_client import BinanceRestClient
from collector.spread_state_tracker import SpreadStateTracker
from collector.runtime_metrics import RuntimeMetrics
from collector.storage import StorageManager
from collector.taker_delta_builder import TakerDeltaBuilder
from collector.telegram_alerts import TelegramAlerter
from collector.trade_collector import TradeCollector

logger = logging.getLogger(__name__)


def configure_logging(settings: Settings) -> None:
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    level = getattr(logging, settings.log_level, logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(name)s %(message)s"
    )
    formatter.converter = __import__("time").gmtime

    file_handler = TimedRotatingFileHandler(
        settings.log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        utc=True,
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)


async def _monitor_loop(
    storage: StorageManager,
    health: HealthState,
    alerter: TelegramAlerter,
    runtime_metrics: RuntimeMetrics,
    settings: Settings,
) -> None:
    last_report_day: str | None = None
    while True:
        await asyncio.sleep(60.0)
        usage = shutil.disk_usage(storage.data_dir)
        free_gb = usage.free / (1024**3)
        total_gb = usage.total / (1024**3)
        runtime_metrics.record_queue_sizes(storage.queue_sizes())
        log_method = logger.warning if free_gb < 10.0 else logger.info
        log_method(
            "Storage queue sizes: %s; free disk: %.2f GB",
            storage.queue_sizes(),
            free_gb,
        )
        await alerter.run_health_monitor(
            health,
            free_gb=free_gb,
            integrity_error_count=storage.integrity_error_count,
        )
        await alerter.maybe_send_daily_summary(
            health, free_gb=free_gb, total_gb=total_gb
        )
        now = datetime.now(UTC)
        if (
            now.hour == settings.quality_report_hour_utc
            and now.minute >= settings.quality_report_minute_utc
        ):
            report_day = (now.date() - timedelta(days=1)).isoformat()
            if last_report_day != report_day:
                try:
                    disconnects, reconnects = alerter.consume_daily_websocket_counts()
                    runtime_metrics.websocket_disconnects = disconnects
                    runtime_metrics.websocket_reconnects = reconnects
                    await asyncio.to_thread(
                        generate_daily_quality_report,
                        settings,
                        report_day,
                        runtime_metrics=runtime_metrics,
                    )
                    last_report_day = report_day
                    runtime_metrics.reset_daily()
                except Exception:
                    logger.exception(
                        "Failed to generate daily quality report for %s", report_day
                    )


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signal_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows ProactorEventLoop relies on KeyboardInterrupt handling.
            pass


async def run_service(settings: Settings) -> None:
    runtime_metrics = RuntimeMetrics()
    storage = StorageManager(settings, runtime_metrics)
    health = HealthState(symbol_count=len(settings.symbols))
    health_runner = None
    tasks: list[asyncio.Task[None]] = []
    stop_event = asyncio.Event()
    ohlcv_builder: OhlcvBuilder | None = None
    taker_delta_builder: TakerDeltaBuilder | None = None
    alerter: TelegramAlerter | None = None
    _install_signal_handlers(stop_event)

    await storage.start()
    try:
        timeout = aiohttp.ClientTimeout(total=20.0, connect=10.0, sock_read=10.0)
        connector = aiohttp.TCPConnector(
            limit=20, ttl_dns_cache=300, enable_cleanup_closed=True
        )
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": "binance-futures-research-collector/1.0"},
        ) as session:
            rest_client = BinanceRestClient(
                session,
                settings.rest_base_url,
                settings.rest_max_attempts,
                settings.rest_concurrency,
                runtime_metrics,
            )
            await rest_client.validate_symbols(settings.symbols)
            alerter = TelegramAlerter(settings, session)
            await alerter.start()
            ohlcv_builder = OhlcvBuilder(storage)
            taker_delta_builder = TakerDeltaBuilder(storage)
            oi_change_tracker = OiChangeTracker(storage)
            funding_event_tracker = FundingEventTracker(
                storage,
                neutral_threshold=settings.funding_neutral_threshold,
                funding_period_ms=int(settings.funding_period_hours * 3_600_000),
            )
            spread_tracker = SpreadStateTracker(storage)
            collectors = (
                TradeCollector(
                    settings,
                    rest_client,
                    storage,
                    health,
                    ohlcv_builder,
                    taker_delta_builder,
                    alerter,
                ),
                LiquidationCollector(settings, storage, health, alerter),
                DepthCollector(
                    settings,
                    rest_client,
                    storage,
                    health,
                    spread_tracker,
                    alerter,
                ),
                FundingCollector(
                    settings,
                    rest_client,
                    storage,
                    health,
                    funding_event_tracker,
                ),
                OpenInterestCollector(
                    settings,
                    rest_client,
                    storage,
                    health,
                    oi_change_tracker,
                ),
                MarkPriceCollector(settings, storage, health, alerter),
                MetadataCollector(settings, rest_client, storage, health),
            )

            health_runner = await start_health_server(
                health, settings.health_host, settings.health_port
            )
            logger.info(
                "Collector started for %d symbols; health endpoint: "
                "http://%s:%d/status",
                len(settings.symbols),
                settings.health_host,
                settings.health_port,
            )

            tasks = [
                asyncio.create_task(collector.run(), name=type(collector).__name__)
                for collector in collectors
            ]
            tasks.append(
                asyncio.create_task(
                    _monitor_loop(
                        storage, health, alerter, runtime_metrics, settings
                    ),
                    name="monitor-loop",
                )
            )
            stop_waiter = asyncio.create_task(
                stop_event.wait(), name="shutdown-waiter"
            )
            supervised_tasks = [*tasks, *storage.background_tasks, stop_waiter]
            done, _ = await asyncio.wait(
                supervised_tasks, return_when=asyncio.FIRST_COMPLETED
            )

            if stop_waiter not in done:
                for task in done:
                    exception = task.exception()
                    if exception is not None:
                        alerter.notify_unexpected_exception(
                            task.get_name(), str(exception)
                        )
                        raise exception
                    alerter.notify_unexpected_exception(
                        task.get_name(), "background task stopped unexpectedly"
                    )
                    raise RuntimeError(
                        f"Background task stopped: {task.get_name()}"
                    )
            stop_waiter.cancel()
    finally:
        logger.info("Shutting down collector")
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if health_runner is not None:
            await health_runner.cleanup()
        if alerter is not None:
            await alerter.close()
        if ohlcv_builder is not None:
            await ohlcv_builder.flush()
            await storage.wait_for_dataset("ohlcv_1m")
        if taker_delta_builder is not None:
            await taker_delta_builder.flush()
            await storage.wait_for_dataset("taker_delta_1m")
        await storage.close()
        logger.info("Collector stopped cleanly")


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings)
    try:
        asyncio.run(run_service(settings))
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception:
        logger.exception("Collector terminated with an error")
        raise


if __name__ == "__main__":
    main()
