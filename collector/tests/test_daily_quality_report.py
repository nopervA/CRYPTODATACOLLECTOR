import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from collector.config import Settings
from collector.daily_quality_report import AlertThresholds, generate_daily_quality_report
from collector.runtime_metrics import RuntimeMetrics


def _write_partition(
    base: Path,
    dataset_dir: str,
    filename: str,
    symbol: str,
    day: str,
    schema: pa.Schema,
    rows: list[dict],
) -> None:
    partition = base / dataset_dir / f"symbol={symbol}" / f"date={day}"
    partition.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, partition / filename, compression="snappy")


def test_generate_daily_quality_report_writes_json_and_markdown(tmp_path) -> None:
    day = "2026-06-11"
    data_dir = tmp_path / "data"
    report_dir = tmp_path / "reports"
    settings = Settings(
        symbols=("BTCUSDT",),
        data_dir=data_dir,
        report_dir=report_dir,
    )
    ts = int(datetime(2026, 6, 11, 12, 0, tzinfo=UTC).timestamp() * 1000)

    _write_partition(
        data_dir,
        "trades",
        "trades.parquet",
        "BTCUSDT",
        day,
        pa.schema([pa.field("timestamp", pa.int64())]),
        [{"timestamp": ts}],
    )
    _write_partition(
        data_dir,
        "mark_price",
        "mark_price.parquet",
        "BTCUSDT",
        day,
        pa.schema([pa.field("event_time", pa.int64())]),
        [{"event_time": ts}],
    )
    _write_partition(
        data_dir,
        "oi",
        "oi.parquet",
        "BTCUSDT",
        day,
        pa.schema([pa.field("timestamp", pa.int64())]),
        [{"timestamp": ts}],
    )
    _write_partition(
        data_dir,
        "funding",
        "funding.parquet",
        "BTCUSDT",
        day,
        pa.schema([pa.field("timestamp", pa.int64())]),
        [{"timestamp": ts}],
    )
    _write_partition(
        data_dir,
        "funding_event",
        "funding_event.parquet",
        "BTCUSDT",
        day,
        pa.schema([pa.field("timestamp", pa.int64())]),
        [{"timestamp": ts}],
    )

    metrics = RuntimeMetrics()
    metrics.record_queue_sizes({"trades": 10})
    metrics.record_rest_failure()

    report = generate_daily_quality_report(
        settings,
        day,
        runtime_metrics=metrics,
        collector_snapshot={
            **metrics.snapshot(
                websocket_disconnects=2,
                websocket_reconnects=3,
            ),
        },
        thresholds=AlertThresholds(max_reconnects_per_day=20),
    )

    json_path = report_dir / f"{day}.json"
    md_path = report_dir / f"{day}.md"
    assert json_path.exists()
    assert md_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["report_day"] == day
    assert payload["per_symbol"]["BTCUSDT"]["trade_count"] == 1
    assert payload["collector_metrics"]["rest_failures"] == 1
    assert payload["status"] in {"ok", "warning", "critical"}
    assert "Daily Data Quality Report" in md_path.read_text(encoding="utf-8")
    assert report["per_symbol"]["BTCUSDT"]["trade_count"] == 1


def test_alert_threshold_reconnect_storm() -> None:
    from collector.daily_quality_report import _evaluate_alerts

    alerts = _evaluate_alerts(
        thresholds=AlertThresholds(max_reconnects_per_day=20),
        collector_metrics={"storage_failures": 0, "rest_failures": 0},
        integrity={
            "missing_minutes": [],
            "missing_oi_intervals": {},
            "missing_funding_intervals": {},
            "large_timestamp_gaps_ms": {},
            "unexpected_empty_partitions": [],
        },
        disk={"usage_percent": 50.0},
        ws_reconnects=25,
    )
    assert any(alert["type"] == "reconnect_storm" for alert in alerts)
