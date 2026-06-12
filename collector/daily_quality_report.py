from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from collector.config import Settings
from collector.runtime_metrics import RuntimeMetrics
from collector.storage import DATASET_LAYOUT

logger = logging.getLogger(__name__)

SYMBOL_DATASETS = (
    "trades",
    "liquidations",
    "depth50",
    "mark_price",
    "open_interest",
    "funding",
    "funding_event",
)

COUNT_KEYS = {
    "trades": "trade_count",
    "liquidations": "liquidation_count",
    "depth50": "depth_count",
    "mark_price": "mark_price_count",
    "open_interest": "oi_count",
    "funding": "funding_count",
    "funding_event": "funding_event_count",
}

TIMESTAMP_FIELDS: dict[str, str] = {
    "trades": "timestamp",
    "liquidations": "timestamp",
    "depth50": "timestamp",
    "mark_price": "event_time",
    "open_interest": "timestamp",
    "funding": "timestamp",
    "funding_event": "timestamp",
}

LARGE_GAP_MS = 5 * 60 * 1000


@dataclass(frozen=True, slots=True)
class AlertThresholds:
    missing_data_minutes: int = 5
    max_reconnects_per_day: int = 20
    disk_usage_percent: float = 80.0


def generate_daily_quality_report(
    settings: Settings,
    report_day: str,
    *,
    runtime_metrics: RuntimeMetrics | None = None,
    collector_snapshot: dict[str, Any] | None = None,
    thresholds: AlertThresholds | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or AlertThresholds()
    symbols = settings.symbols
    data_dir = settings.data_dir
    report_dir = settings.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    ws_disconnects = 0
    ws_reconnects = 0
    if collector_snapshot is not None:
        ws_disconnects = int(collector_snapshot.get("websocket_disconnects", 0))
        ws_reconnects = int(collector_snapshot.get("websocket_reconnects", 0))

    if runtime_metrics is not None:
        collector_metrics = runtime_metrics.snapshot(
            websocket_disconnects=ws_disconnects,
            websocket_reconnects=ws_reconnects,
        )
    elif collector_snapshot is not None:
        collector_metrics = dict(collector_snapshot)
    else:
        collector_metrics = {
            "uptime_seconds": 0.0,
            "websocket_disconnects": ws_disconnects,
            "websocket_reconnects": ws_reconnects,
            "rest_failures": 0,
            "storage_failures": 0,
            "queue_peak": {},
        }

    per_symbol: dict[str, dict[str, int]] = {}
    integrity: dict[str, Any] = {
        "missing_minutes": [],
        "missing_oi_intervals": {},
        "missing_funding_intervals": {},
        "large_timestamp_gaps_ms": {},
        "unexpected_empty_partitions": [],
    }

    for symbol in symbols:
        counts: dict[str, int] = {}
        for dataset in SYMBOL_DATASETS:
            count = _row_count_for_partition(data_dir, dataset, symbol, report_day)
            counts[COUNT_KEYS[dataset]] = count
            if count == 0:
                integrity["unexpected_empty_partitions"].append(
                    f"{dataset}/symbol={symbol}/date={report_day}"
                )
        per_symbol[symbol] = counts
        _check_symbol_integrity(data_dir, symbol, report_day, counts, integrity, settings)

    storage_by_dataset: dict[str, int] = {
        dataset: _dataset_bytes_for_day(data_dir, dataset, report_day, symbols)
        for dataset in DATASET_LAYOUT
    }
    total_bytes = sum(storage_by_dataset.values())
    growth = _growth_trend(report_dir, report_day, total_bytes)
    disk = _disk_usage(data_dir)

    alerts = _evaluate_alerts(
        thresholds=thresholds,
        collector_metrics=collector_metrics,
        integrity=integrity,
        disk=disk,
        ws_reconnects=ws_reconnects,
    )

    report = {
        "report_day": report_day,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "symbols": list(symbols),
        "per_symbol": per_symbol,
        "collector_metrics": collector_metrics,
        "integrity_checks": integrity,
        "storage_metrics": {
            "daily_bytes_by_dataset": storage_by_dataset,
            "total_bytes": total_bytes,
            "growth_trend": growth,
            "projected_monthly_bytes": int(growth["avg_daily_bytes"] * 30),
            "projected_90_day_bytes": int(growth["avg_daily_bytes"] * 90),
            "disk_usage_percent": disk["usage_percent"],
            "disk_free_bytes": disk["free_bytes"],
            "disk_total_bytes": disk["total_bytes"],
        },
        "alerts": alerts,
        "status": _overall_status(alerts),
    }

    json_path = report_dir / f"{report_day}.json"
    md_path = report_dir / f"{report_day}.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    logger.info("Wrote quality report: %s and %s", json_path, md_path)
    return report


def _partition_dir(data_dir: Path, dataset: str, symbol: str, day: str) -> Path:
    directory_name, _ = DATASET_LAYOUT[dataset]
    return data_dir / directory_name / f"symbol={symbol}" / f"date={day}"


def _row_count_for_partition(
    data_dir: Path, dataset: str, symbol: str, day: str
) -> int:
    partition = _partition_dir(data_dir, dataset, symbol, day)
    if not partition.exists():
        return 0
    total = 0
    for path in sorted(partition.glob("*.parquet")):
        if path.name.startswith("."):
            continue
        try:
            total += pq.ParquetFile(path).metadata.num_rows
        except OSError as exc:
            logger.warning("Could not read %s: %s", path, exc)
    return total


def _read_timestamps(data_dir: Path, dataset: str, symbol: str, day: str) -> list[int]:
    partition = _partition_dir(data_dir, dataset, symbol, day)
    field = TIMESTAMP_FIELDS[dataset]
    timestamps: list[int] = []
    if not partition.exists():
        return timestamps
    for path in sorted(partition.glob("*.parquet")):
        if path.name.startswith("."):
            continue
        try:
            table = pq.read_table(path, columns=[field])
            timestamps.extend(int(value) for value in table[field].to_pylist())
        except (OSError, KeyError, ValueError) as exc:
            logger.warning("Could not read timestamps from %s: %s", path, exc)
    timestamps.sort()
    return timestamps


def _expected_intervals(seconds_per_interval: float) -> int:
    return max(1, int(86_400 / seconds_per_interval * 0.90))


def _check_symbol_integrity(
    data_dir: Path,
    symbol: str,
    day: str,
    counts: dict[str, int],
    integrity: dict[str, Any],
    settings: Settings,
) -> None:
    oi_count = counts.get("oi_count", 0)
    expected_oi = _expected_intervals(settings.oi_interval_seconds)
    if 0 < oi_count < expected_oi:
        integrity["missing_oi_intervals"][symbol] = {
            "expected_min": expected_oi,
            "actual": oi_count,
            "missing": expected_oi - oi_count,
        }

    funding_count = counts.get("funding_count", 0)
    expected_funding = _expected_intervals(settings.funding_interval_seconds)
    if 0 < funding_count < expected_funding:
        integrity["missing_funding_intervals"][symbol] = {
            "expected_min": expected_funding,
            "actual": funding_count,
            "missing": expected_funding - funding_count,
        }

    mark_timestamps = _read_timestamps(data_dir, "mark_price", symbol, day)
    if mark_timestamps:
        missing_minutes = _missing_minute_count(mark_timestamps, day)
        if missing_minutes > 0:
            integrity["missing_minutes"].append(
                {"symbol": symbol, "missing_minutes": missing_minutes}
            )
        gaps = _large_gaps(mark_timestamps, LARGE_GAP_MS)
        if gaps:
            integrity["large_timestamp_gaps_ms"][symbol] = gaps[:20]


def _missing_minute_count(timestamps_ms: list[int], day: str) -> int:
    day_start = datetime.fromisoformat(f"{day}T00:00:00+00:00")
    day_end = day_start + timedelta(days=1)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)
    minutes_seen = {
        (ts - start_ms) // 60_000
        for ts in timestamps_ms
        if start_ms <= ts < end_ms
    }
    expected_minutes = 24 * 60
    return max(0, expected_minutes - len(minutes_seen))


def _large_gaps(timestamps_ms: list[int], threshold_ms: int) -> list[dict[str, int]]:
    gaps: list[dict[str, int]] = []
    previous = timestamps_ms[0]
    for current in timestamps_ms[1:]:
        delta = current - previous
        if delta > threshold_ms:
            gaps.append({"from_ms": previous, "to_ms": current, "gap_ms": delta})
        previous = current
    return gaps


def _dataset_bytes_for_day(
    data_dir: Path, dataset: str, day: str, symbols: tuple[str, ...]
) -> int:
    directory_name, _ = DATASET_LAYOUT[dataset]
    total = 0
    if dataset == "metadata":
        partition = data_dir / directory_name / f"date={day}"
        if partition.exists():
            for path in partition.glob("*.parquet"):
                if not path.name.startswith("."):
                    total += path.stat().st_size
        return total
    for symbol in symbols:
        partition = data_dir / directory_name / f"symbol={symbol}" / f"date={day}"
        if not partition.exists():
            continue
        for path in partition.glob("*.parquet"):
            if not path.name.startswith("."):
                total += path.stat().st_size
    return total


def _growth_trend(report_dir: Path, report_day: str, total_bytes: int) -> dict[str, Any]:
    try:
        report_date = date.fromisoformat(report_day)
    except ValueError:
        return {
            "avg_daily_bytes": total_bytes,
            "prior_days_included": 0,
            "delta_from_prior_day_bytes": None,
        }
    prior_days: list[int] = []
    for offset in range(1, 8):
        prior = (report_date - timedelta(days=offset)).isoformat()
        prior_path = report_dir / f"{prior}.json"
        if not prior_path.exists():
            continue
        try:
            payload = json.loads(prior_path.read_text(encoding="utf-8"))
            prior_days.append(int(payload["storage_metrics"]["total_bytes"]))
        except (OSError, KeyError, TypeError, ValueError):
            continue
    avg_daily = int(sum(prior_days + [total_bytes]) / (len(prior_days) + 1))
    delta = total_bytes - prior_days[0] if prior_days else None
    return {
        "avg_daily_bytes": avg_daily,
        "prior_days_included": len(prior_days),
        "delta_from_prior_day_bytes": delta,
    }


def _disk_usage(data_dir: Path) -> dict[str, float | int]:
    try:
        import shutil

        usage = shutil.disk_usage(data_dir)
    except OSError:
        return {"usage_percent": 0.0, "free_bytes": 0, "total_bytes": 0}
    used = usage.total - usage.free
    percent = (used / usage.total * 100.0) if usage.total else 0.0
    return {
        "usage_percent": round(percent, 2),
        "free_bytes": usage.free,
        "total_bytes": usage.total,
    }


def _evaluate_alerts(
    *,
    thresholds: AlertThresholds,
    collector_metrics: dict[str, Any],
    integrity: dict[str, Any],
    disk: dict[str, Any],
    ws_reconnects: int,
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    if ws_reconnects > thresholds.max_reconnects_per_day:
        alerts.append(
            {
                "severity": "CRITICAL",
                "type": "reconnect_storm",
                "message": (
                    f"WebSocket reconnects ({ws_reconnects}) exceed daily threshold "
                    f"({thresholds.max_reconnects_per_day})"
                ),
            }
        )
    if disk["usage_percent"] > thresholds.disk_usage_percent:
        alerts.append(
            {
                "severity": "CRITICAL",
                "type": "disk_usage_high",
                "message": (
                    f"Disk usage {disk['usage_percent']:.1f}% exceeds "
                    f"{thresholds.disk_usage_percent:.0f}% threshold"
                ),
            }
        )
    for item in integrity["missing_minutes"]:
        if item["missing_minutes"] > thresholds.missing_data_minutes:
            alerts.append(
                {
                    "severity": "WARNING",
                    "type": "missing_minutes",
                    "message": (
                        f"{item['symbol']} missing {item['missing_minutes']} "
                        "mark-price minutes"
                    ),
                }
            )
    if integrity["missing_oi_intervals"]:
        alerts.append(
            {
                "severity": "WARNING",
                "type": "oi_missing",
                "message": "OI gaps detected for "
                + ", ".join(sorted(integrity["missing_oi_intervals"])),
            }
        )
    if integrity["missing_funding_intervals"]:
        alerts.append(
            {
                "severity": "WARNING",
                "type": "funding_missing",
                "message": "Funding gaps detected for "
                + ", ".join(sorted(integrity["missing_funding_intervals"])),
            }
        )
    if integrity["unexpected_empty_partitions"]:
        alerts.append(
            {
                "severity": "WARNING",
                "type": "empty_partitions",
                "message": (
                    f"{len(integrity['unexpected_empty_partitions'])} empty partition(s)"
                ),
            }
        )
    if collector_metrics.get("storage_failures", 0) > 0:
        alerts.append(
            {
                "severity": "CRITICAL",
                "type": "storage_failures",
                "message": (
                    f"{collector_metrics['storage_failures']} storage failure(s) recorded"
                ),
            }
        )
    if collector_metrics.get("rest_failures", 0) > 0:
        alerts.append(
            {
                "severity": "WARNING",
                "type": "rest_failures",
                "message": (
                    f"{collector_metrics['rest_failures']} REST failure(s) recorded"
                ),
            }
        )
    return alerts


def _overall_status(alerts: list[dict[str, str]]) -> str:
    if any(alert["severity"] == "CRITICAL" for alert in alerts):
        return "critical"
    if alerts:
        return "warning"
    return "ok"


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Daily Data Quality Report — {report['report_day']}",
        "",
        f"Generated: {report['generated_at']}",
        f"Status: **{report['status'].upper()}**",
        "",
        "## Collector Metrics",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    metrics = report["collector_metrics"]
    lines.extend(
        [
            f"| Uptime (seconds) | {metrics.get('uptime_seconds', 0)} |",
            f"| WebSocket disconnects | {metrics.get('websocket_disconnects', 0)} |",
            f"| WebSocket reconnects | {metrics.get('websocket_reconnects', 0)} |",
            f"| REST failures | {metrics.get('rest_failures', 0)} |",
            f"| Storage failures | {metrics.get('storage_failures', 0)} |",
        ]
    )
    peak = metrics.get("queue_peak", {})
    if peak:
        lines.append(f"| Queue peak (max) | {max(peak.values())} |")
    lines.extend(["", "## Per-Symbol Counts", ""])
    lines.append(
        "| Symbol | Trades | Liq | Depth | Mark | OI | Funding | Funding Event |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for symbol, counts in report["per_symbol"].items():
        lines.append(
            f"| {symbol} | {counts.get('trade_count', 0)} | "
            f"{counts.get('liquidation_count', 0)} | "
            f"{counts.get('depth_count', 0)} | "
            f"{counts.get('mark_price_count', 0)} | "
            f"{counts.get('oi_count', 0)} | "
            f"{counts.get('funding_count', 0)} | "
            f"{counts.get('funding_event_count', 0)} |"
        )
    integrity = report["integrity_checks"]
    lines.extend(
        [
            "",
            "## Integrity Checks",
            "",
            f"- Missing minute buckets: {len(integrity['missing_minutes'])}",
            f"- OI gap symbols: {len(integrity['missing_oi_intervals'])}",
            f"- Funding gap symbols: {len(integrity['missing_funding_intervals'])}",
            f"- Large timestamp gap symbols: {len(integrity['large_timestamp_gaps_ms'])}",
            f"- Unexpected empty partitions: {len(integrity['unexpected_empty_partitions'])}",
            "",
            "## Storage Metrics",
            "",
        ]
    )
    storage = report["storage_metrics"]
    lines.extend(
        [
            f"- Total bytes: {storage['total_bytes']:,}",
            f"- Projected monthly: {storage['projected_monthly_bytes']:,} bytes",
            f"- Projected 90-day: {storage['projected_90_day_bytes']:,} bytes",
            f"- Disk usage: {storage['disk_usage_percent']:.1f}%",
            "",
            "## Alerts",
            "",
        ]
    )
    if report["alerts"]:
        for alert in report["alerts"]:
            lines.append(
                f"- **[{alert['severity']}]** {alert['type']}: {alert['message']}"
            )
    else:
        lines.append("- No alerts triggered.")
    return "\n".join(lines) + "\n"


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Generate daily data quality report")
    parser.add_argument(
        "--day",
        help="UTC report day (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()
    settings = Settings.from_env()
    report_day = args.day or (
        datetime.now(UTC).date() - timedelta(days=1)
    ).isoformat()
    generate_daily_quality_report(settings, report_day)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _cli()
