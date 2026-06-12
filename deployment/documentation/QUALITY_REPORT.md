# Daily Data Quality Reports

Automated UTC daily reports detect missing data, collector failures, reconnect storms, activity drops, and partition corruption.

## Output

Each UTC day produces:

```text
reports/YYYY-MM-DD.json
reports/YYYY-MM-DD.md
```

## Automatic generation

The collector monitor loop generates the previous day's report at **00:15 UTC** by default.

Configure:

```ini
COLLECTOR_REPORT_DIR=reports
QUALITY_REPORT_HOUR_UTC=0
QUALITY_REPORT_MINUTE_UTC=15
```

## Manual generation

```bash
python -m collector.daily_quality_report --day 2026-06-11
# or yesterday (default):
python -m collector.daily_quality_report
```

## Metrics tracked

**Per symbol:** trade, liquidation, depth, mark price, OI, funding, funding_event counts.

**Collector:** uptime, WebSocket disconnect/reconnect counts, REST failures, storage failures, queue peak sizes.

**Integrity:** missing mark-price minutes, OI/funding interval gaps, large timestamp gaps (>5 min), empty partitions.

**Storage:** daily bytes per dataset, total bytes, growth trend, projected monthly/90-day usage, disk usage percent.

## Alert thresholds

| Threshold | Default |
|---|---|
| Missing data | > 5 mark-price minutes |
| Funding gaps | below 90% of expected daily polls |
| OI gaps | below 90% of expected daily polls |
| Reconnect storm | > 20 reconnects/day |
| Disk usage | > 80% |

## Overhead

| Resource | Estimate (25 symbols) |
|---|---|
| Storage per report | ~50–200 KB (JSON + Markdown) |
| CPU per report | ~2–10 s (metadata scans, optional timestamp reads) |
| RAM | negligible (streaming Parquet reads) |

See [reports/example-2026-06-11.json](../reports/example-2026-06-11.json) for a sample payload.
