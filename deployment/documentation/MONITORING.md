# Monitoring Guide

Integrate health checks, systemd monitoring, Google Cloud alerting, and **Telegram notifications** for the production collector.

## Telegram alerts (recommended)

For immediate mobile alerts on disconnects, stale polls, disk pressure, backup failures, and daily summaries, configure Telegram:

```ini
TELEGRAM_BOT_TOKEN=your-bot-token
TELEGRAM_CHAT_ID=your-chat-id
```

Full setup: [TELEGRAM.md](TELEGRAM.md)

## Health endpoint

| Property | Value |
|---|---|
| URL | `http://127.0.0.1:8080/status` |
| Method | `GET` |
| Format | JSON |

Example response:

```json
{
  "uptime_hours": 12.34,
  "symbols": 25,
  "trades_received": 1234567,
  "liquidations_received": 1234,
  "depth_snapshots_received": 987654,
  "mark_price_updates_received": 987654,
  "top_of_book_updates_received": 987654,
  "last_funding_update": "2026-06-11T12:00:00.000000Z",
  "last_oi_update": "2026-06-11T12:01:00.000000Z",
  "last_metadata_update": "2026-06-11T00:00:05.000000Z",
  "last_trade_update": "2026-06-11T12:01:30.000000Z",
  "last_depth50_update": "2026-06-11T12:01:31.000000Z",
  "websocket_reconnects": 2,
  "integrity_error_count": 0
}
```

Quick probe:

```bash
curl -sf http://127.0.0.1:8080/status | python3 -m json.tool
```

`last_metadata_update` is restored on startup by reading the newest `metadata/date=*/metadata.parquet` timestamp. After a restart, it is populated immediately when today's (or any prior) metadata file exists — you do not need to wait for the next daily collection cycle.

## Automated health verification

A systemd timer runs `deployment/scripts/health-check.sh` every minute (enabled automatically on first service start).

| Check | Condition |
|---|---|
| HTTP reachability | `/status` returns 200 |
| Schema | Required JSON fields present |
| Symbol count | `symbols > 0` |
| OI freshness | `last_oi_update` within `HEALTH_MAX_OI_AGE_MINUTES` (default 5) when uptime ≥ threshold |
| Funding freshness | `last_funding_update` within `HEALTH_MAX_FUNDING_AGE_MINUTES` (default 15) when uptime ≥ threshold |
| Trade freshness | `last_trade_update` within `HEALTH_MAX_TRADE_AGE_MINUTES` (default 2) when uptime ≥ threshold |
| Depth freshness | `last_depth50_update` within `HEALTH_MAX_DEPTH_AGE_MINUTES` (default 5) when uptime ≥ threshold |
| Integrity | `integrity_error_count == 0` when uptime ≥ 15 minutes |

Timer status:

```bash
sudo systemctl status binance-futures-collector-health.timer
sudo journalctl -u binance-futures-collector-health.service --since "10 min ago"
```

Manual run:

```bash
sudo /opt/binance-futures-collector/deployment/scripts/health-check.sh
echo $?   # 0 = healthy
```

Configure thresholds in `deployment/systemd/binance-futures-collector.env`:

```ini
HEALTH_MAX_OI_AGE_MINUTES=5
HEALTH_MAX_FUNDING_AGE_MINUTES=15
HEALTH_MAX_TRADE_AGE_MINUTES=2
HEALTH_MAX_DEPTH_AGE_MINUTES=5
```

Failed checks send **CRITICAL** Telegram via `telegram-notify.sh` (`health_check_failed`).

## systemd service monitoring

Core signals:

```bash
sudo systemctl is-active binance-futures-collector
sudo systemctl show binance-futures-collector -p ActiveState,SubState,NRestarts,ExecMainStatus
```

| Signal | Healthy | Unhealthy |
|---|---|---|
| `ActiveState` | `active` | `failed`, `inactive` |
| `NRestarts` | stable over hours | climbing rapidly |
| Journal errors | occasional reconnects | repeated tracebacks |

Live log tail:

```bash
sudo journalctl -u binance-futures-collector -f
```

## Google Cloud Monitoring integration

### Uptime check (external)

If you expose health via an authenticated reverse proxy or internal load balancer, create an HTTPS uptime check against `/status` and alert on non-200.

For localhost-only binding (default), use **Ops Agent** or **Cloud Logging** with log-based metrics instead of external HTTP checks.

### Install Ops Agent (recommended)

```bash
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
```

Collect systemd unit state via journal logs already emitted to Cloud Logging (`SyslogIdentifier=binance-futures-collector`).

### Log-based alert: service failure

Create a log-based metric in Cloud Logging:

- **Filter:** `resource.type="gce_instance" AND jsonPayload.syslog_identifier="binance-futures-collector" AND (textPayload:"terminated with an error" OR textPayload:"Restarting")`
- **Alert:** notification when rate exceeds threshold in 5 minutes

### Log-based alert: disk pressure

The collector logs `free disk` every minute. Alert when:

```text
textPayload:"free disk" AND textPayload:"GB"
```

Parse in log-based metric or monitor with a simple cron on the VM:

```bash
FREE=$(df --output=avail /var/lib/binance-futures-collector | tail -1)
if (( FREE < 10*1024*1024 )); then echo "DISK LOW: ${FREE} KB free"; fi
```

### Health timer → Cloud Logging

Failed health checks appear in journal:

```bash
sudo journalctl -u binance-futures-collector-health.service -p err --since today
```

Create an alert policy on:

```text
resource.type="gce_instance"
log_name="projects/PROJECT_ID/logs/journald"
textPayload:"health-check:"
severity>=ERROR
```

## External monitoring tools

### Uptime Kuma / Grafana

Run a sidecar probe on the VM (SSH or local agent):

```bash
*/1 * * * * /opt/binance-futures-collector/deployment/scripts/health-check.sh || logger -t collector-alert "health check failed"
```

### Prometheus node_exporter

Export custom textfile metric from health check:

```bash
# /etc/cron.d/collector-health-prom
* * * * * root /opt/binance-futures-collector/deployment/scripts/health-check.sh \
  && echo 'collector_health 1' > /var/lib/node_exporter/collector_health.prom \
  || echo 'collector_health 0' > /var/lib/node_exporter/collector_health.prom
```

## Monitoring checklist

| Item | Frequency | Tool |
|---|---|---|
| Service active | Continuous | systemd / Cloud Logging |
| `/status` freshness | 1 min | health timer |
| Disk free space | 1 min | collector queue monitor log |
| Backup success | Weekly (Sun 03:00 UTC) | backup timer journal |
| Counter growth | Hourly | `/status` `trades_received` trend |
| Restart count | Daily | `systemctl show NRestarts` |

## Alert severity guide

| Severity | Condition | Response |
|---|---|---|
| **Critical** | Service down > 5 min | Restart; check journal and disk |
| **Critical** | Data disk < 5 GB free | Expand disk or prune; pause nonessential workloads |
| **Warning** | Health check failing | Inspect API connectivity and symbol config |
| **Warning** | `NRestarts` increased | Review crash traceback |
| **Info** | WebSocket reconnect | Normal; verify recovery in logs |

## Security note

Keep `HEALTH_HOST=127.0.0.1` in production. External monitoring should use SSH tunnels, VPC-internal probes, or log/metric export — not a publicly exposed `/status` endpoint.
