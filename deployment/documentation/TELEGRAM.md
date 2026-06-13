# Telegram Alerting

Immediate Telegram notifications when collector health degrades. Alerts are **non-blocking**, **async**, and **failure tolerant** — the collector continues normally if Telegram is down.

## Setup

### 1. Create a Telegram bot

1. Open Telegram and message [@BotFather](https://t.me/BotFather)
2. Send `/newbot` and follow the prompts
3. Copy the **bot token** (format: `123456789:ABC...`)

### 2. Get your chat ID

**Direct message to yourself (via bot):**

1. Send any message to your new bot
2. Open in a browser (replace `TOKEN`):

   ```text
   https://api.telegram.org/botTOKEN/getUpdates
   ```

3. Find `"chat":{"id": ...}` — that number is your `TELEGRAM_CHAT_ID`

**Group chat:** add the bot to the group, send a message, then read `getUpdates` the same way. Group IDs are negative numbers.

### 3. Configure the collector

Edit the production env file:

```bash
sudo nano /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.env
```

Set:

```ini
TELEGRAM_BOT_TOKEN=123456789:YOUR_BOT_TOKEN
TELEGRAM_CHAT_ID=YOUR_CHAT_ID
```

Restart:

```bash
sudo systemctl restart binance-futures-collector
```

You should receive an `[INFO] collector_restarted` message within a few seconds.

Leave `TELEGRAM_BOT_TOKEN` or `TELEGRAM_CHAT_ID` blank to disable all Telegram alerts.

## Alert events

| Event | Severity | Trigger |
|---|---|---|
| `collector_restarted` | INFO | Process start |
| `collector_stopped` | WARNING | Clean shutdown (SIGTERM) |
| `websocket_disconnected` | WARNING | WebSocket connection lost |
| `repeated_reconnects` | CRITICAL | ≥3 reconnects on one stream within 15 min |
| `funding_updates_missing` | WARNING | No funding poll success within threshold |
| `oi_updates_missing` | WARNING | No OI poll success within threshold |
| `disk_usage_high` | WARNING / CRITICAL | Free disk below threshold |
| `backup_failure` | CRITICAL | Weekly GCS backup failed |
| `backup_success` | INFO | Weekly GCS backup succeeded (via `telegram-notify.sh`) |
| `health_check_failed` | CRITICAL | systemd health timer check failed |
| `data_integrity_failure` | CRITICAL | Parquet compaction error |
| `unexpected_exception` | CRITICAL | Collector or WebSocket task crash |
| `daily_summary` | INFO | Once per day (default 00:05 UTC) |

## Severity levels

| Level | Meaning |
|---|---|
| **INFO** | Normal operational events (start, daily summary) |
| **WARNING** | Degraded but self-healing (disconnect, stale polls, shutdown) |
| **CRITICAL** | Requires attention (repeated reconnects, disk, backup, integrity) |

## Rate limiting

Each event type is limited to **one Telegram message per 15 minutes** (configurable via `TELEGRAM_RATE_LIMIT_SECONDS=900`).

The daily summary bypasses rate limiting but sends at most once per UTC day.

## Daily summary

Sent once per day at `TELEGRAM_DAILY_SUMMARY_HOUR_UTC`:`TELEGRAM_DAILY_SUMMARY_MINUTE_UTC` (default **00:05 UTC**).

Includes:

- Trade count
- Liquidation count
- Disk used / total
- Uptime
- WebSocket disconnect and reconnect counts
- Last backup status

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | unset | Bot token from BotFather |
| `TELEGRAM_CHAT_ID` | unset | Target chat or group ID |
| `TELEGRAM_RATE_LIMIT_SECONDS` | `900` | Min seconds between same event type |
| `TELEGRAM_DISK_FREE_GB_WARN` | `10` | Free-disk warning threshold (GB) |
| `TELEGRAM_REPEATED_RECONNECT_THRESHOLD` | `3` | Reconnects before CRITICAL alert |
| `TELEGRAM_REPEATED_RECONNECT_WINDOW_SECONDS` | `900` | Window for reconnect counting |
| `TELEGRAM_FUNDING_STALE_MINUTES` | `15` | Funding staleness threshold |
| `TELEGRAM_OI_STALE_MINUTES` | `5` | OI staleness threshold |
| `TELEGRAM_DAILY_SUMMARY_HOUR_UTC` | `0` | Daily summary hour (UTC) |
| `TELEGRAM_DAILY_SUMMARY_MINUTE_UTC` | `5` | Daily summary minute (UTC) |
| `BACKUP_STATUS_FILE` | `/var/lib/.../backup_status.json` | Backup status for alerts |

## Backup alerts

When `BACKUP_ENABLED=1`, `collector.cloud_backup` (via `deployment/scripts/backup.sh`):

1. Writes `backup_status.json` on success or failure
2. Sends an immediate Telegram alert via `telegram-notify.sh`:
   - **INFO** on success (`backup_success`)
   - **CRITICAL** on failure (`backup_failure`)
3. The in-process monitor also picks up failed status on its next 60 s cycle (`backup_failure` event)

## Architecture

```text
Collectors / WebSocketManager
        │ notify() — non-blocking queue put
        ▼
TelegramAlerter worker (async aiohttp)
        │
        ▼
Telegram Bot API
```

- Alerts never block ingestion hot paths
- Failed Telegram HTTP calls are logged and dropped
- Queue overflow drops alerts rather than blocking the collector
- Restart sends `collector_restarted`; shutdown sends `collector_stopped`

## Manual test

```bash
# From the VM env file
source /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.env
/opt/binance-futures-collector/deployment/scripts/telegram-notify.sh INFO "Test alert" "Manual verification"
```

## Troubleshooting

| Issue | Fix |
|---|---|
| No messages | Verify token and chat ID; message the bot first |
| `403 Forbidden` | User has not started the bot chat |
| Alerts stop entirely | Check collector logs for `Telegram alert delivery failed` |
| Too many alerts | Increase `TELEGRAM_RATE_LIMIT_SECONDS` |
| No daily summary | Confirm UTC hour/minute; collector must be running at that time |

See also [MONITORING.md](MONITORING.md) for health endpoint and systemd integration.
