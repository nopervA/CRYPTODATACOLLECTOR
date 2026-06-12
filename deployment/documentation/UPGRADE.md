# Upgrade Guide

Upgrade a running production collector on Debian 12 with minimal downtime.

## Standard upgrade (recommended)

```bash
cd /opt/binance-futures-collector
sudo systemctl stop binance-futures-collector
sudo git fetch --tags
sudo git checkout vX.Y.Z          # or: sudo git pull origin main
sudo .venv/bin/pip install -r requirements.txt
sudo chmod +x deployment/scripts/*.sh
sudo systemctl start binance-futures-collector
```

Stopping first ensures a clean shutdown: queues drain, OHLCV/taker-delta buffers flush, and open Parquet segments finalize.

## Zero-downtime note

This collector maintains long-lived WebSocket connections and in-memory dedup caches. A rolling upgrade without stop is not supported. Planned maintenance windows of 1–2 minutes are normal.

## Verify after upgrade

```bash
sudo systemctl status binance-futures-collector
curl -s http://127.0.0.1:8080/status | python3 -m json.tool
sudo /opt/binance-futures-collector/deployment/scripts/health-check.sh
sudo journalctl -u binance-futures-collector --since "5 min ago"
```

Confirm:

- Service is `active (running)`
- `/status` shows increasing counters
- Health check exits 0
- No repeated restart loops in journal (`Restart=always` should not fire continuously)

## Configuration-only changes

When only environment variables change:

```bash
sudo nano /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.env
sudo systemctl restart binance-futures-collector
```

Or use a systemd drop-in:

```bash
sudo systemctl edit binance-futures-collector
sudo systemctl restart binance-futures-collector
```

## Schema or dataset additions

New collector versions may add derived datasets without changing existing Parquet schemas. After upgrade:

1. New dataset directories appear under `COLLECTOR_DATA_DIR` automatically on first write.
2. Startup recovery scans all registered datasets, including new ones.
3. No migration script is required for backward-compatible releases.

Breaking schema changes (rare) will be called out in release notes with explicit migration steps.

## Rollback

```bash
cd /opt/binance-futures-collector
sudo systemctl stop binance-futures-collector
sudo git checkout PREVIOUS_TAG
sudo .venv/bin/pip install -r requirements.txt
sudo systemctl start binance-futures-collector
```

Existing Parquet data remains compatible unless the release notes say otherwise. Do not delete data during rollback.

## Dependency updates

`requirements.txt` pins major versions. After `pip install`, run the test suite on a staging VM before production:

```bash
cd /opt/binance-futures-collector
.venv/bin/pytest
```

## systemd unit changes

When upgrading across releases that modify unit files:

```bash
sudo systemctl daemon-reload
sudo systemctl enable /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.service
sudo systemctl restart binance-futures-collector
```

Timer units (health, backup) are re-registered by `ensure-runtime.sh` on next service start.
