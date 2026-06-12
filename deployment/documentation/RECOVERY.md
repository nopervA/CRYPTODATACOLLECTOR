# Recovery Guide

Procedures for restoring the collector after crashes, unclean shutdowns, disk pressure, and VM failures.

## Built-in startup recovery

Every start runs `StorageManager._recover_segments()` before accepting live data:

1. Scans all dataset partitions under `COLLECTOR_DATA_DIR`
2. Removes unfinished `.tmp` and stale `.merge.*.parquet` files
3. Compacts orphaned `.segment.*.parquet` files into the daily `{dataset}.parquet`
4. Verifies row counts after merge

This runs automatically — no manual step is required after reboot or crash restart.

## Automatic restart after crash

The systemd unit uses `Restart=always` with `RestartSec=10`. If the process exits uncleanly:

```bash
sudo systemctl status binance-futures-collector
sudo journalctl -u binance-futures-collector -n 100 --no-pager
```

Common crash signatures:

| Symptom | Likely cause | Action |
|---|---|---|
| Repeated restart loop | Bad config/env, missing venv, permission error | Fix env; check `ensure-runtime.sh` output in journal |
| OOM kill | Undersized VM | Increase RAM; reduce symbol count temporarily |
| Disk full | Data volume full | Free space or expand disk (below) |
| Binance API errors | Network or rate limit | Usually self-heals; check egress firewall |

Rate-limited restart: `StartLimitBurst=5` within `StartLimitIntervalSec=300` prevents infinite tight loops.

## Clean shutdown vs kill

Preferred stop (drains queues, flushes buffers, finalizes Parquet):

```bash
sudo systemctl stop binance-futures-collector
```

Avoid `kill -9` except when the process is hung beyond `TimeoutStopSec=120`. SIGKILL skips graceful flush; startup recovery handles segments on next start.

## Recover from unclean shutdown

1. Start (or restart) the service:

   ```bash
   sudo systemctl start binance-futures-collector
   ```

2. Watch recovery in logs:

   ```bash
   sudo journalctl -u binance-futures-collector -f
   grep -i recover /var/log/binance-futures-collector/collector.log
   ```

3. Verify health:

   ```bash
   sudo /opt/binance-futures-collector/deployment/scripts/health-check.sh
   ```

## Disk full recovery

The queue monitor logs a warning when free disk drops below 10 GB.

1. Check usage:

   ```bash
   df -h /var/lib/binance-futures-collector
   du -sh /var/lib/binance-futures-collector/data/*
   ```

2. Free space:
   - Enable GCS backups and offload old partitions (see [BACKUP.md](BACKUP.md))
   - Expand the GCP persistent disk and grow the filesystem (`growpart` is in the `cloud-guest-utils` package — installed in [INSTALL.md](INSTALL.md)):

     ```bash
     sudo growpart /dev/sdb 1
     sudo resize2fs /dev/sdb1
     ```

3. Restart if the collector was failing writes:

   ```bash
   sudo systemctl restart binance-futures-collector
   ```

## VM rebuild (disaster recovery)

When replacing the VM but retaining data:

1. Attach the existing data disk to the new VM
2. Mount at `/var/lib/binance-futures-collector`
3. Follow [INSTALL.md](INSTALL.md) four-command deploy
4. Existing Parquet partitions are picked up immediately; collection continues from the current UTC day

Optional: restore from GCS if the data disk is lost (see [BACKUP.md](BACKUP.md)).

## WebSocket gap recovery

Trade gaps within 24 hours are backfilled automatically via REST `aggTrades`. Longer outages are logged as unrecoverable for tick data — depth and liquidation history cannot be backfilled from public APIs.

Check logs for:

```text
trade gap
unrecoverable
```

## Manual segment inspection

If recovery logs errors for a specific partition:

```bash
DATA=/var/lib/binance-futures-collector/data
ls -la "${DATA}/trades/symbol=BTCUSDT/date=$(date -u +%F)/"
```

Look for:

- `.segment.*.parquet` — pending segments (should compact on start)
- `.merge.*.parquet` — interrupted compaction (removed on start)
- `*.tmp` — interrupted write (removed on start)

Do not manually delete finalized `{dataset}.parquet` files unless you intend to lose that day's data.

## Health endpoint during recovery

`/status` is available once the health server starts, even while segment recovery runs. Counters may be zero briefly after restart until collectors reconnect.

## Escalation checklist

1. `systemctl status` and last 200 journal lines
2. Tail `collector.log`
3. `df -h` on data mount
4. `curl http://127.0.0.1:8080/status`
5. `health-check.sh` output
6. Disk inventory under `COLLECTOR_DATA_DIR`
