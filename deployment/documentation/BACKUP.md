# Google Cloud Storage Backup

Automated **weekly** backup of collector data and logs to Google Cloud Storage. The collector process is **never stopped** during backup.

## Schedule

| Setting | Value |
|---|---|
| Time | **Sunday 03:00 UTC** |
| Timer unit | `binance-futures-collector-backup.timer` |
| Engine | `python -m collector.cloud_backup` via `deployment/scripts/backup.sh` |

## Backup scope

| Local path (production) | GCS destination |
|---|---|
| `COLLECTOR_DATA_DIR` (default `/var/lib/binance-futures-collector/data`) | `gs://…/weekly/YYYY-MM-DD/data/` |
| `COLLECTOR_LOG_DIR` (default `/var/log/binance-futures-collector`) | `gs://…/weekly/YYYY-MM-DD/logs/` |

Production bucket (verified):

```text
gs://binance-futures-research-data/weekly/2026-06-15/data/
gs://binance-futures-research-data/weekly/2026-06-15/logs/
```

**Excluded from rsync:** `__pycache__`, `*.pyc`, `.pytest_cache`, `*.tmp`, `.segment.*`, `.merge.*`

## Status file

After each run, `BACKUP_STATUS_FILE` (default `/var/lib/binance-futures-collector/backup_status.json`):

```json
{
  "timestamp": "2026-06-15T03:12:34Z",
  "bytes_uploaded": 141450611,
  "duration_seconds": 312.456,
  "success": true,
  "error_message": null,
  "backup_uri": "gs://binance-futures-research-data/weekly/2026-06-15"
}
```

Detailed run reports are also written to `backup_reports/YYYY-MM-DD.json`.

## Telegram alerts

| Outcome | Severity | Mechanism |
|---|---|---|
| Success | INFO | `telegram-notify.sh` from `collector.cloud_backup` |
| Failure | CRITICAL | `telegram-notify.sh` + in-process `backup_failure` on next monitor cycle |

Backup runs in a **separate systemd oneshot** — failures never stop the collector.

## Enable on Debian 12

### 1. Bucket

Bucket already exists:

```text
gs://binance-futures-research-data
```

Ensure the VM service account has `roles/storage.objectAdmin` on the bucket.

### 2. Install Google Cloud CLI

```bash
sudo apt-get install -y google-cloud-cli
gcloud auth list   # VM should use attached service account
```

### 3. Configure env

```bash
sudo nano /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.env
```

```ini
BACKUP_ENABLED=1
BACKUP_GCS_URI=gs://binance-futures-research-data
BACKUP_GCS_PREFIX=weekly
BACKUP_REPORT_DIR=/opt/binance-futures-collector/backup_reports
BACKUP_STATUS_FILE=/var/lib/binance-futures-collector/backup_status.json
BACKUP_MAX_RETRIES=3
```

### 4. Enable timer

```bash
sudo systemctl daemon-reload
sudo systemctl enable binance-futures-collector-backup.timer
sudo systemctl start binance-futures-collector-backup.timer
sudo systemctl list-timers binance-futures-collector-backup.timer
```

The timer is also registered automatically when `BACKUP_ENABLED=1` and the collector service starts (`ensure-runtime.sh`).

### 5. Manual run

```bash
sudo /opt/binance-futures-collector/deployment/scripts/backup.sh
# or
cd /opt/binance-futures-collector
.venv/bin/python -m collector.cloud_backup
```

## Retry and failure handling

- Up to **3 attempts** (`BACKUP_MAX_RETRIES`)
- Exponential backoff: 5s, 10s, 20s
- Failures write status JSON with `success: false`
- Collector keeps running; backup is an independent oneshot unit

## Restore

```bash
DAY=2026-06-15
sudo systemctl stop binance-futures-collector   # optional, for clean restore
gcloud storage rsync -r \
  "gs://binance-futures-research-data/weekly/${DAY}/data" \
  /var/lib/binance-futures-collector/data
gcloud storage rsync -r \
  "gs://binance-futures-research-data/weekly/${DAY}/logs" \
  /var/log/binance-futures-collector
sudo systemctl start binance-futures-collector
```

## Storage estimates (25 symbols)

| Phase | Typical upload |
|---|---|
| First weekly full | 2–8 GB |
| Steady weekly delta | 200 MB – 2 GB |
| 12 weekly snapshots retained | ~25–100 GB in bucket |

**Recommendation:** add a GCS lifecycle rule to delete `weekly/` prefixes older than 90 days.

## Architecture

```text
systemd timer (Sun 03:00 UTC)
    └── backup.sh
            └── python -m collector.cloud_backup
                    ├── gcloud storage rsync (data, logs)
                    ├── verify (ls + du)
                    ├── backup_status.json
                    ├── backup_reports/YYYY-MM-DD.json
                    └── telegram-notify.sh (INFO / CRITICAL)
```

Low memory: streaming rsync subprocesses, no in-memory dataset loading.

Restart safe: each run uses a dated GCS prefix; rsync is idempotent within a prefix.
