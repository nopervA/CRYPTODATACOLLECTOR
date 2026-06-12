# Google Cloud Storage Backup

Automated daily backup of collector data, logs, README, and configuration snapshots to Google Cloud Storage. The collector process is **never stopped** during backup.

## Schedule

| Setting | Default |
|---|---|
| Time | **03:00 UTC** daily |
| Timer unit | `binance-futures-collector-backup.timer` |
| Engine | `python -m collector.cloud_backup` |

Override schedule via systemd drop-in or edit the timer. Environment variables document intent:

```ini
BACKUP_HOUR_UTC=3
BACKUP_MINUTE_UTC=0
```

## Backup scope

| Path | Destination |
|---|---|
| `COLLECTOR_DATA_DIR` (`data/`) | `gs://.../YYYY-MM-DD/data/` |
| `COLLECTOR_LOG_DIR` (`logs/`) | `gs://.../YYYY-MM-DD/logs/` |
| `README.md` (repo root) | `gs://.../YYYY-MM-DD/README.md` |
| Config snapshot | `gs://.../YYYY-MM-DD/config/` |

**Excluded:** `__pycache__`, `*.pyc`, `.pytest_cache`, `*.tmp`, `.segment.*`, `.merge.*`

## Reports

After each run:

```text
backup_reports/YYYY-MM-DD.json
```

Fields:

| Field | Description |
|---|---|
| `timestamp` | UTC ISO8601 completion time |
| `report_day` | UTC day backed up |
| `files_uploaded` | Local file count in scope |
| `bytes_uploaded` | Local byte count in scope |
| `duration_seconds` | Wall-clock duration |
| `success` | Boolean |
| `error_message` | Set on failure |
| `gcs_uri` | Destination prefix |
| `objects_verified` | Objects seen via `gcloud storage ls -r` |
| `bytes_verified` | Bytes via `gcloud storage du -s` |
| `attempts` | Retry count used |

Telegram/status integration continues via `backup_status.json`.

## Enable on Debian 12

### 1. Create bucket

```bash
gcloud storage buckets create gs://YOUR-PROJECT-collector-backups \
  --location=us-central1 \
  --uniform-bucket-level-access
```

### 2. IAM permissions (VM service account)

Minimum role for the backup service account or VM default SA:

| Permission | Purpose |
|---|---|
| `storage.objects.create` | Upload objects |
| `storage.objects.delete` | Rsync overwrite/delete deltas |
| `storage.objects.get` | Verify listing |
| `storage.objects.list` | Verify counts |

**Recommended IAM role:** `roles/storage.objectAdmin` on the backup bucket prefix.

Example binding:

```bash
gcloud storage buckets add-iam-policy-binding gs://YOUR-PROJECT-collector-backups \
  --member="serviceAccount:COLLECTOR_VM_SA@YOUR-PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### 3. Install Google Cloud CLI

```bash
sudo apt-get install -y google-cloud-cli
gcloud auth list   # VM should use attached service account
```

### 4. Configure env

```bash
sudo nano /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.env
```

```ini
BACKUP_ENABLED=1
BACKUP_GCS_URI=gs://YOUR-PROJECT-collector-backups/binance-futures
BACKUP_REPORT_DIR=/opt/binance-futures-collector/backup_reports
BACKUP_MAX_RETRIES=3
```

### 5. Enable timer

```bash
sudo systemctl enable binance-futures-collector-backup.timer
sudo systemctl start binance-futures-collector-backup.timer
sudo systemctl status binance-futures-collector-backup.timer
```

### 6. Manual run

```bash
sudo /opt/binance-futures-collector/deployment/scripts/backup.sh
# or
cd /opt/binance-futures-collector
.venv/bin/python -m collector.cloud_backup
```

## Retry and failure handling

- Up to **3 attempts** (configurable via `BACKUP_MAX_RETRIES`)
- Exponential backoff: 5s, 10s, 20s
- Failures write `backup_reports/YYYY-MM-DD.json` with `success: false`
- Collector keeps running; backup is a separate systemd oneshot
- Optional Telegram alert via `telegram-notify.sh`

## Restore

```bash
DAY=2026-06-11
gcloud storage rsync -r \
  "gs://YOUR-PROJECT-collector-backups/binance-futures/${DAY}/data" \
  /var/lib/binance-futures-collector/data
```

## Traffic and storage estimates (25 symbols)

### Daily backup traffic (incremental rsync)

| Phase | Typical |
|---|---|
| Day 1 (full) | 2–8 GB upload |
| Steady state | 50–500 MB/day (delta Parquet + logs) |
| Peak volatile day | 1–3 GB |

Uses `gcloud storage rsync` — only changed objects transfer after the first full sync.

### GCS storage growth (90-day retention)

| Component | Estimate |
|---|---|
| Data growth | ~75–200 GB/month on VM |
| GCS with daily dated prefixes | ~same order if all days retained |
| 90-day full mirror | **225–450 GB** in bucket |
| Logs + config per day | < 50 MB |

**Recommendation:** add a GCS lifecycle rule to delete prefixes older than 90 days, or keep only `LATEST` + weekly full snapshots.

```bash
gcloud storage buckets update gs://YOUR-PROJECT-collector-backups \
  --lifecycle-file=- <<'EOF'
{
  "rule": [{
    "action": {"type": "Delete"},
    "condition": {"age": 90}
  }]
}
EOF
```

## Architecture

```text
systemd timer (03:00 UTC)
    └── backup.sh
            └── python -m collector.cloud_backup
                    ├── gcloud storage rsync (data, logs)
                    ├── gcloud storage cp (README, config)
                    ├── verify (ls + du)
                    └── backup_reports/YYYY-MM-DD.json
```

Low memory: streaming rsync subprocesses, no in-memory dataset loading.

Restart safe: each run is idempotent; dated GCS prefix prevents overwrite confusion.
