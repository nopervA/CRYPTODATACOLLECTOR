# Production Installation

Deploy the Binance USDT-M Futures research collector on **Google Cloud Compute Engine** running **Debian 12 (bookworm)**.

Target layout:

| Path | Purpose |
|---|---|
| `/opt/binance-futures-collector` | Application code and virtualenv |
| `/var/lib/binance-futures-collector/data` | Parquet datasets (attach large disk here) |
| `/var/log/binance-futures-collector` | Application logs |

## VM prerequisites (one-time)

Create a GCP VM with the **Debian 12** image (`debian-12` / bookworm). Recommended starting shape for the 25-symbol universe:

| Resource | Recommendation |
|---|---|
| Machine type | `e2-standard-4` or larger |
| Boot disk | 30 GB |
| Data disk | 500 GB SSD, mounted at `/var/lib/binance-futures-collector` |
| Network | Egress to Binance APIs; health endpoint stays on localhost |

### Attach and mount the data disk

```bash
sudo mkfs.ext4 -F /dev/disk/by-id/google-collector-data
sudo mkdir -p /var/lib/binance-futures-collector
echo '/dev/disk/by-id/google-collector-data /var/lib/binance-futures-collector ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab
sudo mount -a
sudo mkdir -p /var/lib/binance-futures-collector/data
```

Replace `google-collector-data` with your attached disk identifier (`ls -l /dev/disk/by-id/google-*`).

### Install system packages

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git curl logrotate cloud-guest-utils
```

Debian 12 ships Python 3.11, which satisfies the collector requirement (3.11+). `cloud-guest-utils` provides `growpart` for online disk expansion (see [RECOVERY.md](RECOVERY.md)).

Optional (for GCS backups): install the Google Cloud SDK on Debian 12 — see [BACKUP.md](BACKUP.md) — and grant the VM service account Storage Object Admin on your backup bucket.

## Deploy in four commands

Clone to the fixed install path expected by the systemd unit:

```bash
sudo git clone https://github.com/YOUR_ORG/master-handoff-crypto-futures-edge-discovery.git /opt/binance-futures-collector
cd /opt/binance-futures-collector
sudo python3 -m venv .venv && sudo .venv/bin/pip install -r requirements.txt
sudo chmod +x deployment/scripts/*.sh
sudo systemctl enable /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.service
sudo systemctl start binance-futures-collector
```

That is the complete deployment. No manual user creation, directory setup, or logrotate configuration is required — the service runs `ensure-runtime.sh` on first start.

### Verify

```bash
sudo systemctl status binance-futures-collector
curl -s http://127.0.0.1:8080/status | python3 -m json.tool
sudo /opt/binance-futures-collector/deployment/scripts/health-check.sh
```

Expected `/status` fields include `uptime_hours`, `symbols`, `trades_received`, and recent `last_oi_update` / `last_funding_update` timestamps.

## What happens on first start

`deployment/scripts/ensure-runtime.sh` (invoked automatically as `ExecStartPre`):

1. Creates the `collector` system user if missing
2. Creates data and log directories with correct ownership
3. Installs logrotate configuration to `/etc/logrotate.d/`
4. Enables the health-check timer (every minute)
5. Enables the backup timer when `BACKUP_ENABLED=1` in the env file

## Configuration

Edit `deployment/systemd/binance-futures-collector.env` before or after first start, then reload:

```bash
sudo nano /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.env
sudo systemctl restart binance-futures-collector
```

Common overrides:

| Variable | Default | Notes |
|---|---|---|
| `COLLECTOR_DATA_DIR` | `/var/lib/binance-futures-collector/data` | Point at mounted data disk |
| `COLLECTOR_LOG_DIR` | `/var/log/binance-futures-collector` | Application log directory |
| `COLLECTOR_SYMBOLS` | 25 default symbols | Comma-separated override |
| `HEALTH_HOST` | `127.0.0.1` | Bind locally; use reverse proxy for external probes |
| `HEALTH_PORT` | `8080` | Health HTTP port |
| `LOG_LEVEL` | `INFO` | Collector log verbosity |
| `BACKUP_ENABLED` | `0` | Set `1` to enable weekly GCS backup timer |
| `BACKUP_GCS_URI` | unset | e.g. `gs://binance-futures-research-data` |
| `BACKUP_GCS_PREFIX` | `weekly` | Prefix under bucket for dated snapshots |

Persistent overrides without editing the repo file:

```bash
sudo systemctl edit binance-futures-collector
```

## Automatic restart and boot behavior

The systemd unit provides:

| Setting | Value | Effect |
|---|---|---|
| `Restart=always` | on | Restarts after crash or unclean exit |
| `RestartSec=10` | 10 s | Backoff between restart attempts |
| `WantedBy=multi-user.target` | enabled | Starts automatically on reboot |
| `TimeoutStopSec=120` | 120 s | Grace period for queue drain on shutdown |

On startup the collector calls `StorageManager._recover_segments()` to finalize incomplete Parquet segments from prior crashes.

## Log rotation

Two layers:

1. **In-process:** `TimedRotatingFileHandler` rotates `collector.log` daily, keeps 30 backups (UTC).
2. **System:** logrotate config installed to `/etc/logrotate.d/binance-futures-collector` (daily, 30 rotations, compress).

View logs:

```bash
sudo journalctl -u binance-futures-collector -f
sudo tail -f /var/log/binance-futures-collector/collector.log
```

## Firewall

The health endpoint binds to `127.0.0.1` by default. Do not expose port 8080 publicly unless fronted by authenticated monitoring infrastructure.

GCP egress to `fstream.binance.com` and `fapi.binance.com` must be allowed.

## Related documentation

- [UPGRADE.md](UPGRADE.md) — release upgrades
- [RECOVERY.md](RECOVERY.md) — crash and data recovery
- [BACKUP.md](BACKUP.md) — GCS backup and restore
- [MONITORING.md](MONITORING.md) — health checks and alerting
