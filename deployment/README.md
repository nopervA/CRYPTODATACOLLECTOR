# Deployment

Production deployment assets for **Google Cloud VM · Debian 12 (bookworm)**.

## Quick start

```bash
sudo git clone <repo-url> /opt/binance-futures-collector
cd /opt/binance-futures-collector
sudo python3 -m venv .venv && sudo .venv/bin/pip install -r requirements.txt
sudo chmod +x deployment/scripts/*.sh
sudo systemctl enable /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.service
sudo systemctl start binance-futures-collector
```

See [documentation/INSTALL.md](documentation/INSTALL.md) for VM sizing, disk mount, and verification.

## Layout

```text
deployment/
  systemd/
    binance-futures-collector.service       # main service (restart + boot)
    binance-futures-collector.env           # production defaults
    binance-futures-collector-health.*      # health verification timer
    binance-futures-collector-backup.*      # optional GCS backup timer
  scripts/
    ensure-runtime.sh                       # first-boot user/dirs/logrotate/timers
    health-check.sh                         # /status probe for monitoring
    backup.sh                               # gsutil rsync to GCS
    telegram-notify.sh                      # standalone Telegram helper
  logrotate/
    binance-futures-collector               # daily log rotation
  documentation/
    INSTALL.md
    UPGRADE.md
    RECOVERY.md
    BACKUP.md
    MONITORING.md
    TELEGRAM.md
```

## Features

| Feature | Implementation |
|---|---|
| Start on reboot | `WantedBy=multi-user.target` |
| Crash restart | `Restart=always`, `RestartSec=10` |
| Log rotation | In-process daily rotate + logrotate |
| Startup recovery | Collector `StorageManager._recover_segments()` |
| Health verification | Timer + `health-check.sh` |
| Backup | Optional GCS rsync timer |
| Monitoring | `/status` JSON + journal + Telegram alerts |

## Documentation

- [INSTALL.md](documentation/INSTALL.md)
- [UPGRADE.md](documentation/UPGRADE.md)
- [RECOVERY.md](documentation/RECOVERY.md)
- [BACKUP.md](documentation/BACKUP.md)
- [MONITORING.md](documentation/MONITORING.md)
- [TELEGRAM.md](documentation/TELEGRAM.md)
