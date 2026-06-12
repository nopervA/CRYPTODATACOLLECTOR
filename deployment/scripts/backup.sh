#!/usr/bin/env bash
# Automated Google Cloud Storage backup for collector data, logs, and config.
set -euo pipefail

REPO_ROOT="/opt/binance-futures-collector"
ENV_FILE="${REPO_ROOT}/deployment/systemd/binance-futures-collector.env"
NOTIFY_SCRIPT="${REPO_ROOT}/deployment/scripts/telegram-notify.sh"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

export COLLECTOR_REPO_ROOT="${REPO_ROOT}"

if [[ "${BACKUP_ENABLED:-0}" != "1" ]]; then
  echo "binance-futures-collector-backup: BACKUP_ENABLED is not 1; skipping"
  exit 0
fi

if ! "${REPO_ROOT}/.venv/bin/python" -m collector.cloud_backup; then
  if [[ -x "${NOTIFY_SCRIPT}" ]]; then
    "${NOTIFY_SCRIPT}" CRITICAL "Cloud backup failed" "See backup_reports/ and journalctl" || true
  fi
  exit 1
fi

exit 0
