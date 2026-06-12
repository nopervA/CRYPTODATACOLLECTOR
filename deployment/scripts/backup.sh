#!/usr/bin/env bash
# Weekly GCS backup for collector data and logs. Independent of the collector service.
set -uo pipefail

REPO_ROOT="/opt/binance-futures-collector"
ENV_FILE="${REPO_ROOT}/deployment/systemd/binance-futures-collector.env"
PYTHON="${REPO_ROOT}/.venv/bin/python"

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

if [[ ! -x "${PYTHON}" ]]; then
  echo "binance-futures-collector-backup: Python venv not found at ${PYTHON}" >&2
  exit 1
fi

# Backup failures must never stop the collector (separate oneshot unit).
# Telegram notifications are sent from collector.cloud_backup after status is written.
if "${PYTHON}" -m collector.cloud_backup; then
  exit 0
fi

exit 1
