#!/usr/bin/env bash
# Idempotent first-boot setup invoked by systemd ExecStartPre (runs as root).
set -euo pipefail

REPO_ROOT="/opt/binance-futures-collector"
DATA_DIR="/var/lib/binance-futures-collector/data"
LOG_DIR="/var/log/binance-futures-collector"
ENV_FILE="${REPO_ROOT}/deployment/systemd/binance-futures-collector.env"
LOGROTATE_SRC="${REPO_ROOT}/deployment/logrotate/binance-futures-collector"
LOGROTATE_DST="/etc/logrotate.d/binance-futures-collector"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

DATA_DIR="${COLLECTOR_DATA_DIR:-${DATA_DIR}}"
LOG_DIR="${COLLECTOR_LOG_DIR:-${LOG_DIR}}"

if ! id collector &>/dev/null; then
  useradd --system --home-dir "${REPO_ROOT}" --shell /usr/sbin/nologin collector
fi

install -d -o collector -g collector -m 0750 "${DATA_DIR}"
install -d -o collector -g collector -m 0750 "${LOG_DIR}"

if [[ -d "${REPO_ROOT}" ]]; then
  chown -R collector:collector "${DATA_DIR}" "${LOG_DIR}"
  chmod -R u+rwX,g+rX "${REPO_ROOT}/.venv" 2>/dev/null || true
  chmod o+rX "${REPO_ROOT}" "${REPO_ROOT}/.venv" "${REPO_ROOT}/.venv/bin" 2>/dev/null || true
fi

if [[ -f "${LOGROTATE_SRC}" ]]; then
  sed "s|/var/log/binance-futures-collector|${LOG_DIR}|g" "${LOGROTATE_SRC}" > "${LOGROTATE_DST}.tmp"
  mv "${LOGROTATE_DST}.tmp" "${LOGROTATE_DST}"
  chmod 0644 "${LOGROTATE_DST}"
fi

HEALTH_UNIT="${REPO_ROOT}/deployment/systemd/binance-futures-collector-health.timer"
HEALTH_SERVICE="${REPO_ROOT}/deployment/systemd/binance-futures-collector-health.service"
if [[ -f "${HEALTH_UNIT}" && -f "${HEALTH_SERVICE}" ]]; then
  ln -sf "${HEALTH_SERVICE}" /etc/systemd/system/binance-futures-collector-health.service
  ln -sf "${HEALTH_UNIT}" /etc/systemd/system/binance-futures-collector-health.timer
  systemctl daemon-reload
  systemctl enable binance-futures-collector-health.timer >/dev/null 2>&1 || true
  systemctl start binance-futures-collector-health.timer >/dev/null 2>&1 || true
fi

BACKUP_ENABLED="${BACKUP_ENABLED:-0}"
BACKUP_TIMER="${REPO_ROOT}/deployment/systemd/binance-futures-collector-backup.timer"
BACKUP_SERVICE="${REPO_ROOT}/deployment/systemd/binance-futures-collector-backup.service"
if [[ "${BACKUP_ENABLED}" == "1" && -f "${BACKUP_TIMER}" && -f "${BACKUP_SERVICE}" ]]; then
  ln -sf "${BACKUP_SERVICE}" /etc/systemd/system/binance-futures-collector-backup.service
  ln -sf "${BACKUP_TIMER}" /etc/systemd/system/binance-futures-collector-backup.timer
  systemctl daemon-reload
  systemctl enable binance-futures-collector-backup.timer >/dev/null 2>&1 || true
  systemctl start binance-futures-collector-backup.timer >/dev/null 2>&1 || true
fi

exit 0
