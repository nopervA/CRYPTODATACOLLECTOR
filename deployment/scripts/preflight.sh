#!/usr/bin/env bash
# Startup validation before the collector process starts. Runs as root via ExecStartPre.
set -euo pipefail

REPO_ROOT="/opt/binance-futures-collector"
ENV_FILE="${REPO_ROOT}/deployment/systemd/binance-futures-collector.env"
PYTHON="${REPO_ROOT}/.venv/bin/python"
MAIN="${REPO_ROOT}/main.py"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

DATA_DIR="${COLLECTOR_DATA_DIR:-/var/lib/binance-futures-collector/data}"
LOG_DIR="${COLLECTOR_LOG_DIR:-/var/log/binance-futures-collector}"

fail() {
  echo "preflight: $*" >&2
  exit 1
}

[[ -d "${REPO_ROOT}" ]] || fail "repository missing: ${REPO_ROOT}"
[[ -x "${PYTHON}" ]] || fail "Python venv not executable: ${PYTHON}"
[[ -f "${MAIN}" ]] || fail "entrypoint missing: ${MAIN}"

for script in "${REPO_ROOT}"/deployment/scripts/*.sh; do
  [[ -f "${script}" ]] || continue
  [[ -x "${script}" ]] || fail "script not executable: ${script}"
done

[[ -d "${DATA_DIR}" ]] || fail "data directory missing: ${DATA_DIR}"
[[ -d "${LOG_DIR}" ]] || fail "log directory missing: ${LOG_DIR}"

if ! id collector &>/dev/null; then
  fail "system user 'collector' does not exist (run ensure-runtime.sh first)"
fi

if ! sudo -u collector test -w "${DATA_DIR}"; then
  fail "collector user cannot write to ${DATA_DIR}"
fi
if ! sudo -u collector test -w "${LOG_DIR}"; then
  fail "collector user cannot write to ${LOG_DIR}"
fi

if ! sudo -u collector "${PYTHON}" -c "import collector; import collector.main" 2>/dev/null; then
  fail "collector package import failed"
fi

echo "preflight: ok repo=${REPO_ROOT} data=${DATA_DIR} log=${LOG_DIR}"
