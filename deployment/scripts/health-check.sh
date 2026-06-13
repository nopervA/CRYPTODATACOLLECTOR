#!/usr/bin/env bash
# Verify the collector health endpoint. Exit 0 when healthy, non-zero otherwise.
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

HEALTH_HOST="${HEALTH_HOST:-127.0.0.1}"
HEALTH_PORT="${HEALTH_PORT:-8080}"
HEALTH_MAX_OI_AGE_MINUTES="${HEALTH_MAX_OI_AGE_MINUTES:-5}"
HEALTH_MAX_FUNDING_AGE_MINUTES="${HEALTH_MAX_FUNDING_AGE_MINUTES:-15}"
HEALTH_MAX_TRADE_AGE_MINUTES="${HEALTH_MAX_TRADE_AGE_MINUTES:-2}"
HEALTH_MAX_DEPTH_AGE_MINUTES="${HEALTH_MAX_DEPTH_AGE_MINUTES:-5}"
URL="http://${HEALTH_HOST}:${HEALTH_PORT}/status"

notify_failure() {
  local message="$1"
  if [[ -x "${NOTIFY_SCRIPT}" ]]; then
    "${NOTIFY_SCRIPT}" CRITICAL "health_check_failed" "${message}" || true
  fi
}

if ! command -v curl >/dev/null 2>&1; then
  echo "health-check: curl is required" >&2
  notify_failure "curl is required"
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "health-check: python3 is required" >&2
  notify_failure "python3 is required"
  exit 2
fi

if ! response="$(curl --fail --silent --show-error --max-time 5 "${URL}")"; then
  message="HTTP request failed for ${URL}"
  echo "health-check: ${message}" >&2
  notify_failure "${message}"
  exit 1
fi

set +e
check_output="$(
  python3 - "${response}" \
    "${HEALTH_MAX_OI_AGE_MINUTES}" \
    "${HEALTH_MAX_FUNDING_AGE_MINUTES}" \
    "${HEALTH_MAX_TRADE_AGE_MINUTES}" \
    "${HEALTH_MAX_DEPTH_AGE_MINUTES}" <<'PY'
import json
import sys
from datetime import UTC, datetime, timedelta

payload = json.loads(sys.argv[1])
max_oi_minutes = float(sys.argv[2])
max_funding_minutes = float(sys.argv[3])
max_trade_minutes = float(sys.argv[4])
max_depth_minutes = float(sys.argv[5])

required = (
    "uptime_hours",
    "symbols",
    "last_oi_update",
    "last_funding_update",
    "last_trade_update",
    "last_depth50_update",
    "websocket_reconnects",
    "integrity_error_count",
)
missing = [key for key in required if key not in payload]
if missing:
    raise SystemExit(f"health-check: missing fields: {', '.join(missing)}")

symbols = int(payload["symbols"])
if symbols <= 0:
    raise SystemExit("health-check: symbols must be > 0")

uptime_hours = float(payload["uptime_hours"])
if uptime_hours < 0:
    raise SystemExit("health-check: invalid uptime_hours")

integrity_errors = int(payload["integrity_error_count"])
if uptime_hours >= 0.25 and integrity_errors > 0:
    raise SystemExit(
        f"health-check: integrity_error_count={integrity_errors} (compaction failures)"
    )

def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)

def assert_fresh(field: str, max_minutes: float) -> None:
    if uptime_hours < max_minutes / 60.0:
        return
    last = parse_iso(payload.get(field))
    if last is None:
        raise SystemExit(f"health-check: {field} is stale/missing")
    if datetime.now(UTC) - last > timedelta(minutes=max_minutes):
        raise SystemExit(
            f"health-check: {field} older than {max_minutes} minutes"
        )

assert_fresh("last_oi_update", max_oi_minutes)
assert_fresh("last_funding_update", max_funding_minutes)
assert_fresh("last_trade_update", max_trade_minutes)
assert_fresh("last_depth50_update", max_depth_minutes)

print(
    "health-check: ok "
    f"uptime={payload['uptime_hours']}h symbols={symbols} "
    f"trades={payload.get('trades_received', 0)} "
    f"ws_reconnects={payload.get('websocket_reconnects', 0)}"
)
PY
)"
check_rc=$?
set -e

if (( check_rc != 0 )); then
  echo "${check_output}" >&2
  notify_failure "${check_output}"
  exit "${check_rc}"
fi

echo "${check_output}"
