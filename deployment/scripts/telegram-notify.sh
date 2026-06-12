#!/usr/bin/env bash
# Send a Telegram alert using TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.
# Usage: telegram-notify.sh SEVERITY "Title" ["Details"]
# Failure tolerant by default (exit 0 when Telegram is unavailable).
set -u

REPO_ROOT="/opt/binance-futures-collector"
ENV_FILE="${REPO_ROOT}/deployment/systemd/binance-futures-collector.env"
STRICT=0

if [[ "${1:-}" == "--strict" ]]; then
  STRICT=1
  shift
fi

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

SEVERITY="${1:-WARNING}"
TITLE="${2:-Collector alert}"
DETAILS="${3:-}"
TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TELEGRAM_CHAT_ID:-}"

if [[ -z "${TOKEN}" || -z "${CHAT_ID}" ]]; then
  echo "telegram-notify: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set" >&2
  [[ "${STRICT}" -eq 1 ]] && exit 1
  exit 0
fi

TEXT="[${SEVERITY}] ${TITLE}"
if [[ -n "${DETAILS}" ]]; then
  TEXT="${TEXT}"$'\n'"${DETAILS}"
fi
TEXT="${TEXT}"$'\n'"time=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if ! curl --fail --silent --show-error --max-time 10 \
  -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
  --data-urlencode "chat_id=${CHAT_ID}" \
  --data-urlencode "text=${TEXT}" \
  --data-urlencode "disable_web_page_preview=true"; then
  echo "telegram-notify: delivery failed" >&2
  [[ "${STRICT}" -eq 1 ]] && exit 1
fi

exit 0
