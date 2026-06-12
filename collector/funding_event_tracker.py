from __future__ import annotations

from typing import Any

from collector.storage import StorageManager

FUNDING_WINDOWS = (
    "NORMAL",
    "PRE_60M",
    "PRE_30M",
    "PRE_15M",
    "PRE_5M",
    "POST_5M",
    "POST_15M",
    "POST_30M",
    "POST_60M",
)


def funding_direction(
    predicted_funding_rate: float, neutral_threshold: float
) -> int:
    if abs(predicted_funding_rate) < neutral_threshold:
        return 0
    if predicted_funding_rate > 0.0:
        return 1
    return -1


def build_funding_event_record(
    funding_record: dict[str, Any],
    *,
    neutral_threshold: float = 0.00001,
    funding_period_ms: int = 8 * 60 * 60 * 1000,
) -> dict[str, Any]:
    timestamp = int(funding_record["timestamp"])
    next_funding_time = int(funding_record["next_funding_time"])
    predicted_funding_rate = float(funding_record["funding_rate"])

    if timestamp < next_funding_time:
        previous_funding_time = next_funding_time - funding_period_ms
        minutes_to_funding = (next_funding_time - timestamp) / 60_000.0
    else:
        previous_funding_time = next_funding_time
        minutes_to_funding = 0.0

    minutes_since_previous_funding = max(
        0.0, (timestamp - previous_funding_time) / 60_000.0
    )

    is_pre_funding_60m = 0.0 < minutes_to_funding <= 60.0
    is_pre_funding_30m = 0.0 < minutes_to_funding <= 30.0
    is_pre_funding_15m = 0.0 < minutes_to_funding <= 15.0
    is_pre_funding_5m = 0.0 < minutes_to_funding <= 5.0
    is_post_funding_5m = 0.0 < minutes_since_previous_funding <= 5.0
    is_post_funding_15m = 0.0 < minutes_since_previous_funding <= 15.0
    is_post_funding_30m = 0.0 < minutes_since_previous_funding <= 30.0
    is_post_funding_60m = 0.0 < minutes_since_previous_funding <= 60.0

    if is_pre_funding_5m:
        funding_window = "PRE_5M"
    elif is_pre_funding_15m:
        funding_window = "PRE_15M"
    elif is_pre_funding_30m:
        funding_window = "PRE_30M"
    elif is_pre_funding_60m:
        funding_window = "PRE_60M"
    elif is_post_funding_5m:
        funding_window = "POST_5M"
    elif is_post_funding_15m:
        funding_window = "POST_15M"
    elif is_post_funding_30m:
        funding_window = "POST_30M"
    elif is_post_funding_60m:
        funding_window = "POST_60M"
    else:
        funding_window = "NORMAL"

    return {
        "timestamp": timestamp,
        "symbol": str(funding_record["symbol"]),
        "predicted_funding_rate": predicted_funding_rate,
        "next_funding_time": next_funding_time,
        "minutes_to_funding": minutes_to_funding,
        "minutes_since_previous_funding": minutes_since_previous_funding,
        "funding_window": funding_window,
        "is_pre_funding_60m": is_pre_funding_60m,
        "is_pre_funding_30m": is_pre_funding_30m,
        "is_pre_funding_15m": is_pre_funding_15m,
        "is_pre_funding_5m": is_pre_funding_5m,
        "is_post_funding_5m": is_post_funding_5m,
        "is_post_funding_15m": is_post_funding_15m,
        "is_post_funding_30m": is_post_funding_30m,
        "is_post_funding_60m": is_post_funding_60m,
        "funding_direction": funding_direction(
            predicted_funding_rate, neutral_threshold
        ),
    }


class FundingEventTracker:
    """Derive funding settlement regime metrics from funding snapshots."""

    def __init__(
        self,
        storage: StorageManager,
        *,
        neutral_threshold: float = 0.00001,
        funding_period_ms: int = 8 * 60 * 60 * 1000,
    ) -> None:
        self._storage = storage
        self._neutral_threshold = neutral_threshold
        self._funding_period_ms = funding_period_ms

    async def on_funding(self, funding_record: dict[str, Any]) -> None:
        await self._storage.write(
            "funding_event",
            build_funding_event_record(
                funding_record,
                neutral_threshold=self._neutral_threshold,
                funding_period_ms=self._funding_period_ms,
            ),
        )
