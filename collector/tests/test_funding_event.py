import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow.parquet as pq

from collector.config import Settings
from collector.funding_event_tracker import (
    FundingEventTracker,
    build_funding_event_record,
    funding_direction,
)
from collector.storage import StorageManager


def _funding_record(
    *,
    timestamp: int,
    next_funding_time: int,
    funding_rate: float = 0.0001,
    symbol: str = "BTCUSDT",
) -> dict:
    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "funding_rate": funding_rate,
        "next_funding_time": next_funding_time,
        "mark_price": 65_000.0,
        "received_at": timestamp + 1,
    }


def test_funding_direction_neutral_positive_negative() -> None:
    assert funding_direction(0.0, 0.00001) == 0
    assert funding_direction(0.000005, 0.00001) == 0
    assert funding_direction(-0.000005, 0.00001) == 0
    assert funding_direction(0.0001, 0.00001) == 1
    assert funding_direction(-0.0001, 0.00001) == -1


def test_build_funding_event_pre_funding_windows() -> None:
    next_funding = 1_700_000_000_000
    record = build_funding_event_record(
        _funding_record(timestamp=next_funding - 10 * 60_000, next_funding_time=next_funding)
    )
    assert record["minutes_to_funding"] == 10.0
    assert record["is_pre_funding_15m"] is True
    assert record["is_pre_funding_5m"] is False
    assert record["funding_window"] == "PRE_15M"
    assert record["is_post_funding_5m"] is False


def test_build_funding_event_post_funding_windows() -> None:
    next_funding = 1_700_000_000_000
    period_ms = 8 * 60 * 60 * 1000
    timestamp = next_funding + 20 * 60_000
    record = build_funding_event_record(
        _funding_record(timestamp=timestamp, next_funding_time=next_funding),
        funding_period_ms=period_ms,
    )
    assert record["minutes_to_funding"] == 0.0
    assert record["minutes_since_previous_funding"] == 20.0
    assert record["is_post_funding_30m"] is True
    assert record["is_post_funding_60m"] is True
    assert record["is_post_funding_15m"] is False
    assert record["funding_window"] == "POST_30M"


def test_build_funding_event_normal_window() -> None:
    next_funding = 1_700_000_000_000
    period_ms = 8 * 60 * 60 * 1000
    timestamp = next_funding + 90 * 60_000
    record = build_funding_event_record(
        _funding_record(timestamp=timestamp, next_funding_time=next_funding),
        funding_period_ms=period_ms,
    )
    assert record["funding_window"] == "NORMAL"
    assert record["is_post_funding_60m"] is False
    assert record["is_pre_funding_60m"] is False


def test_funding_event_tracker_writes_derived_record() -> None:
    class CollectStorage:
        def __init__(self) -> None:
            self.records: list[dict] = []

        async def write(self, dataset: str, record: dict) -> None:
            assert dataset == "funding_event"
            self.records.append(record)

    async def scenario() -> None:
        storage = CollectStorage()
        tracker = FundingEventTracker(storage, neutral_threshold=0.00001)
        next_funding = 1_700_000_000_000
        funding = _funding_record(
            timestamp=next_funding - 3 * 60_000,
            next_funding_time=next_funding,
            funding_rate=-0.0002,
        )
        await tracker.on_funding(funding)
        assert len(storage.records) == 1
        event = storage.records[0]
        assert event["predicted_funding_rate"] == -0.0002
        assert event["funding_direction"] == -1
        assert event["funding_window"] == "PRE_5M"
        assert event["is_pre_funding_5m"] is True

    asyncio.run(scenario())


def test_funding_event_storage_round_trip(tmp_path) -> None:
    async def scenario() -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            symbols=("BTCUSDT",),
        )
        storage = StorageManager(settings)
        await storage.start()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        next_funding = now_ms + 30 * 60_000
        funding = _funding_record(
            timestamp=now_ms,
            next_funding_time=next_funding,
            funding_rate=0.00005,
        )
        tracker = FundingEventTracker(
            storage,
            neutral_threshold=settings.funding_neutral_threshold,
            funding_period_ms=int(settings.funding_period_hours * 3_600_000),
        )
        await tracker.on_funding(funding)
        await storage.close()

        day = datetime.fromtimestamp(now_ms / 1000, UTC).date().isoformat()
        path = (
            tmp_path
            / "data"
            / "funding_event"
            / "symbol=BTCUSDT"
            / f"date={day}"
            / "funding_event.parquet"
        )
        assert path.exists()
        table = pq.ParquetFile(path).read()
        assert table.num_rows == 1
        row = table.to_pylist()[0]
        assert row["symbol"] == "BTCUSDT"
        assert row["predicted_funding_rate"] == 0.00005
        assert row["funding_window"] == "PRE_30M"
        assert row["funding_direction"] == 1

    asyncio.run(scenario())
