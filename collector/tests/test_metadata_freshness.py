from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collector.config import Settings
from collector.health import HealthState
from collector.metadata_collector import MetadataCollector
from collector.metadata_freshness import (
    hydrate_last_metadata_update,
    latest_metadata_update_from_disk,
)
from collector.storage import SCHEMAS


def _write_metadata_parquet(path, timestamp_ms: int, symbol: str = "BTCUSDT") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "symbol": symbol,
        "timestamp": timestamp_ms,
        "tick_size": 0.1,
        "step_size": 0.001,
        "min_qty": 0.001,
        "min_notional": 5.0,
        "maker_fee": None,
        "taker_fee": None,
    }
    schema = SCHEMAS["metadata"]
    frame = pd.DataFrame.from_records([record], columns=schema.names)
    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False, safe=False)
    pq.write_table(table, path, compression="snappy")


def test_latest_metadata_update_from_disk_returns_none_when_missing(tmp_path) -> None:
    assert latest_metadata_update_from_disk(tmp_path / "data") is None


def test_latest_metadata_update_from_disk_reads_existing_parquet(tmp_path) -> None:
    data_dir = tmp_path / "data"
    day = "2026-06-11"
    ts_ms = int(datetime(2026, 6, 11, 0, 0, 5, tzinfo=UTC).timestamp() * 1000)
    _write_metadata_parquet(
        data_dir / "metadata" / f"date={day}" / "metadata.parquet",
        ts_ms,
    )

    result = latest_metadata_update_from_disk(data_dir)

    assert result == "2026-06-11T00:00:05Z"


def test_hydrate_last_metadata_update_on_fresh_startup(tmp_path) -> None:
    data_dir = tmp_path / "data"
    ts_ms = int(datetime(2026, 6, 11, 0, 0, 5, tzinfo=UTC).timestamp() * 1000)
    _write_metadata_parquet(
        data_dir / "metadata" / "date=2026-06-11" / "metadata.parquet",
        ts_ms,
    )

    health = HealthState(symbol_count=25)
    assert health.last_metadata_update is None

    hydrate_last_metadata_update(health, data_dir)

    assert health.last_metadata_update == "2026-06-11T00:00:05Z"
    assert health.snapshot()["last_metadata_update"] == "2026-06-11T00:00:05Z"


def test_restart_scenario_hydrates_without_new_collection(tmp_path) -> None:
    data_dir = tmp_path / "data"
    ts_ms = int(datetime(2026, 6, 11, 0, 0, 5, tzinfo=UTC).timestamp() * 1000)
    _write_metadata_parquet(
        data_dir / "metadata" / "date=2026-06-11" / "metadata.parquet",
        ts_ms,
    )

    before_restart = HealthState(symbol_count=25)
    hydrate_last_metadata_update(before_restart, data_dir)

    after_restart = HealthState(symbol_count=25)
    assert after_restart.last_metadata_update is None
    hydrate_last_metadata_update(after_restart, data_dir)

    assert after_restart.last_metadata_update == before_restart.last_metadata_update


def test_metadata_collector_skips_collection_but_hydrates_health(tmp_path) -> None:
    data_dir = tmp_path / "data"
    day = datetime.now(UTC).date().isoformat()
    ts_ms = int(datetime(2026, 6, 11, 0, 0, 5, tzinfo=UTC).timestamp() * 1000)
    _write_metadata_parquet(
        data_dir / "metadata" / f"date={day}" / "metadata.parquet",
        ts_ms,
    )

    settings = replace(
        Settings(),
        data_dir=data_dir,
        log_dir=tmp_path / "logs",
    )
    health = HealthState(symbol_count=1)
    storage = AsyncMock()
    storage.data_dir = data_dir
    client = AsyncMock()
    collector = MetadataCollector(settings, client, storage, health)

    async def scenario() -> None:
        await collector._collect_if_needed()

    asyncio.run(scenario())

    client.get_json.assert_not_called()
    storage.write.assert_not_called()
    assert health.last_metadata_update == "2026-06-11T00:00:05Z"
