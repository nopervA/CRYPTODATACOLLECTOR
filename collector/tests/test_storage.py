import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow.parquet as pq

from collector.config import Settings
from collector.storage import RecentKeyCache, StorageManager


def test_recent_key_cache_is_bounded_and_deduplicates() -> None:
    cache = RecentKeyCache(2)
    assert cache.add(("BTCUSDT", 1))
    assert not cache.add(("BTCUSDT", 1))
    assert cache.add(("BTCUSDT", 2))
    assert cache.add(("BTCUSDT", 3))
    assert len(cache) == 2
    assert cache.add(("BTCUSDT", 1))


def test_storage_deduplicates_and_finalizes_daily_file(tmp_path) -> None:
    async def scenario() -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        )
        storage = StorageManager(settings)
        await storage.start()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        record = {
            "timestamp": now_ms,
            "symbol": "BTCUSDT",
            "price": 65000.0,
            "quantity": 0.1,
            "is_buyer_maker": True,
            "trade_id": 42,
            "is_recovered": False,
            "received_at": now_ms + 1,
        }
        await storage.write("trades", record)
        await storage.write("trades", dict(record))
        await storage.close()

        day = datetime.fromtimestamp(now_ms / 1000, UTC).date().isoformat()
        output = (
            tmp_path
            / "data"
            / "trades"
            / "symbol=BTCUSDT"
            / f"date={day}"
            / "trades.parquet"
        )
        table = pq.ParquetFile(output).read()
        assert table.num_rows == 1
        assert table.column("trade_id").to_pylist() == [42]

    asyncio.run(scenario())


def test_storage_merges_a_same_day_restart(tmp_path) -> None:
    async def write_trade(trade_id: int) -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        )
        storage = StorageManager(settings)
        await storage.start()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        await storage.write(
            "trades",
            {
                "timestamp": now_ms,
                "symbol": "ETHUSDT",
                "price": 3500.0,
                "quantity": 0.5,
                "is_buyer_maker": False,
                "trade_id": trade_id,
                "is_recovered": False,
                "received_at": now_ms + 1,
            },
        )
        await storage.close()

    asyncio.run(write_trade(100))
    asyncio.run(write_trade(101))

    day = datetime.now(UTC).date().isoformat()
    output = (
        tmp_path
        / "data"
        / "trades"
        / "symbol=ETHUSDT"
        / f"date={day}"
        / "trades.parquet"
    )
    table = pq.ParquetFile(output).read()
    assert table.num_rows == 2
    assert table.column("trade_id").to_pylist() == [100, 101]


def test_storage_recovers_unfinished_segments_on_startup(tmp_path) -> None:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    from collector.storage import SCHEMAS

    settings = replace(
        Settings(),
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    day = datetime.fromtimestamp(now_ms / 1000, UTC).date().isoformat()
    partition_dir = (
        tmp_path / "data" / "funding" / "symbol=BTCUSDT" / f"date={day}"
    )
    partition_dir.mkdir(parents=True)
    record = {
        "timestamp": now_ms,
        "symbol": "BTCUSDT",
        "funding_rate": 0.0001,
        "next_funding_time": now_ms + 3_600_000,
        "mark_price": 65000.0,
        "received_at": now_ms + 1,
    }
    schema = SCHEMAS["funding"]
    frame = pd.DataFrame.from_records([record], columns=schema.names)
    table = pa.Table.from_pandas(
        frame, schema=schema, preserve_index=False, safe=False
    )
    pq.write_table(
        table,
        partition_dir / ".segment.recovery-test.parquet",
        compression="snappy",
    )

    async def restart_and_compact() -> None:
        storage = StorageManager(settings)
        await storage.start()
        await storage.close()

    asyncio.run(restart_and_compact())

    output = partition_dir / "funding.parquet"
    assert output.exists()
    assert not list(partition_dir.glob(".segment.*.parquet"))
    assert pq.ParquetFile(output).read().num_rows == 1
