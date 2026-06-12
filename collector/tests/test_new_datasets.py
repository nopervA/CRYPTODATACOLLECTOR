import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow.parquet as pq

from collector.config import Settings
from collector.mark_price_collector import parse_mark_price
from collector.metadata_collector import parse_exchange_metadata
from collector.ohlcv_builder import OhlcvBuilder
from collector.storage import StorageManager
from collector.top_of_book import build_top_of_book_record


def test_parse_mark_price() -> None:
    record = parse_mark_price(
        {
            "E": 1_700_000_000_001,
            "s": "BTCUSDT",
            "p": "65000.5",
            "i": "64998.0",
            "P": "65001.0",
            "r": "0.00015",
            "T": 1_700_003_600_000,
        },
        1_700_000_000_010,
    )
    assert record["event_time"] == 1_700_000_000_001
    assert record["mark_price"] == 65000.5
    assert record["index_price"] == 64998.0
    assert record["estimated_settle_price"] == 65001.0
    assert record["predicted_funding_rate"] == 0.00015

    without_settle = parse_mark_price(
        {
            "E": 1_700_000_000_002,
            "s": "ETHUSDT",
            "p": "3500",
            "i": "3499",
            "r": "0.0001",
            "T": 1_700_003_600_000,
        },
        1_700_000_000_020,
    )
    assert without_settle["estimated_settle_price"] is None


def test_build_top_of_book_record() -> None:
    record = build_top_of_book_record(
        {
            "timestamp": 1_700_000_000_001,
            "symbol": "BTCUSDT",
            "received_at": 1_700_000_000_010,
            "bid_price_1": 65000.0,
            "bid_qty_1": 1.5,
            "ask_price_1": 65001.0,
            "ask_qty_1": 2.0,
        }
    )
    assert record["best_bid_price"] == 65000.0
    assert record["spread"] == 1.0
    assert record["mid_price"] == 65000.5
    assert abs(record["spread_bps"] - (1.0 / 65000.5 * 10_000)) < 1e-9


def test_parse_exchange_metadata() -> None:
    records = parse_exchange_metadata(
        ("BTCUSDT",),
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.10",
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.001",
                            "minQty": "0.001",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "notional": "5",
                        },
                    ],
                }
            ]
        },
        1_700_000_000_000,
    )
    assert len(records) == 1
    assert records[0]["tick_size"] == 0.10
    assert records[0]["step_size"] == 0.001
    assert records[0]["min_notional"] == 5.0
    assert records[0]["maker_fee"] is None


def test_ohlcv_builder_emits_completed_minute() -> None:
    class CollectStorage:
        def __init__(self) -> None:
            self.records: list[dict] = []

        async def write(self, dataset: str, record: dict) -> None:
            assert dataset == "ohlcv_1m"
            self.records.append(record)

    async def scenario() -> None:
        storage = CollectStorage()
        builder = OhlcvBuilder(storage)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        minute_start = (now_ms // 60_000) * 60_000

        await builder.on_trade(
            {
                "symbol": "BTCUSDT",
                "timestamp": minute_start + 1_000,
                "price": 100.0,
                "quantity": 1.0,
            }
        )
        await builder.on_trade(
            {
                "symbol": "BTCUSDT",
                "timestamp": minute_start + 2_000,
                "price": 105.0,
                "quantity": 2.0,
            }
        )
        await builder.on_trade(
            {
                "symbol": "BTCUSDT",
                "timestamp": minute_start + 60_000,
                "price": 110.0,
                "quantity": 1.0,
            }
        )
        await builder.flush()

        assert len(storage.records) == 2
        assert storage.records[0]["minute_start"] == minute_start
        assert storage.records[0]["open"] == 100.0
        assert storage.records[0]["high"] == 105.0
        assert storage.records[0]["close"] == 105.0
        assert storage.records[0]["volume"] == 3.0
        assert storage.records[0]["trade_count"] == 2
        assert storage.records[1]["minute_start"] == minute_start + 60_000
        assert storage.records[1]["open"] == 110.0

    asyncio.run(scenario())


def test_ohlcv_1m_storage_round_trip(tmp_path) -> None:
    async def scenario() -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        )
        storage = StorageManager(settings)
        await storage.start()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        minute_start = (now_ms // 60_000) * 60_000
        for offset, close in ((0, 100.0), (60_000, 110.0)):
            await storage.write(
                "ohlcv_1m",
                {
                    "symbol": "BTCUSDT",
                    "minute_start": minute_start + offset,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 1.0,
                    "trade_count": 1,
                },
            )
        await storage.close()

        day = datetime.fromtimestamp(minute_start / 1000, UTC).date().isoformat()
        output = (
            tmp_path
            / "data"
            / "ohlcv_1m"
            / "symbol=BTCUSDT"
            / f"date={day}"
            / "ohlcv_1m.parquet"
        )
        table = pq.ParquetFile(output).read()
        assert table.num_rows == 2

    asyncio.run(scenario())


def test_metadata_writes_date_partition(tmp_path) -> None:
    async def scenario() -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            symbols=("BTCUSDT", "ETHUSDT"),
        )
        storage = StorageManager(settings)
        await storage.start()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        day = datetime.fromtimestamp(now_ms / 1000, UTC).date().isoformat()
        for symbol in settings.symbols:
            await storage.write(
                "metadata",
                {
                    "symbol": symbol,
                    "timestamp": now_ms,
                    "tick_size": 0.01,
                    "step_size": 0.001,
                    "min_qty": 0.001,
                    "min_notional": 5.0,
                    "maker_fee": None,
                    "taker_fee": None,
                },
            )
        await storage.close()

        output = tmp_path / "data" / "metadata" / f"date={day}" / "metadata.parquet"
        table = pq.ParquetFile(output).read()
        assert table.num_rows == 2
        assert sorted(table.column("symbol").to_pylist()) == ["BTCUSDT", "ETHUSDT"]

    asyncio.run(scenario())
