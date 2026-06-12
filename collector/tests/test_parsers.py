import asyncio

from collector.config import Settings
from collector.depth_collector import parse_depth20
from collector.funding_collector import parse_funding
from collector.health import HealthState
from collector.liquidation_collector import parse_liquidation
from collector.oi_collector import parse_open_interest
from collector.trade_collector import (
    TradeCollector,
    parse_agg_trade,
    parse_rest_agg_trade,
)


def test_parse_agg_trade() -> None:
    record = parse_agg_trade(
        {
            "T": 1_700_000_000_001,
            "s": "BTCUSDT",
            "p": "65000.25",
            "q": "0.010",
            "m": True,
            "a": 12345,
        },
        1_700_000_000_010,
    )
    assert record["trade_id"] == 12345
    assert record["price"] == 65000.25
    assert record["is_buyer_maker"] is True
    assert record["is_recovered"] is False

    recovered = parse_rest_agg_trade(
        {
            "T": 1_700_000_000_001,
            "p": "65000.25",
            "q": "0.010",
            "m": True,
            "a": 12345,
        },
        "BTCUSDT",
        1_700_000_000_020,
    )
    assert recovered["is_recovered"] is True


def test_liquidation_uses_filled_quantity_and_average_price() -> None:
    record = parse_liquidation(
        {
            "E": 1_700_000_000_001,
            "o": {
                "s": "ETHUSDT",
                "S": "SELL",
                "p": "3500",
                "ap": "3498",
                "q": "2",
                "z": "1.5",
                "T": 1_700_000_000_000,
            },
        },
        1_700_000_000_010,
    )
    assert record["price"] == 3498.0
    assert record["quantity"] == 1.5
    assert record["notional"] == 5247.0


def test_depth_snapshot_is_flattened_to_twenty_levels() -> None:
    record = parse_depth20(
        {
            "E": 1_700_000_000_001,
            "T": 1_700_000_000_000,
            "s": "SOLUSDT",
            "U": 100,
            "u": 105,
            "pu": 99,
            "b": [["150.1", "10"], ["150.0", "11"]],
            "a": [["150.2", "12"]],
        },
        1_700_000_000_010,
    )
    assert record["bid_price_1"] == 150.1
    assert record["bid_qty_2"] == 11.0
    assert record["ask_price_1"] == 150.2
    assert record["ask_price_20"] is None
    assert record["final_update_id"] == 105


def test_rest_payload_parsers() -> None:
    funding = parse_funding(
        {
            "symbol": "BTCUSDT",
            "markPrice": "65000",
            "lastFundingRate": "0.0001",
            "nextFundingTime": 1_700_000_800_000,
            "time": 1_700_000_000_000,
        },
        1_700_000_000_010,
    )
    oi = parse_open_interest(
        {
            "symbol": "BTCUSDT",
            "openInterest": "12345.67",
            "time": 1_700_000_000_000,
        },
        1_700_000_000_010,
    )
    assert funding["funding_rate"] == 0.0001
    assert oi["open_interest"] == 12345.67


def test_trade_collector_recovers_id_gap() -> None:
    class FakeRestClient:
        async def get_json(self, path, params):
            assert path == "/fapi/v1/aggTrades"
            assert params["fromId"] == "11"
            return [
                {"a": 11, "p": "100", "q": "1", "m": False, "T": 1001},
                {"a": 12, "p": "101", "q": "2", "m": True, "T": 1002},
            ]

    class FakeStorage:
        def __init__(self):
            self.records = []

        async def write(self, dataset, record):
            assert dataset == "trades"
            self.records.append(record)

    async def scenario() -> None:
        storage = FakeStorage()
        health = HealthState(symbol_count=1)
        collector = TradeCollector(
            Settings(symbols=("BTCUSDT",)),
            FakeRestClient(),
            storage,
            health,
        )
        await collector._handle_message(
            "",
            {
                "T": 1000,
                "s": "BTCUSDT",
                "p": "99",
                "q": "1",
                "m": False,
                "a": 10,
            },
            2000,
        )
        await collector._handle_message(
            "",
            {
                "T": 1003,
                "s": "BTCUSDT",
                "p": "102",
                "q": "1",
                "m": False,
                "a": 13,
            },
            2003,
        )
        assert [row["trade_id"] for row in storage.records] == [10, 11, 12, 13]
        assert [row["is_recovered"] for row in storage.records] == [
            False,
            True,
            True,
            False,
        ]
        assert health.trades_received == 4

    asyncio.run(scenario())
