import asyncio

from collector.depth_metrics import (
    book_imbalance,
    build_book_imbalance_record,
    build_liquidity_stress_record,
    merge_depth50,
    parse_depth_ws,
)
from collector.oi_change_tracker import OiChangeTracker
from collector.spread_state_tracker import SpreadStateTracker
from collector.taker_delta_builder import TakerDeltaBuilder


def _depth_ws_record() -> dict:
    bids = [[100.0, 1.0], [99.0, 2.0]]
    asks = [[101.0, 1.5], [102.0, 2.5]]
    payload = {
        "E": 1_700_000_000_000,
        "T": 1_700_000_000_000,
        "s": "BTCUSDT",
        "U": 1,
        "u": 2,
        "pu": 0,
        "b": bids,
        "a": asks,
    }
    return parse_depth_ws(payload, 1_700_000_000_010, levels=20)


def test_merge_depth50_uses_rest_levels_21_to_50() -> None:
    ws_record = _depth_ws_record()
    rest_payload = {
        "bids": [[float(100 - index), 1.0] for index in range(50)],
        "asks": [[float(101 + index), 1.0] for index in range(50)],
    }
    merged = merge_depth50(ws_record, rest_payload)
    assert merged["bid_price_21"] == 100.0 - 20
    assert merged["bid_price_50"] == 100.0 - 49
    assert merged["final_update_id"] == 2


def test_book_imbalance_and_liquidity_stress() -> None:
    ws_record = _depth_ws_record()
    rest_payload = {
        "bids": [[100.0 - index, 1.0] for index in range(50)],
        "asks": [[101.0 + index, 1.0] for index in range(50)],
    }
    depth50 = merge_depth50(ws_record, rest_payload)
    imbalance = build_book_imbalance_record(depth50)
    assert -1.0 <= imbalance["imbalance_5"] <= 1.0
    assert imbalance["bid_notional"] > 0.0

    stress = build_liquidity_stress_record(
        depth50,
        previous_total_notional=imbalance["bid_notional"] + imbalance["ask_notional"],
        is_wide_spread=False,
    )
    assert stress["depth_change_1m"] == 0.0
    assert book_imbalance(depth50, 5) == imbalance["imbalance_5"]


def test_oi_change_tracker_emits_deltas() -> None:
    class CollectStorage:
        def __init__(self) -> None:
            self.records: list[dict] = []

        async def write(self, dataset: str, record: dict) -> None:
            assert dataset == "oi_change"
            self.records.append(record)

    async def scenario() -> None:
        storage = CollectStorage()
        tracker = OiChangeTracker(storage)
        await tracker.on_open_interest(
            {"symbol": "ETHUSDT", "timestamp": 1_700_000_000_000, "open_interest": 1000.0}
        )
        await tracker.on_open_interest(
            {"symbol": "ETHUSDT", "timestamp": 1_700_000_060_000, "open_interest": 1100.0}
        )
        assert len(storage.records) == 2
        assert storage.records[1]["oi_delta"] == 100.0
        assert storage.records[1]["oi_delta_pct"] == 0.1

    asyncio.run(scenario())


def test_taker_delta_builder_classifies_aggressor() -> None:
    class CollectStorage:
        def __init__(self) -> None:
            self.records: list[dict] = []

        async def write(self, dataset: str, record: dict) -> None:
            assert dataset == "taker_delta_1m"
            self.records.append(record)

    async def scenario() -> None:
        storage = CollectStorage()
        builder = TakerDeltaBuilder(storage)
        minute = 1_700_000_040_000
        await builder.on_trade(
            {
                "symbol": "BTCUSDT",
                "timestamp": minute + 1_000,
                "price": 100.0,
                "quantity": 1.0,
                "is_buyer_maker": False,
            }
        )
        await builder.on_trade(
            {
                "symbol": "BTCUSDT",
                "timestamp": minute + 2_000,
                "price": 100.0,
                "quantity": 2.0,
                "is_buyer_maker": True,
            }
        )
        await builder.flush()
        assert len(storage.records) == 1
        assert storage.records[0]["buy_volume"] == 1.0
        assert storage.records[0]["sell_volume"] == 2.0
        assert storage.records[0]["delta_volume"] == -1.0

    asyncio.run(scenario())


def test_spread_state_tracker_flags_wide_spreads() -> None:
    class CollectStorage:
        def __init__(self) -> None:
            self.records: list[dict] = []

        async def write(self, dataset: str, record: dict) -> None:
            assert dataset == "spread_state"
            self.records.append(record)

    async def scenario() -> None:
        storage = CollectStorage()
        tracker = SpreadStateTracker(storage, window=5, zscore_threshold=1.0)
        base = {
            "symbol": "SOLUSDT",
            "event_time": 1_700_000_000_000,
            "received_at": 1_700_000_000_001,
            "best_bid_price": 100.0,
            "best_bid_qty": 1.0,
            "best_ask_price": 100.1,
            "best_ask_qty": 1.0,
            "spread": 0.1,
            "mid_price": 100.05,
        }
        for index in range(4):
            record = dict(base)
            record["event_time"] = base["event_time"] + index * 1_000
            record["spread_bps"] = 10.0
            await tracker.on_top_of_book(record)
        wide = dict(base)
        wide["event_time"] = base["event_time"] + 5_000
        wide["spread_bps"] = 100.0
        is_wide = await tracker.on_top_of_book(wide)
        assert is_wide is True
        assert storage.records[-1]["is_wide_spread"] is True

    asyncio.run(scenario())
