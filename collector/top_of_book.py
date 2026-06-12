from __future__ import annotations

from typing import Any


def build_top_of_book_record(depth_record: dict[str, Any]) -> dict[str, Any]:
    best_bid_price = float(depth_record["bid_price_1"])
    best_bid_qty = float(depth_record["bid_qty_1"])
    best_ask_price = float(depth_record["ask_price_1"])
    best_ask_qty = float(depth_record["ask_qty_1"])
    spread = best_ask_price - best_bid_price
    mid_price = (best_ask_price + best_bid_price) / 2.0
    spread_bps = (spread / mid_price) * 10_000.0 if mid_price else 0.0

    return {
        "event_time": int(depth_record["timestamp"]),
        "symbol": str(depth_record["symbol"]),
        "received_at": int(depth_record["received_at"]),
        "best_bid_price": best_bid_price,
        "best_bid_qty": best_bid_qty,
        "best_ask_price": best_ask_price,
        "best_ask_qty": best_ask_qty,
        "spread": spread,
        "spread_bps": spread_bps,
        "mid_price": mid_price,
    }
