from __future__ import annotations

from typing import Any

DEPTH_LEVELS = 50


def _optional_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def parse_depth_ws(
    payload: dict[str, Any], received_at: int, levels: int = 20
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp": int(payload["E"]),
        "symbol": str(payload["s"]).upper(),
    }
    bids = payload.get("b", payload.get("bids", []))
    asks = payload.get("a", payload.get("asks", []))

    for side_name, side_levels in (("bid", bids), ("ask", asks)):
        for index in range(levels):
            price_key = f"{side_name}_price_{index + 1}"
            qty_key = f"{side_name}_qty_{index + 1}"
            if index < len(side_levels):
                record[price_key] = float(side_levels[index][0])
                record[qty_key] = float(side_levels[index][1])
            else:
                record[price_key] = None
                record[qty_key] = None

    record.update(
        {
            "first_update_id": _optional_int(payload.get("U")),
            "final_update_id": _optional_int(
                payload.get("u", payload.get("lastUpdateId"))
            ),
            "prev_final_update_id": _optional_int(payload.get("pu")),
            "transaction_time": _optional_int(payload.get("T")),
            "received_at": received_at,
        }
    )
    return record


def merge_depth50(
    ws_record: dict[str, Any], rest_payload: dict[str, Any] | None
) -> dict[str, Any]:
    """Combine WS top-20 (with update IDs) and REST levels 21-50."""
    record = {key: ws_record[key] for key in ws_record}
    for side_name in ("bid", "ask"):
        rest_side = "bids" if side_name == "bid" else "asks"
        levels = rest_payload.get(rest_side, []) if rest_payload else []
        for level in range(21, DEPTH_LEVELS + 1):
            price_key = f"{side_name}_price_{level}"
            qty_key = f"{side_name}_qty_{level}"
            rest_index = level - 1
            if rest_index < len(levels):
                record[price_key] = float(levels[rest_index][0])
                record[qty_key] = float(levels[rest_index][1])
            else:
                record[price_key] = None
                record[qty_key] = None
    return record


def parse_rest_depth(symbol: str, payload: dict[str, Any], received_at: int) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp": received_at,
        "symbol": symbol.upper(),
        "first_update_id": _optional_int(payload.get("lastUpdateId")),
        "final_update_id": _optional_int(payload.get("lastUpdateId")),
        "prev_final_update_id": None,
        "transaction_time": None,
        "received_at": received_at,
    }
    bids = payload.get("bids", [])
    asks = payload.get("asks", [])
    for side_name, side_levels in (("bid", bids), ("ask", asks)):
        for index in range(DEPTH_LEVELS):
            price_key = f"{side_name}_price_{index + 1}"
            qty_key = f"{side_name}_qty_{index + 1}"
            if index < len(side_levels):
                record[price_key] = float(side_levels[index][0])
                record[qty_key] = float(side_levels[index][1])
            else:
                record[price_key] = None
                record[qty_key] = None
    return record


def level_notional(record: dict[str, Any], side: str, levels: int) -> float:
    total = 0.0
    for level in range(1, levels + 1):
        price = record.get(f"{side}_price_{level}")
        qty = record.get(f"{side}_qty_{level}")
        if price is not None and qty is not None:
            total += float(price) * float(qty)
    return total


def book_imbalance(record: dict[str, Any], levels: int) -> float:
    bid = level_notional(record, "bid", levels)
    ask = level_notional(record, "ask", levels)
    total = bid + ask
    if total <= 0.0:
        return 0.0
    return (bid - ask) / total


def build_book_imbalance_record(depth_record: dict[str, Any]) -> dict[str, Any]:
    bid_notional = level_notional(depth_record, "bid", DEPTH_LEVELS)
    ask_notional = level_notional(depth_record, "ask", DEPTH_LEVELS)
    return {
        "timestamp": int(depth_record["timestamp"]),
        "symbol": str(depth_record["symbol"]),
        "imbalance_5": book_imbalance(depth_record, 5),
        "imbalance_10": book_imbalance(depth_record, 10),
        "imbalance_20": book_imbalance(depth_record, 20),
        "imbalance_50": book_imbalance(depth_record, DEPTH_LEVELS),
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
    }


def build_liquidity_stress_record(
    depth_record: dict[str, Any],
    *,
    previous_total_notional: float | None,
    is_wide_spread: bool,
) -> dict[str, Any]:
    bid_notional = level_notional(depth_record, "bid", DEPTH_LEVELS)
    ask_notional = level_notional(depth_record, "ask", DEPTH_LEVELS)
    total = bid_notional + ask_notional
    depth_imbalance = book_imbalance(depth_record, DEPTH_LEVELS)
    if previous_total_notional and previous_total_notional > 0.0:
        depth_change_1m = (total - previous_total_notional) / previous_total_notional
    else:
        depth_change_1m = 0.0
    is_stress_event = depth_change_1m <= -0.15 or (
        is_wide_spread and abs(depth_imbalance) >= 0.35
    )
    return {
        "timestamp": int(depth_record["timestamp"]),
        "symbol": str(depth_record["symbol"]),
        "bid_depth_notional": bid_notional,
        "ask_depth_notional": ask_notional,
        "depth_imbalance": depth_imbalance,
        "depth_change_1m": depth_change_1m,
        "is_stress_event": is_stress_event,
    }
