from __future__ import annotations

import re
from collections.abc import Iterable

DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "ADAUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "WIFUSDT",
    "1000PEPEUSDT",
    "APTUSDT",
    "ARBUSDT",
    "SEIUSDT",
    "ATOMUSDT",
    "NEARUSDT",
    "INJUSDT",
    "FILUSDT",
    "TIAUSDT",
    "OPUSDT",
    "RENDERUSDT",
    "FETUSDT",
    "TRXUSDT",
    "LTCUSDT",
)

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,24}$")


def normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    """Normalize, validate, and de-duplicate symbols while preserving order."""
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_symbol in symbols:
        symbol = raw_symbol.strip().upper()
        if not symbol:
            continue
        if not _SYMBOL_RE.fullmatch(symbol) or not symbol.endswith("USDT"):
            raise ValueError(f"Invalid Binance USDT-M symbol: {raw_symbol!r}")
        if symbol not in seen:
            normalized.append(symbol)
            seen.add(symbol)

    if not normalized:
        raise ValueError("At least one symbol must be configured")
    return tuple(normalized)


def parse_symbols(value: str | None) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_SYMBOLS
    return normalize_symbols(value.split(","))
