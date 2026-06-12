import pytest

from collector.symbols import normalize_symbols, parse_symbols


def test_symbols_are_normalized_and_deduplicated() -> None:
    assert normalize_symbols(["btcusdt", " ETHUSDT ", "BTCUSDT"]) == (
        "BTCUSDT",
        "ETHUSDT",
    )


def test_invalid_symbol_is_rejected() -> None:
    with pytest.raises(ValueError):
        normalize_symbols(["BTCUSD"])


def test_default_symbol_universe_size() -> None:
    symbols = parse_symbols(None)
    assert len(symbols) == 25
    assert "BTCUSDT" in symbols
    assert "1000PEPEUSDT" in symbols
    assert "PEPEUSDT" not in symbols


def test_default_symbols_validate_against_live_binance() -> None:
    """Ensure DEFAULT_SYMBOLS matches active Binance USDT-M perpetuals."""
    import json
    import urllib.request

    symbols = parse_symbols(None)
    with urllib.request.urlopen(
        "https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=30
    ) as response:
        payload = json.load(response)
    active = {
        item["symbol"]
        for item in payload.get("symbols", [])
        if item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
    }
    invalid = sorted(set(symbols) - active)
    assert not invalid, f"Invalid default symbols: {', '.join(invalid)}"
