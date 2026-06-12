# Binance Futures Research Data Collector

Long-running asynchronous collector for Binance USDT-M Futures market
microstructure data. It records aggregate trades, liquidation snapshots,
funding state, open interest, one-second top-20 order-book snapshots, mark
price, top-of-book metrics, one-minute OHLCV bars, and daily exchange
metadata in daily Snappy-compressed Parquet files.

This is research infrastructure. It does not place or manage orders.

## Collected Data

Default symbols (25, override with `COLLECTOR_SYMBOLS`):

```text
BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, SUIUSDT,
ADAUSDT, LINKUSDT, AVAXUSDT, WIFUSDT, 1000PEPEUSDT, APTUSDT, ARBUSDT,
SEIUSDT, ATOMUSDT, NEARUSDT, INJUSDT, FILUSDT, TIAUSDT, OPUSDT,
RENDERUSDT, FETUSDT, TRXUSDT, LTCUSDT
```

Sources:

| Dataset | Binance source | Frequency |
|---|---|---:|
| Trades | `/market`: `<symbol>@aggTrade` | Event driven |
| Liquidations | `/market`: `<symbol>@forceOrder` | Event driven |
| Funding | `/fapi/v1/premiumIndex` | 5 minutes |
| Open interest | `/fapi/v1/openInterest` | 1 minute |
| Depth50 | `/public`: `<symbol>@depth20@500ms` + REST depth limit 50 | 1 second |
| Mark price | `/market`: `<symbol>@markPrice@1s` | 1 second |
| Top of book | Derived from depth50 | 1 second |
| OHLCV 1m | Derived from aggregate trades | 1 minute |
| OI change | Derived from open interest | 1 minute |
| Taker delta 1m | Derived from aggregate trades | 1 minute |
| Book imbalance | Derived from depth50 | 1 second |
| Spread state | Derived from top of book | 1 second |
| Liquidity stress | Derived from depth50 | 1 second |
| Funding event | Derived from funding snapshots | 5 minutes |
| Exchange metadata | `/fapi/v1/exchangeInfo` | Once per UTC day |

Legacy `depth20` partitions remain readable; new collection writes `depth50`.
Binance partial-book WebSocket feeds expose at most 20 levels. Levels 21–50
are refreshed from REST (`/fapi/v1/depth?limit=50`) every
`DEPTH50_REST_REFRESH_SECONDS` (default 5 s) and merged into each 1 Hz sample.

### Research value of derived datasets

| Dataset | Why it matters |
|---|---|
| **OI change** | Rolling OI deltas surface position build-up, flush events, and deleveraging ahead of liquidation cascades. |
| **Taker delta 1m** | Minute-level buy/sell aggressor imbalance from aggTrade flags supports flow-driven OI and cascade timing studies. |
| **Book imbalance** | Multi-level (5/10/20/50) notional imbalance quantifies order-book stress and directional liquidity. |
| **Spread state** | Rolling spread z-scores flag wide-spread liquidity regimes for execution simulation. |
| **Liquidity stress** | Depth notional drops and imbalance spikes mark cascade/onset and post-liquidation recovery windows. |
| **Depth50** | Top-50 snapshots support deeper book stress metrics without full L2 reconstruction. |
| **Mark price** | Mark/index divergence and 1 s predicted funding support basis and funding-carry research. |
| **Top of book** | Compact 1 Hz spread/mid series for microstructure and slippage modelling. |
| **OHLCV 1m** | Fast volume-burst detection without scanning tick data. |
| **Metadata** | Tick/lot/min-notional for cross-sectional altcoin comparability and execution constraints. |
| **Funding event** | Pre/post funding settlement windows, minutes-to-funding, and funding direction for carry, positioning, and mean-reversion studies without recomputing from raw funding polls. |

All `timestamp`, `event_time`, `minute_start`, `received_at`, funding-time,
transaction-time, and update-ID time fields are stored as Unix milliseconds. Prices, quantities, rates, and
open interest are stored as `float64`.

`received_at` is the local wall-clock time immediately after the message or
REST response is received. Depth rows also retain Binance update IDs and
transaction time, which makes feed-gap and latency checks possible.
Trade rows contain `is_recovered`; it is `true` when a WebSocket ID gap was
successfully filled from Binance's aggregate-trades REST endpoint.

Binance's current routed WebSocket API no longer documents the former
`@depth20@1000ms` suffix. The collector subscribes to the supported 500 ms
top-20 feed and retains the first complete snapshot observed in each exchange
event-time second. This preserves the requested one-row-per-second storage
rate without relying on a retired stream variant.

## Important Liquidation Limitation

Binance's public force-order stream is a liquidation *snapshot* stream. For
each symbol, it publishes only the latest liquidation order within each
1,000 ms window. No public collector can recover additional liquidations
suppressed inside that window. In this project, "every liquidation event"
means every event Binance actually publishes on the configured stream.

## Installation

Python 3.11 or 3.12 is recommended.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Start

From the project root:

```powershell
python main.py
```

Stop with `Ctrl+C`. The service drains its queues, writes outstanding rows,
and finalizes current Parquet partitions before exiting.

## Production deployment (GCP / Debian 12)

Systemd service, automatic restart, log rotation, health checks, GCS backup
hooks, and recovery procedures live under `deployment/`.

```bash
sudo git clone <repo-url> /opt/binance-futures-collector
cd /opt/binance-futures-collector
sudo python3 -m venv .venv && sudo .venv/bin/pip install -r requirements.txt
sudo chmod +x deployment/scripts/*.sh
sudo systemctl enable /opt/binance-futures-collector/deployment/systemd/binance-futures-collector.service
sudo systemctl start binance-futures-collector
```

Full instructions: [deployment/documentation/INSTALL.md](deployment/documentation/INSTALL.md)

## Configuration

Configuration uses environment variables.

| Variable | Default |
|---|---|
| `COLLECTOR_SYMBOLS` | Seven symbols listed above |
| `COLLECTOR_DATA_DIR` | `data` |
| `COLLECTOR_LOG_DIR` | `logs` |
| `BINANCE_WS_BASE_URL` | `wss://fstream.binance.com` |
| `BINANCE_REST_BASE_URL` | `https://fapi.binance.com` |
| `FUNDING_INTERVAL_SECONDS` | `300` |
| `OI_INTERVAL_SECONDS` | `60` |
| `HEALTH_HOST` | `127.0.0.1` |
| `HEALTH_PORT` | `8080` |
| `LOG_LEVEL` | `INFO` |
| `TRADE_QUEUE_SIZE` | `50000` |
| `LIQUIDATION_QUEUE_SIZE` | `10000` |
| `FUNDING_QUEUE_SIZE` | `2000` |
| `OI_QUEUE_SIZE` | `5000` |
| `DEPTH_QUEUE_SIZE` | `20000` |
| `DEPTH50_QUEUE_SIZE` | `10000` |
| `DEPTH50_REST_REFRESH_SECONDS` | `5` |
| `MARK_PRICE_QUEUE_SIZE` | `5000` |
| `TOP_OF_BOOK_QUEUE_SIZE` | `10000` |
| `OHLCV_QUEUE_SIZE` | `2000` |
| `METADATA_QUEUE_SIZE` | `500` |
| `OI_CHANGE_QUEUE_SIZE` | `2000` |
| `TAKER_DELTA_QUEUE_SIZE` | `2000` |
| `BOOK_IMBALANCE_QUEUE_SIZE` | `10000` |
| `SPREAD_STATE_QUEUE_SIZE` | `10000` |
| `LIQUIDITY_STRESS_QUEUE_SIZE` | `10000` |
| `FUNDING_EVENT_QUEUE_SIZE` | `2000` |
| `FUNDING_NEUTRAL_THRESHOLD` | `0.00001` |
| `FUNDING_PERIOD_HOURS` | `8` |
| `TELEGRAM_BOT_TOKEN` | unset (alerts disabled) |
| `TELEGRAM_CHAT_ID` | unset |
| `TELEGRAM_RATE_LIMIT_SECONDS` | `900` |
| `DEDUP_CACHE_SIZE` | `500000` |

Example:

```powershell
$env:COLLECTOR_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT"
$env:COLLECTOR_DATA_DIR = "D:\binance-data"
python main.py
```

The queue limits impose backpressure rather than allowing unbounded memory
growth. Queue sizes are logged once per minute. Sustained growth indicates
that the disk cannot keep up.

## Storage Layout

```text
data/
  trades/symbol=BTCUSDT/date=2026-06-11/trades.parquet
  liquidations/symbol=BTCUSDT/date=2026-06-11/liquidations.parquet
  funding/symbol=BTCUSDT/date=2026-06-11/funding.parquet
  oi/symbol=BTCUSDT/date=2026-06-11/oi.parquet
  depth50/symbol=BTCUSDT/date=2026-06-11/depth50.parquet
  oi_change/symbol=BTCUSDT/date=2026-06-11/oi_change.parquet
  taker_delta_1m/symbol=BTCUSDT/date=2026-06-11/taker_delta_1m.parquet
  book_imbalance/symbol=BTCUSDT/date=2026-06-11/book_imbalance.parquet
  spread_state/symbol=BTCUSDT/date=2026-06-11/spread_state.parquet
  liquidity_stress/symbol=BTCUSDT/date=2026-06-11/liquidity_stress.parquet
  funding_event/symbol=BTCUSDT/date=2026-06-11/funding_event.parquet
  mark_price/symbol=BTCUSDT/date=2026-06-11/mark_price.parquet
  top_of_book/symbol=BTCUSDT/date=2026-06-11/top_of_book.parquet
  ohlcv_1m/symbol=BTCUSDT/date=2026-06-11/ohlcv_1m.parquet
  metadata/date=2026-06-11/metadata.parquet
```

During the active UTC day, the collector writes small, complete hidden
Parquet segments in each partition directory. At UTC rotation it merges them
into one compact `{dataset}.parquet` file using an atomic replacement. This design
has three useful properties:

1. A crash cannot invalidate an entire day's open Parquet writer.
2. Restart recovery finds and finalizes prior-day segments.
3. Compaction uses bounded record batches instead of loading a full day into
   memory.

On a clean shutdown, the current day is finalized too. If collection resumes
on the same day, new segments are merged into the existing daily file at the
next rotation or shutdown.

Duplicate suppression uses bounded exact key caches:

- Trades: `(symbol, trade_id)`
- Liquidations: exchange/order time and event contents
- Funding and OI: `(symbol, timestamp)`
- Depth50: `(symbol, final_update_id)`
- Mark price and top of book: `(symbol, event_time)`
- OHLCV 1m: `(symbol, minute_start)`
- OI change / book imbalance / spread state / liquidity stress / funding event: `(symbol, timestamp)`
- Taker delta 1m: `(symbol, minute)`
- Metadata: `(symbol, timestamp)`

After a WebSocket reconnect, aggregate-trade IDs are checked for continuity.
Missing IDs are fetched from `/fapi/v1/aggTrades` before live ingestion
continues. Binance limits that REST history to the most recent 24 hours, so a
longer outage is explicitly logged as unrecoverable. Depth update-chain
breaks are also logged, but past depth and liquidation snapshots cannot be
backfilled from Binance's public APIs.

## Health Endpoint

```text
GET http://localhost:8080/status
```

Example response:

```json
{
  "uptime_hours": 123.4,
  "symbols": 25,
  "trades_received": 1234567,
  "liquidations_received": 1234,
  "depth_snapshots_received": 987654,
  "mark_price_updates_received": 987654,
  "top_of_book_updates_received": 987654,
  "last_funding_update": "2026-06-11T12:00:00.000000Z",
  "last_oi_update": "2026-06-11T12:00:00.000000Z",
  "last_metadata_update": "2026-06-11T00:00:05.000000Z"
}
```

Logs are written to `logs/collector.log` and rotate daily in UTC with 30
backups.

## Estimated Storage (25 symbols, 3 months)

Planning target: **250–450 GB** over 90 days on a **500 GB** VPS.
Measure the first 72 hours and annualize per dataset.

| Dataset | Approximate monthly size (25 symbols) |
|---|---:|
| Trades | 30–120 GB |
| Depth50 at 1 Hz | 35–65 GB |
| Mark price + top of book | 2–6 GB |
| Book imbalance + spread + liquidity stress | 3–8 GB |
| Taker delta + OHLCV + OI change | 0.5–2 GB |
| Liquidations + funding + OI + metadata | under 0.5 GB |
| Funding event (derived) | under 0.05 GB |
| **Total typical** | **75–200 GB/month** |
| **3-month typical** | **225–450 GB** |

High-volatility months with heavy altcoin trade flow can push trades and
depth50 toward the upper bound. `DEPTH50_REST_REFRESH_SECONDS` and buffer
flush thresholds trade REST load and segment count against depth fidelity.

### Resource estimates (25 symbols)

| Resource | Estimate |
|---|---|
| CPU | 2–4 cores sustained (async I/O bound; peaks during compaction) |
| RAM | 2–4 GB (queues, dedup caches, depth REST cache, rolling windows) |
| Disk write | ~3–15 MB/s typical; higher during volatile sessions |
| Network | ~5–20 Mbit/s (WebSocket + REST depth refresh) |

## Historical Coverage

The system creates a high-quality historical archive from the moment it is
started. Binance's public APIs cannot backfill full top-20 snapshots or the
complete public liquidation stream. Complete pre-start microstructure history
requires a specialist historical-data vendor. Funding and aggregate trades
can be backfilled separately, but backfill is intentionally outside this
live collector so it cannot interfere with ingestion.

## Tests

```powershell
pytest
```

Tests cover source parsing, symbol validation, bounded deduplication, Parquet
writing, daily-file compaction, derived datasets (including funding event
settlement windows), and mark-price, top-of-book, OHLCV, and metadata datasets.

## Project Structure

```text
collector/
  __init__.py
  config.py
  symbols.py
  storage.py
  websocket_manager.py
  rest_client.py
  health.py
  funding_collector.py
  oi_collector.py
  trade_collector.py
  liquidation_collector.py
  depth_collector.py
  depth_metrics.py
  oi_change_tracker.py
  taker_delta_builder.py
  spread_state_tracker.py
  funding_event_tracker.py
  telegram_alerts.py
  mark_price_collector.py
  metadata_collector.py
  ohlcv_builder.py
  top_of_book.py
  main.py
  tests/
main.py
requirements.txt
deployment/
pytest.ini
```
