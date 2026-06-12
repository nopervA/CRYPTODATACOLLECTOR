from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from collections import deque
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collector.config import Settings
from collector.depth_metrics import DEPTH_LEVELS
from collector.runtime_metrics import RuntimeMetrics

logger = logging.getLogger(__name__)

DATASET_NAMES = (
    "trades",
    "liquidations",
    "funding",
    "open_interest",
    "depth20",
    "depth50",
    "mark_price",
    "top_of_book",
    "ohlcv_1m",
    "metadata",
    "oi_change",
    "taker_delta_1m",
    "book_imbalance",
    "spread_state",
    "liquidity_stress",
    "funding_event",
)
_STOP = object()

# Internal dataset name -> (directory name, compacted filename)
DATASET_LAYOUT: dict[str, tuple[str, str]] = {
    "trades": ("trades", "trades.parquet"),
    "liquidations": ("liquidations", "liquidations.parquet"),
    "funding": ("funding", "funding.parquet"),
    "open_interest": ("oi", "oi.parquet"),
    "depth20": ("depth", "depth.parquet"),
    "depth50": ("depth50", "depth50.parquet"),
    "mark_price": ("mark_price", "mark_price.parquet"),
    "top_of_book": ("top_of_book", "top_of_book.parquet"),
    "ohlcv_1m": ("ohlcv_1m", "ohlcv_1m.parquet"),
    "metadata": ("metadata", "metadata.parquet"),
    "oi_change": ("oi_change", "oi_change.parquet"),
    "taker_delta_1m": ("taker_delta_1m", "taker_delta_1m.parquet"),
    "book_imbalance": ("book_imbalance", "book_imbalance.parquet"),
    "spread_state": ("spread_state", "spread_state.parquet"),
    "liquidity_stress": ("liquidity_stress", "liquidity_stress.parquet"),
    "funding_event": ("funding_event", "funding_event.parquet"),
}

_SEGMENT_RE = re.compile(r"^\.segment\..+\.parquet$")
_SYMBOL_DIR_RE = re.compile(r"^symbol=(?P<symbol>.+)$")
_DATE_DIR_RE = re.compile(r"^date=(?P<day>\d{4}-\d{2}-\d{2})$")


def _depth_fields(levels: int) -> list[pa.Field]:
    fields: list[pa.Field] = [
        pa.field("timestamp", pa.int64(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
    ]
    for side in ("bid", "ask"):
        for level in range(1, levels + 1):
            fields.append(pa.field(f"{side}_price_{level}", pa.float64()))
            fields.append(pa.field(f"{side}_qty_{level}", pa.float64()))
    fields.extend(
        [
            pa.field("first_update_id", pa.int64()),
            pa.field("final_update_id", pa.int64()),
            pa.field("prev_final_update_id", pa.int64()),
            pa.field("transaction_time", pa.int64()),
            pa.field("received_at", pa.int64(), nullable=False),
        ]
    )
    return fields


def _depth_fields_legacy() -> list[pa.Field]:
    return _depth_fields(20)


SCHEMAS: dict[str, pa.Schema] = {
    "trades": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            pa.field("quantity", pa.float64(), nullable=False),
            pa.field("is_buyer_maker", pa.bool_(), nullable=False),
            pa.field("trade_id", pa.int64(), nullable=False),
            pa.field("is_recovered", pa.bool_(), nullable=False),
            pa.field("received_at", pa.int64(), nullable=False),
        ]
    ),
    "liquidations": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            pa.field("quantity", pa.float64(), nullable=False),
            pa.field("notional", pa.float64(), nullable=False),
            pa.field("order_timestamp", pa.int64()),
            pa.field("received_at", pa.int64(), nullable=False),
        ]
    ),
    "funding": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("funding_rate", pa.float64(), nullable=False),
            pa.field("next_funding_time", pa.int64(), nullable=False),
            pa.field("mark_price", pa.float64(), nullable=False),
            pa.field("received_at", pa.int64(), nullable=False),
        ]
    ),
    "open_interest": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("open_interest", pa.float64(), nullable=False),
            pa.field("received_at", pa.int64(), nullable=False),
        ]
    ),
    "depth20": pa.schema(_depth_fields_legacy()),
    "depth50": pa.schema(_depth_fields(DEPTH_LEVELS)),
    "mark_price": pa.schema(
        [
            pa.field("event_time", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("received_at", pa.int64(), nullable=False),
            pa.field("mark_price", pa.float64(), nullable=False),
            pa.field("index_price", pa.float64(), nullable=False),
            pa.field("estimated_settle_price", pa.float64()),
            pa.field("predicted_funding_rate", pa.float64(), nullable=False),
            pa.field("next_funding_time", pa.int64(), nullable=False),
        ]
    ),
    "top_of_book": pa.schema(
        [
            pa.field("event_time", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("received_at", pa.int64(), nullable=False),
            pa.field("best_bid_price", pa.float64(), nullable=False),
            pa.field("best_bid_qty", pa.float64(), nullable=False),
            pa.field("best_ask_price", pa.float64(), nullable=False),
            pa.field("best_ask_qty", pa.float64(), nullable=False),
            pa.field("spread", pa.float64(), nullable=False),
            pa.field("spread_bps", pa.float64(), nullable=False),
            pa.field("mid_price", pa.float64(), nullable=False),
        ]
    ),
    "ohlcv_1m": pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("minute_start", pa.int64(), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("trade_count", pa.int64(), nullable=False),
        ]
    ),
    "metadata": pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("tick_size", pa.float64(), nullable=False),
            pa.field("step_size", pa.float64(), nullable=False),
            pa.field("min_qty", pa.float64(), nullable=False),
            pa.field("min_notional", pa.float64(), nullable=False),
            pa.field("maker_fee", pa.float64()),
            pa.field("taker_fee", pa.float64()),
        ]
    ),
    "oi_change": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("oi", pa.float64(), nullable=False),
            pa.field("oi_delta", pa.float64(), nullable=False),
            pa.field("oi_delta_pct", pa.float64(), nullable=False),
            pa.field("rolling_5m_delta", pa.float64(), nullable=False),
            pa.field("rolling_15m_delta", pa.float64(), nullable=False),
            pa.field("rolling_60m_delta", pa.float64(), nullable=False),
        ]
    ),
    "taker_delta_1m": pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("minute", pa.int64(), nullable=False),
            pa.field("buy_volume", pa.float64(), nullable=False),
            pa.field("sell_volume", pa.float64(), nullable=False),
            pa.field("delta_volume", pa.float64(), nullable=False),
            pa.field("delta_ratio", pa.float64(), nullable=False),
            pa.field("trade_count", pa.int64(), nullable=False),
        ]
    ),
    "book_imbalance": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("imbalance_5", pa.float64(), nullable=False),
            pa.field("imbalance_10", pa.float64(), nullable=False),
            pa.field("imbalance_20", pa.float64(), nullable=False),
            pa.field("imbalance_50", pa.float64(), nullable=False),
            pa.field("bid_notional", pa.float64(), nullable=False),
            pa.field("ask_notional", pa.float64(), nullable=False),
        ]
    ),
    "spread_state": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("spread_bps", pa.float64(), nullable=False),
            pa.field("spread_zscore", pa.float64(), nullable=False),
            pa.field("is_wide_spread", pa.bool_(), nullable=False),
            pa.field("rolling_spread_mean", pa.float64(), nullable=False),
            pa.field("rolling_spread_std", pa.float64(), nullable=False),
        ]
    ),
    "liquidity_stress": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("bid_depth_notional", pa.float64(), nullable=False),
            pa.field("ask_depth_notional", pa.float64(), nullable=False),
            pa.field("depth_imbalance", pa.float64(), nullable=False),
            pa.field("depth_change_1m", pa.float64(), nullable=False),
            pa.field("is_stress_event", pa.bool_(), nullable=False),
        ]
    ),
    "funding_event": pa.schema(
        [
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("predicted_funding_rate", pa.float64(), nullable=False),
            pa.field("next_funding_time", pa.int64(), nullable=False),
            pa.field("minutes_to_funding", pa.float64(), nullable=False),
            pa.field("minutes_since_previous_funding", pa.float64(), nullable=False),
            pa.field("funding_window", pa.string(), nullable=False),
            pa.field("is_pre_funding_60m", pa.bool_(), nullable=False),
            pa.field("is_pre_funding_30m", pa.bool_(), nullable=False),
            pa.field("is_pre_funding_15m", pa.bool_(), nullable=False),
            pa.field("is_pre_funding_5m", pa.bool_(), nullable=False),
            pa.field("is_post_funding_5m", pa.bool_(), nullable=False),
            pa.field("is_post_funding_15m", pa.bool_(), nullable=False),
            pa.field("is_post_funding_30m", pa.bool_(), nullable=False),
            pa.field("is_post_funding_60m", pa.bool_(), nullable=False),
            pa.field("funding_direction", pa.int8(), nullable=False),
        ]
    ),
}


def _trade_key(record: dict[str, Any]) -> Hashable:
    return record["symbol"], record["trade_id"]


def _liquidation_key(record: dict[str, Any]) -> Hashable:
    return (
        record["symbol"],
        record["timestamp"],
        record["order_timestamp"],
        record["side"],
        record["price"],
        record["quantity"],
    )


def _timestamp_key(record: dict[str, Any]) -> Hashable:
    return record["symbol"], record["timestamp"]


def _depth_key(record: dict[str, Any]) -> Hashable:
    update_id = record.get("final_update_id")
    if update_id is not None:
        return record["symbol"], update_id
    return record["symbol"], record["timestamp"]


def _event_time_key(record: dict[str, Any]) -> Hashable:
    return record["symbol"], record["event_time"]


def _minute_start_key(record: dict[str, Any]) -> Hashable:
    return record["symbol"], record["minute_start"]


def _minute_key(record: dict[str, Any]) -> Hashable:
    return record["symbol"], record["minute"]


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    schema: pa.Schema
    dedup_key: Callable[[dict[str, Any]], Hashable]
    flush_rows: int
    flush_seconds: float
    timestamp_field: str = "timestamp"
    partition_by_symbol: bool = True


SPECS: dict[str, DatasetSpec] = {
    "trades": DatasetSpec(SCHEMAS["trades"], _trade_key, 10_000, 60.0),
    "liquidations": DatasetSpec(
        SCHEMAS["liquidations"], _liquidation_key, 500, 60.0
    ),
    "funding": DatasetSpec(SCHEMAS["funding"], _timestamp_key, 500, 60.0),
    "open_interest": DatasetSpec(
        SCHEMAS["open_interest"], _timestamp_key, 1_000, 60.0
    ),
    "depth20": DatasetSpec(SCHEMAS["depth20"], _depth_key, 5_000, 60.0),
    "depth50": DatasetSpec(SCHEMAS["depth50"], _depth_key, 2_000, 120.0),
    "mark_price": DatasetSpec(
        SCHEMAS["mark_price"],
        _event_time_key,
        500,
        60.0,
        timestamp_field="event_time",
    ),
    "top_of_book": DatasetSpec(
        SCHEMAS["top_of_book"],
        _event_time_key,
        5_000,
        60.0,
        timestamp_field="event_time",
    ),
    "ohlcv_1m": DatasetSpec(
        SCHEMAS["ohlcv_1m"],
        _minute_start_key,
        500,
        60.0,
        timestamp_field="minute_start",
    ),
    "metadata": DatasetSpec(
        SCHEMAS["metadata"],
        _timestamp_key,
        100,
        60.0,
        partition_by_symbol=False,
    ),
    "oi_change": DatasetSpec(SCHEMAS["oi_change"], _timestamp_key, 500, 60.0),
    "taker_delta_1m": DatasetSpec(
        SCHEMAS["taker_delta_1m"],
        _minute_key,
        500,
        60.0,
        timestamp_field="minute",
    ),
    "book_imbalance": DatasetSpec(
        SCHEMAS["book_imbalance"], _timestamp_key, 5_000, 60.0
    ),
    "spread_state": DatasetSpec(
        SCHEMAS["spread_state"], _timestamp_key, 5_000, 60.0
    ),
    "liquidity_stress": DatasetSpec(
        SCHEMAS["liquidity_stress"], _timestamp_key, 5_000, 60.0
    ),
    "funding_event": DatasetSpec(
        SCHEMAS["funding_event"], _timestamp_key, 500, 60.0
    ),
}


class RecentKeyCache:
    """Bounded exact cache for duplicate suppression without memory growth."""

    def __init__(self, max_size: int) -> None:
        self._max_size = max_size
        self._keys: set[Hashable] = set()
        self._order: deque[Hashable] = deque()

    def add(self, key: Hashable) -> bool:
        if key in self._keys:
            return False
        self._keys.add(key)
        self._order.append(key)
        if len(self._order) > self._max_size:
            expired = self._order.popleft()
            self._keys.remove(expired)
        return True

    def __len__(self) -> int:
        return len(self._order)


@dataclass(slots=True)
class PartitionBuffer:
    rows: list[dict[str, Any]] = field(default_factory=list)
    first_row_monotonic: float = 0.0

    def append(self, record: dict[str, Any]) -> None:
        if not self.rows:
            self.first_row_monotonic = time.monotonic()
        self.rows.append(record)


def _parquet_row_count(path: Path) -> int:
    with path.open("rb") as input_file:
        return pq.ParquetFile(input_file).metadata.num_rows


class StorageManager:
    """Bounded asynchronous Parquet storage with UTC daily compaction."""

    def __init__(
        self,
        settings: Settings,
        runtime_metrics: RuntimeMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._runtime_metrics = runtime_metrics
        queue_sizes = {
            "trades": settings.trade_queue_size,
            "liquidations": settings.liquidation_queue_size,
            "funding": settings.funding_queue_size,
            "open_interest": settings.oi_queue_size,
            "depth20": settings.depth_queue_size,
            "depth50": settings.depth50_queue_size,
            "mark_price": settings.mark_price_queue_size,
            "top_of_book": settings.top_of_book_queue_size,
            "ohlcv_1m": settings.ohlcv_queue_size,
            "metadata": settings.metadata_queue_size,
            "oi_change": settings.oi_change_queue_size,
            "taker_delta_1m": settings.taker_delta_queue_size,
            "book_imbalance": settings.book_imbalance_queue_size,
            "spread_state": settings.spread_state_queue_size,
            "liquidity_stress": settings.liquidity_stress_queue_size,
            "funding_event": settings.funding_event_queue_size,
        }
        self._queues: dict[str, asyncio.Queue[Any]] = {
            name: asyncio.Queue(maxsize=queue_sizes[name])
            for name in DATASET_NAMES
        }
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._finalizer_task: asyncio.Task[None] | None = None
        self._finalize_queue: asyncio.Queue[Any] = asyncio.Queue()
        self._scheduled_finalizations: set[tuple[str, str, str]] = set()
        self._finalizer_errors: list[str] = []
        self._started = False
        self._closed = False

    async def start(self) -> None:
        if self._started:
            return
        from collector.storage_layout import (
            WRITE_DATASET_NAMES,
            audit_data_layout,
            format_layout_summary,
            migrate_legacy_layout,
        )

        self._settings.data_dir.mkdir(parents=True, exist_ok=True)
        for dataset in WRITE_DATASET_NAMES:
            directory_name, _ = DATASET_LAYOUT[dataset]
            (self._settings.data_dir / directory_name).mkdir(parents=True, exist_ok=True)

        migration = await asyncio.to_thread(
            migrate_legacy_layout, self._settings.data_dir
        )
        audit = await asyncio.to_thread(
            audit_data_layout, self._settings.data_dir
        )
        logger.info(format_layout_summary(audit, migration))
        for path in migration.quarantined_paths:
            logger.warning("Quarantined conflicting legacy file: %s", path)
        for risk in audit.duplicate_risks:
            logger.warning("Legacy/hive duplicate risk: %s", risk)
        for note in audit.inconsistencies:
            logger.warning("Storage layout inconsistency: %s", note)

        self._finalizer_task = asyncio.create_task(
            self._finalizer_loop(), name="parquet-finalizer"
        )
        self._worker_tasks = [
            asyncio.create_task(
                self._worker(dataset), name=f"storage-{dataset}"
            )
            for dataset in WRITE_DATASET_NAMES
        ]
        self._started = True
        await self._recover_segments()

    async def write(self, dataset: str, record: dict[str, Any]) -> None:
        if not self._started or self._closed:
            raise RuntimeError("Storage manager is not accepting records")
        if dataset == "depth20":
            raise ValueError(
                "depth20 is read-only legacy storage; writes use depth50"
            )
        try:
            queue = self._queues[dataset]
        except KeyError as exc:
            raise ValueError(f"Unknown dataset: {dataset}") from exc
        await queue.put(record)

    def queue_sizes(self) -> dict[str, int]:
        return {name: queue.qsize() for name, queue in self._queues.items()}

    async def wait_for_dataset(self, dataset: str) -> None:
        """Block until all queued records for a dataset have been processed."""
        await self._queues[dataset].join()

    @property
    def data_dir(self) -> Path:
        return self._settings.data_dir

    @property
    def integrity_error_count(self) -> int:
        return len(self._finalizer_errors)

    @property
    def background_tasks(self) -> tuple[asyncio.Task[None], ...]:
        tasks = list(self._worker_tasks)
        if self._finalizer_task is not None:
            tasks.append(self._finalizer_task)
        return tuple(tasks)

    async def close(self) -> None:
        if not self._started or self._closed:
            return
        self._closed = True

        failed_workers = [
            task
            for task in self._worker_tasks
            if task.done() and not task.cancelled() and task.exception() is not None
        ]
        if failed_workers:
            for task in self._worker_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._finalizer_errors.extend(
                f"{task.get_name()}: {task.exception()}"
                for task in failed_workers
            )
        else:
            for queue in self._queues.values():
                await queue.join()
            for queue in self._queues.values():
                await queue.put(_STOP)
            await asyncio.gather(*self._worker_tasks)

        if self._finalizer_task is not None and not self._finalizer_task.done():
            await self._finalize_queue.join()
            await self._finalize_queue.put(_STOP)
            await self._finalizer_task
        elif self._finalizer_task is not None and not self._finalizer_task.cancelled():
            exception = self._finalizer_task.exception()
            if exception is not None:
                self._finalizer_errors.append(f"parquet-finalizer: {exception}")

        if self._finalizer_errors:
            raise RuntimeError(
                "One or more Parquet partitions could not be finalized: "
                + "; ".join(self._finalizer_errors)
            )

    async def _worker(self, dataset: str) -> None:
        queue = self._queues[dataset]
        spec = SPECS[dataset]
        cache = RecentKeyCache(self._settings.dedup_cache_size)
        buffers: dict[tuple[str, str], PartitionBuffer] = {}

        while True:
            item: Any = None
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

            if item is _STOP:
                queue.task_done()
                await self._flush_all(dataset, spec, buffers, finalize=True)
                return

            if item is not None:
                try:
                    key = spec.dedup_key(item)
                    if cache.add(key):
                        partition = self._partition_for_record(dataset, spec, item)
                        buffer = buffers.setdefault(partition, PartitionBuffer())
                        buffer.append(item)
                        if len(buffer.rows) >= spec.flush_rows:
                            await self._flush_partition(
                                dataset, partition, spec, buffer
                            )
                except Exception:
                    logger.exception(
                        "Failed to process storage record",
                        extra={"dataset": dataset},
                    )
                finally:
                    queue.task_done()

            await self._maintenance(dataset, spec, buffers)

    async def _maintenance(
        self,
        dataset: str,
        spec: DatasetSpec,
        buffers: dict[tuple[str, str], PartitionBuffer],
    ) -> None:
        now_monotonic = time.monotonic()
        today = datetime.now(UTC).date().isoformat()

        for partition, buffer in list(buffers.items()):
            if (
                buffer.rows
                and now_monotonic - buffer.first_row_monotonic
                >= spec.flush_seconds
            ):
                await self._flush_partition(dataset, partition, spec, buffer)

            if partition[1] < today:
                if buffer.rows:
                    await self._flush_partition(dataset, partition, spec, buffer)
                del buffers[partition]
                await self._schedule_finalization(
                    dataset, partition[0], partition[1]
                )

    async def _flush_all(
        self,
        dataset: str,
        spec: DatasetSpec,
        buffers: dict[tuple[str, str], PartitionBuffer],
        finalize: bool,
    ) -> None:
        for partition, buffer in list(buffers.items()):
            if buffer.rows:
                await self._flush_partition(dataset, partition, spec, buffer)
            if finalize:
                await self._schedule_finalization(
                    dataset, partition[0], partition[1]
                )
        buffers.clear()

    async def _flush_partition(
        self,
        dataset: str,
        partition: tuple[str, str],
        spec: DatasetSpec,
        buffer: PartitionBuffer,
    ) -> None:
        if not buffer.rows:
            return
        rows = buffer.rows
        buffer.rows = []
        buffer.first_row_monotonic = 0.0
        try:
            await asyncio.to_thread(
                self._write_segment_sync,
                dataset,
                partition[0],
                partition[1],
                spec,
                rows,
            )
        except Exception:
            buffer.rows = rows
            buffer.first_row_monotonic = time.monotonic()
            raise

    def _partition_for_record(
        self, dataset: str, spec: DatasetSpec, record: dict[str, Any]
    ) -> tuple[str, str]:
        day = self._day_for_timestamp(int(record[spec.timestamp_field]))
        if spec.partition_by_symbol:
            return str(record["symbol"]), day
        return "", day

    def _partition_dir(self, dataset: str, symbol: str, day: str) -> Path:
        directory_name, _ = DATASET_LAYOUT[dataset]
        base = self._settings.data_dir / directory_name
        if SPECS[dataset].partition_by_symbol:
            return base / f"symbol={symbol}" / f"date={day}"
        return base / f"date={day}"

    def _final_path(self, dataset: str, symbol: str, day: str) -> Path:
        _, filename = DATASET_LAYOUT[dataset]
        return self._partition_dir(dataset, symbol, day) / filename

    def _write_segment_sync(
        self,
        dataset: str,
        symbol: str,
        day: str,
        spec: DatasetSpec,
        rows: list[dict[str, Any]],
    ) -> None:
        partition_dir = self._partition_dir(dataset, symbol, day)
        partition_dir.mkdir(parents=True, exist_ok=True)
        token = f"{time.time_ns()}.{uuid.uuid4().hex}"
        destination = partition_dir / f".segment.{token}.parquet"
        temporary = partition_dir / f".segment.{token}.parquet.tmp"

        frame = pd.DataFrame.from_records(rows, columns=spec.schema.names)
        table = pa.Table.from_pandas(
            frame, schema=spec.schema, preserve_index=False, safe=False
        )
        pq.write_table(
            table,
            temporary,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )
        os.replace(temporary, destination)

    async def _schedule_finalization(
        self, dataset: str, symbol: str, day: str
    ) -> None:
        key = (dataset, symbol, day)
        if key in self._scheduled_finalizations:
            return
        self._scheduled_finalizations.add(key)
        await self._finalize_queue.put(key)

    async def _finalizer_loop(self) -> None:
        while True:
            item = await self._finalize_queue.get()
            if item is _STOP:
                self._finalize_queue.task_done()
                return

            dataset, symbol, day = item
            try:
                for attempt in range(1, 4):
                    try:
                        await asyncio.to_thread(
                            self._compact_partition_sync,
                            dataset,
                            symbol,
                            day,
                        )
                        break
                    except Exception as exc:
                        if attempt == 3:
                            raise
                        logger.warning(
                            "Parquet compaction failed; retrying",
                            extra={
                                "dataset": dataset,
                                "symbol": symbol,
                                "day": day,
                                "attempt": attempt,
                                "error": str(exc),
                            },
                        )
                        await asyncio.sleep(float(2**attempt))
            except Exception as exc:
                message = f"{dataset}/{symbol}/{day}: {exc}"
                self._finalizer_errors.append(message)
                if self._runtime_metrics is not None:
                    self._runtime_metrics.record_storage_failure()
                logger.exception(
                    "Parquet compaction failed",
                    extra={"dataset": dataset, "symbol": symbol, "day": day},
                )
            finally:
                self._scheduled_finalizations.discard(item)
                self._finalize_queue.task_done()

    def _compact_partition_sync(
        self, dataset: str, symbol: str, day: str
    ) -> None:
        partition_dir = self._partition_dir(dataset, symbol, day)
        if not partition_dir.exists():
            return

        segments = sorted(
            path
            for path in partition_dir.glob(".segment.*.parquet")
            if _SEGMENT_RE.match(path.name)
        )
        final_path = self._final_path(dataset, symbol, day)

        if not segments and not final_path.exists():
            return

        if not segments and final_path.exists():
            return

        if not final_path.exists() and len(segments) == 1:
            os.replace(segments[0], final_path)
            logger.info(
                "Compacted Parquet partition",
                extra={
                    "dataset": dataset,
                    "symbol": symbol,
                    "day": day,
                    "rows": _parquet_row_count(final_path),
                },
            )
            return

        schema = SPECS[dataset].schema
        merge_path = partition_dir / f".merge.{uuid.uuid4().hex}.parquet"
        sources = ([final_path] if final_path.exists() else []) + segments
        expected_rows = sum(_parquet_row_count(source) for source in sources)

        writer: pq.ParquetWriter | None = None
        try:
            writer = pq.ParquetWriter(
                merge_path,
                schema,
                compression="snappy",
                use_dictionary=True,
                write_statistics=True,
            )
            for source in sources:
                with source.open("rb") as input_file:
                    parquet_file = pq.ParquetFile(input_file)
                    for batch in parquet_file.iter_batches(batch_size=65_536):
                        table = pa.Table.from_batches([batch]).cast(schema)
                        writer.write_table(table)
            writer.close()
            writer = None

            actual_rows = _parquet_row_count(merge_path)
            if actual_rows != expected_rows:
                raise RuntimeError(
                    "Compaction row count mismatch for "
                    f"{dataset}/symbol={symbol}/date={day}: "
                    f"expected {expected_rows}, got {actual_rows}"
                )

            os.replace(merge_path, final_path)
            for segment in segments:
                segment.unlink(missing_ok=True)
        finally:
            if writer is not None:
                writer.close()
            merge_path.unlink(missing_ok=True)

        logger.info(
            "Compacted Parquet partition",
            extra={
                "dataset": dataset,
                "symbol": symbol,
                "day": day,
                "rows": expected_rows,
                "segments_merged": len(segments),
            },
        )

    async def _recover_segments(self) -> None:
        for dataset in DATASET_NAMES:
            directory_name, _ = DATASET_LAYOUT[dataset]
            dataset_dir = self._settings.data_dir / directory_name
            if not dataset_dir.exists():
                continue

            if SPECS[dataset].partition_by_symbol:
                partition_dirs = (
                    (symbol_match.group("symbol"), day_match.group("day"), date_dir)
                    for symbol_dir in dataset_dir.iterdir()
                    if symbol_dir.is_dir()
                    for symbol_match in [_SYMBOL_DIR_RE.match(symbol_dir.name)]
                    if symbol_match
                    for date_dir in symbol_dir.iterdir()
                    if date_dir.is_dir()
                    for day_match in [_DATE_DIR_RE.match(date_dir.name)]
                    if day_match
                )
            else:
                partition_dirs = (
                    ("", day_match.group("day"), date_dir)
                    for date_dir in dataset_dir.iterdir()
                    if date_dir.is_dir()
                    for day_match in [_DATE_DIR_RE.match(date_dir.name)]
                    if day_match
                )

            for symbol, day, date_dir in partition_dirs:
                for temporary in date_dir.glob("*.tmp"):
                    logger.warning(
                        "Removing unfinished Parquet write",
                        extra={
                            "dataset": dataset,
                            "symbol": symbol or "all",
                            "day": day,
                            "path": str(temporary),
                        },
                    )
                    temporary.unlink(missing_ok=True)

                for merge_temp in date_dir.glob(".merge.*.parquet"):
                    merge_temp.unlink(missing_ok=True)

                segments = [
                    path
                    for path in date_dir.glob(".segment.*.parquet")
                    if _SEGMENT_RE.match(path.name)
                ]
                if segments:
                    await self._schedule_finalization(dataset, symbol, day)

    @staticmethod
    def _day_for_timestamp(timestamp_ms: int) -> str:
        return datetime.fromtimestamp(timestamp_ms / 1000.0, UTC).date().isoformat()
