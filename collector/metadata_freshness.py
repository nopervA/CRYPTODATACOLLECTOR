from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

if TYPE_CHECKING:
    from collector.health import HealthState

logger = logging.getLogger(__name__)

_METADATA_FILENAME = "metadata.parquet"


def latest_metadata_update_from_disk(data_dir: Path) -> str | None:
    """Return the newest metadata snapshot time from on-disk parquet partitions."""
    metadata_root = data_dir / "metadata"
    if not metadata_root.is_dir():
        return None

    latest_ms: int | None = None
    for date_dir in metadata_root.iterdir():
        if not date_dir.is_dir() or not date_dir.name.startswith("date="):
            continue
        for path in _metadata_parquet_paths(date_dir):
            file_latest = _max_timestamp_ms(path)
            if file_latest is not None and (
                latest_ms is None or file_latest > latest_ms
            ):
                latest_ms = file_latest

    if latest_ms is None:
        return None
    return _iso_from_ms(latest_ms)


def hydrate_last_metadata_update(health: HealthState, data_dir: Path) -> str | None:
    """Set HealthState.last_metadata_update from existing metadata parquet, if any."""
    latest = latest_metadata_update_from_disk(data_dir)
    if latest is not None:
        health.last_metadata_update = latest
    return latest


def _metadata_parquet_paths(partition_dir: Path) -> list[Path]:
    final_path = partition_dir / _METADATA_FILENAME
    if final_path.is_file():
        return [final_path]
    return sorted(
        path
        for path in partition_dir.glob(".segment.*.parquet")
        if path.is_file()
    )


def _max_timestamp_ms(path: Path) -> int | None:
    try:
        with path.open("rb") as input_file:
            parquet_file = pq.ParquetFile(input_file)
            if parquet_file.metadata.num_rows == 0:
                return None
            table = parquet_file.read(columns=["timestamp"])
            timestamps = table.column("timestamp").to_pylist()
            if not timestamps:
                return None
            return max(int(value) for value in timestamps)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Could not read metadata timestamp from %s: %s", path, exc)
        return None


def _iso_from_ms(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000.0, UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
