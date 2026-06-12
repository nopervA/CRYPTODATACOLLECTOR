import asyncio
from dataclasses import replace

import pyarrow.parquet as pq
import pytest

from collector.config import Settings
from collector.storage import StorageManager
from collector.storage_layout import (
    WRITE_DATASET_NAMES,
    audit_data_layout,
    migrate_legacy_layout,
)


def test_write_dataset_names_exclude_depth20() -> None:
    assert "depth20" not in WRITE_DATASET_NAMES
    assert "depth50" in WRITE_DATASET_NAMES
    assert "trades" in WRITE_DATASET_NAMES


def test_migrate_legacy_flat_segment_files(tmp_path) -> None:
    legacy_dir = tmp_path / "trades" / "BTCUSDT"
    legacy_dir.mkdir(parents=True)
    segment = legacy_dir / ".2026-06-10.abc123.segment.parquet"
    segment.write_bytes(b"legacy-segment")

    migration = migrate_legacy_layout(tmp_path)
    assert migration.migrated_paths

    target_dir = tmp_path / "trades" / "symbol=BTCUSDT" / "date=2026-06-10"
    assert (target_dir / segment.name).exists()
    assert not legacy_dir.exists()


def test_migrate_legacy_symbol_date_layout(tmp_path) -> None:
    legacy_dir = tmp_path / "trades" / "BTCUSDT" / "2026-06-10"
    legacy_dir.mkdir(parents=True)
    legacy_file = legacy_dir / "trades.parquet"
    legacy_file.write_bytes(b"legacy-placeholder")

    migration = migrate_legacy_layout(tmp_path)
    assert migration.migrated_paths

    target = (
        tmp_path / "trades" / "symbol=BTCUSDT" / "date=2026-06-10" / "trades.parquet"
    )
    assert target.exists()
    assert not (tmp_path / "trades" / "BTCUSDT").exists()

    audit = audit_data_layout(tmp_path)
    assert audit.legacy_partition_count == 0
    assert audit.hive_partition_count == 1


def test_migrate_metadata_bare_date_dir(tmp_path) -> None:
    legacy = tmp_path / "metadata" / "2026-06-10"
    legacy.mkdir(parents=True)
    (legacy / "metadata.parquet").write_bytes(b"meta")

    migrate_legacy_layout(tmp_path)

    target = tmp_path / "metadata" / "date=2026-06-10" / "metadata.parquet"
    assert target.exists()
    assert not (tmp_path / "metadata" / "2026-06-10").exists()


def test_audit_flags_duplicate_legacy_and_hive(tmp_path) -> None:
    hive = tmp_path / "trades" / "symbol=ETHUSDT" / "date=2026-06-11"
    hive.mkdir(parents=True)
    (hive / "trades.parquet").write_bytes(b"hive")

    legacy = tmp_path / "trades" / "ETHUSDT" / "2026-06-11"
    legacy.mkdir(parents=True)
    (legacy / "trades.parquet").write_bytes(b"legacy")

    audit = audit_data_layout(tmp_path)
    assert audit.legacy_partition_count == 1
    assert audit.hive_partition_count == 1
    assert any("ETHUSDT" in risk for risk in audit.duplicate_risks)


def test_storage_start_migrates_legacy_on_startup(tmp_path) -> None:
    legacy_dir = tmp_path / "data" / "funding" / "BTCUSDT" / "2026-06-09"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "funding.parquet").write_bytes(b"x")

    async def scenario() -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        )
        storage = StorageManager(settings)
        await storage.start()
        await storage.close()

    asyncio.run(scenario())

    target = (
        tmp_path
        / "data"
        / "funding"
        / "symbol=BTCUSDT"
        / "date=2026-06-09"
        / "funding.parquet"
    )
    assert target.exists()
    assert not (tmp_path / "data" / "funding" / "BTCUSDT").exists()


def test_depth20_write_rejected(tmp_path) -> None:
    async def scenario() -> None:
        settings = replace(
            Settings(),
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        )
        storage = StorageManager(settings)
        await storage.start()
        with pytest.raises(ValueError, match="depth20"):
            await storage.write("depth20", {"symbol": "BTCUSDT"})
        await storage.close()

    asyncio.run(scenario())
