from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from collector.storage import DATASET_LAYOUT

logger = logging.getLogger(__name__)

_LEGACY_SYMBOL_RE = re.compile(r"^[A-Z0-9]{3,24}USDT$")
_LEGACY_DATE_IN_NAME_RE = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")
_LEGACY_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HIVE_SYMBOL_RE = re.compile(r"^symbol=(?P<symbol>.+)$")
_HIVE_DATE_RE = re.compile(r"^date=(?P<day>\d{4}-\d{2}-\d{2})$")

# Datasets that accept new writes. depth20 is read-only legacy storage.
WRITE_DATASET_NAMES = tuple(name for name in DATASET_LAYOUT if name != "depth20")


@dataclass
class LayoutAuditReport:
    hive_partition_count: int = 0
    legacy_partition_count: int = 0
    hive_bytes: int = 0
    legacy_bytes: int = 0
    legacy_paths: list[str] = field(default_factory=list)
    duplicate_risks: list[str] = field(default_factory=list)
    inconsistencies: list[str] = field(default_factory=list)


@dataclass
class LayoutMigrationReport:
    migrated_paths: list[str] = field(default_factory=list)
    removed_empty_paths: list[str] = field(default_factory=list)
    quarantined_paths: list[str] = field(default_factory=list)
    skipped_paths: list[str] = field(default_factory=list)


def audit_data_layout(data_dir: Path) -> LayoutAuditReport:
    report = LayoutAuditReport()
    for directory_name in _unique_dataset_directories():
        dataset_root = data_dir / directory_name
        if not dataset_root.is_dir():
            continue
        _audit_dataset_root(dataset_root, directory_name, report)

    if (data_dir / "depth").exists() and (data_dir / "depth50").exists():
        report.inconsistencies.append(
            "Both legacy depth/ and hive depth50/ exist; new writes go to depth50/ only"
        )
    return report


def migrate_legacy_layout(data_dir: Path) -> LayoutMigrationReport:
    report = LayoutMigrationReport()
    for directory_name in _unique_dataset_directories():
        dataset_root = data_dir / directory_name
        if not dataset_root.is_dir():
            continue
        _migrate_dataset_root(data_dir, dataset_root, directory_name, report)
    return report


def _unique_dataset_directories() -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for directory_name, _ in DATASET_LAYOUT.values():
        if directory_name not in seen:
            seen.add(directory_name)
            ordered.append(directory_name)
    return tuple(ordered)


def _audit_dataset_root(
    dataset_root: Path, directory_name: str, report: LayoutAuditReport
) -> None:
    partition_by_symbol = directory_name != "metadata"
    for child in sorted(dataset_root.iterdir()):
        if not child.is_dir():
            continue
        hive_symbol = _HIVE_SYMBOL_RE.match(child.name)
        legacy_symbol = partition_by_symbol and _LEGACY_SYMBOL_RE.fullmatch(
            child.name
        )
        hive_date = _HIVE_DATE_RE.match(child.name)

        if hive_symbol:
            for date_dir in child.iterdir():
                if date_dir.is_dir() and _HIVE_DATE_RE.match(date_dir.name):
                    report.hive_partition_count += 1
                    report.hive_bytes += _dir_bytes(date_dir)
        elif legacy_symbol:
            report.legacy_partition_count += 1
            report.legacy_paths.append(str(child))
            report.legacy_bytes += _dir_bytes(child)
            symbol = child.name
            for date_dir in child.iterdir():
                if not date_dir.is_dir():
                    continue
                day = _day_from_dirname(date_dir.name)
                if day is None:
                    continue
                hive_target = (
                    dataset_root / f"symbol={symbol}" / f"date={day}"
                )
                if hive_target.exists() and any(hive_target.iterdir()):
                    report.duplicate_risks.append(
                        f"{directory_name}: {symbol}/{day} exists in legacy and hive layouts"
                    )
        elif hive_date and directory_name == "metadata":
            report.hive_partition_count += 1
            report.hive_bytes += _dir_bytes(child)
        elif partition_by_symbol and _LEGACY_DATE_RE.fullmatch(child.name):
            report.legacy_paths.append(str(child))
            report.legacy_bytes += _dir_bytes(child)
            report.inconsistencies.append(
                f"{directory_name}: bare date directory {child.name} at dataset root"
            )


def _migrate_dataset_root(
    data_dir: Path,
    dataset_root: Path,
    directory_name: str,
    report: LayoutMigrationReport,
) -> None:
    if directory_name == "metadata":
        for child in list(dataset_root.iterdir()):
            if child.is_dir() and _LEGACY_DATE_RE.fullmatch(child.name):
                target = dataset_root / f"date={child.name}"
                _merge_directory(child, target, report)
        return

    for child in list(dataset_root.iterdir()):
        if not child.is_dir():
            continue
        if _HIVE_SYMBOL_RE.match(child.name):
            continue
        if not _LEGACY_SYMBOL_RE.fullmatch(child.name):
            continue
        symbol = child.name
        hive_symbol_dir = dataset_root / f"symbol={symbol}"
        hive_symbol_dir.mkdir(parents=True, exist_ok=True)

        for entry in list(child.iterdir()):
            day = _day_from_dirname(entry.name) if entry.is_dir() else None
            if entry.is_dir() and day is not None:
                target = hive_symbol_dir / f"date={day}"
                _merge_directory(entry, target, report)
                continue
            if entry.is_file() and _is_parquet_artifact(entry.name):
                day = _infer_day_from_parquet(entry)
                target = hive_symbol_dir / f"date={day}"
                target.mkdir(parents=True, exist_ok=True)
                destination = target / entry.name
                _move_file(entry, destination, data_dir, report)
                continue
            report.skipped_paths.append(str(entry))

        if child.exists() and not any(child.iterdir()):
            child.rmdir()
            report.removed_empty_paths.append(str(child))


def _merge_directory(source: Path, target: Path, report: LayoutMigrationReport) -> None:
    if target.exists() and any(target.iterdir()) and any(source.iterdir()):
        for item in source.iterdir():
            destination = target / item.name
            if destination.exists():
                report.skipped_paths.append(
                    f"conflict: {item} already exists at {destination}"
                )
                continue
            shutil.move(str(item), str(destination))
            report.migrated_paths.append(f"{item} -> {destination}")
        if source.exists() and not any(source.iterdir()):
            source.rmdir()
            report.removed_empty_paths.append(str(source))
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        shutil.move(str(source), str(target))
        report.migrated_paths.append(f"{source} -> {target}")
        return

    for item in source.iterdir():
        destination = target / item.name
        shutil.move(str(item), str(destination))
        report.migrated_paths.append(f"{item} -> {destination}")
    if source.exists() and not any(source.iterdir()):
        source.rmdir()
        report.removed_empty_paths.append(str(source))


def _move_file(
    source: Path,
    destination: Path,
    data_dir: Path,
    report: LayoutMigrationReport,
) -> None:
    if destination.exists():
        quarantine = data_dir / "_layout_quarantine" / destination.relative_to(
            data_dir
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(quarantine))
        report.quarantined_paths.append(f"{source} -> {quarantine}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))
    report.migrated_paths.append(f"{source} -> {destination}")


def _day_from_dirname(name: str) -> str | None:
    hive_match = _HIVE_DATE_RE.match(name)
    if hive_match:
        return hive_match.group("day")
    if _LEGACY_DATE_RE.fullmatch(name):
        return name
    return None


def _infer_day_from_parquet(path: Path) -> str:
    embedded = _LEGACY_DATE_IN_NAME_RE.search(path.name)
    if embedded:
        return embedded.group(1)
    stem = path.name
    for suffix in (".parquet.tmp", ".parquet"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    if _LEGACY_DATE_RE.fullmatch(stem):
        return stem
    mtime = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    return mtime.date().isoformat()


def _is_parquet_artifact(name: str) -> bool:
    return (
        name.endswith(".parquet")
        or name.endswith(".parquet.tmp")
        or name.startswith((".segment.", ".merge."))
    )


def _dir_bytes(path: Path) -> int:
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += file_path.stat().st_size
    return total


def format_layout_summary(
    audit: LayoutAuditReport, migration: LayoutMigrationReport | None = None
) -> str:
    lines = [
        "Storage layout audit:",
        f"  hive partitions: {audit.hive_partition_count}",
        f"  legacy partitions: {audit.legacy_partition_count}",
        f"  hive bytes: {audit.hive_bytes:,}",
        f"  legacy bytes: {audit.legacy_bytes:,}",
    ]
    if audit.duplicate_risks:
        lines.append(f"  duplicate risks: {len(audit.duplicate_risks)}")
    if audit.inconsistencies:
        lines.append(f"  inconsistencies: {len(audit.inconsistencies)}")
    if migration is not None:
        lines.extend(
            [
                "Migration:",
                f"  migrated: {len(migration.migrated_paths)}",
                f"  quarantined: {len(migration.quarantined_paths)}",
                f"  removed empty: {len(migration.removed_empty_paths)}",
            ]
        )
    return "\n".join(lines)


def main() -> None:
    import argparse
    import json
    from collector.config import Settings

    parser = argparse.ArgumentParser(
        description="Audit and optionally migrate collector data layout"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Settings().data_dir,
        help="Data root (default: Settings().data_dir)",
    )
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Migrate legacy paths to Hive partitions before audit",
    )
    args = parser.parse_args()
    data_dir: Path = args.data_dir

    migration = migrate_legacy_layout(data_dir) if args.migrate else None
    audit = audit_data_layout(data_dir)
    print(format_layout_summary(audit, migration))
    if audit.legacy_paths:
        print("\nLegacy paths:")
        for path in audit.legacy_paths[:50]:
            print(f"  {path}")
        if len(audit.legacy_paths) > 50:
            print(f"  ... and {len(audit.legacy_paths) - 50} more")
    if audit.duplicate_risks:
        print("\nDuplicate risks:")
        for risk in audit.duplicate_risks:
            print(f"  {risk}")
    if audit.inconsistencies:
        print("\nInconsistencies:")
        for note in audit.inconsistencies:
            print(f"  {note}")
    if migration and migration.quarantined_paths:
        print("\nQuarantined:")
        for path in migration.quarantined_paths:
            print(f"  {path}")

    payload = {
        "hive_partitions": audit.hive_partition_count,
        "legacy_partitions": audit.legacy_partition_count,
        "hive_bytes": audit.hive_bytes,
        "legacy_bytes": audit.legacy_bytes,
        "duplicate_risks": audit.duplicate_risks,
        "inconsistencies": audit.inconsistencies,
    }
    if migration:
        payload["migrated"] = len(migration.migrated_paths)
        payload["quarantined"] = len(migration.quarantined_paths)
    print("\nJSON:", json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
