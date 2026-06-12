from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from collector.config import Settings

logger = logging.getLogger(__name__)

DEFAULT_REPO_ROOT = Path("/opt/binance-futures-collector")
BACKUP_EXCLUDE_PATTERNS = (
    r"__pycache__",
    r"\.pyc$",
    r"\.pytest_cache",
    r"\.tmp$",
    r"\.segment\.",
    r"\.merge\.",
)

_EXCLUDE_RE = re.compile("|".join(BACKUP_EXCLUDE_PATTERNS))


@dataclass(frozen=True, slots=True)
class BackupScope:
    data_dir: Path
    log_dir: Path
    readme_path: Path
    config_paths: tuple[Path, ...]


@dataclass
class BackupReport:
    timestamp: str
    report_day: str
    files_uploaded: int
    bytes_uploaded: int
    duration_seconds: float
    success: bool
    error_message: str | None = None
    gcs_uri: str | None = None
    objects_verified: int | None = None
    bytes_verified: int | None = None
    attempts: int = 1

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timestamp": self.timestamp,
            "report_day": self.report_day,
            "files_uploaded": self.files_uploaded,
            "bytes_uploaded": self.bytes_uploaded,
            "duration_seconds": round(self.duration_seconds, 3),
            "success": self.success,
            "error_message": self.error_message,
        }
        if self.gcs_uri is not None:
            payload["gcs_uri"] = self.gcs_uri
        if self.objects_verified is not None:
            payload["objects_verified"] = self.objects_verified
        if self.bytes_verified is not None:
            payload["bytes_verified"] = self.bytes_verified
        payload["attempts"] = self.attempts
        return payload


@dataclass
class CloudBackupRunner:
    settings: Settings
    repo_root: Path = DEFAULT_REPO_ROOT
    max_retries: int = 3
    base_backoff_seconds: float = 5.0

    def run(self, report_day: str | None = None) -> BackupReport:
        if not self.settings.backup_enabled:
            return self._skipped_report(report_day, "BACKUP_ENABLED is not 1")
        if not self.settings.backup_gcs_uri:
            return self._failed_report(
                report_day, "BACKUP_GCS_URI is not configured", duration=0.0
            )
        if not shutil.which("gcloud"):
            return self._failed_report(
                report_day, "gcloud CLI not found on PATH", duration=0.0
            )

        day = report_day or datetime.now(UTC).date().isoformat()
        scope = self._build_scope()
        started = time.monotonic()
        last_error = "unknown error"

        for attempt in range(1, self.max_retries + 1):
            try:
                report = self._run_once(scope, day, attempt=attempt)
                report.duration_seconds = time.monotonic() - started
                self._write_report(report)
                self._write_status(report)
                return report
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Cloud backup attempt %d/%d failed: %s",
                    attempt,
                    self.max_retries,
                    last_error,
                )
                if attempt < self.max_retries:
                    sleep_seconds = self.base_backoff_seconds * (2 ** (attempt - 1))
                    time.sleep(sleep_seconds)

        report = self._failed_report(day, last_error, duration=time.monotonic() - started)
        report.attempts = self.max_retries
        self._write_report(report)
        self._write_status(report)
        return report

    def _run_once(self, scope: BackupScope, report_day: str, *, attempt: int) -> BackupReport:
        gcs_base = self.settings.backup_gcs_uri.rstrip("/")
        dest_root = f"{gcs_base}/{report_day}"
        local_files, local_bytes = self._count_scope(scope)

        self._sync_path(scope.data_dir, f"{dest_root}/data")
        self._sync_path(scope.log_dir, f"{dest_root}/logs")
        if scope.readme_path.exists():
            self._copy_file(scope.readme_path, f"{dest_root}/README.md")
        self._write_config_snapshot(scope, dest_root)

        objects_verified, bytes_verified = self._verify_destination(dest_root)
        if bytes_verified <= 0:
            raise RuntimeError("verification failed: zero bytes in destination")
        if local_bytes > 0 and bytes_verified < int(local_bytes * 0.90):
            raise RuntimeError(
                f"verification byte mismatch: local={local_bytes} verified={bytes_verified}"
            )
        if objects_verified <= 0:
            raise RuntimeError("verification failed: zero objects in destination")

        self._write_latest_pointer(gcs_base, report_day)
        return BackupReport(
            timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            report_day=report_day,
            files_uploaded=local_files,
            bytes_uploaded=local_bytes,
            duration_seconds=0.0,
            success=True,
            error_message=None,
            gcs_uri=dest_root,
            objects_verified=objects_verified,
            bytes_verified=bytes_verified,
            attempts=attempt,
        )

    def _build_scope(self) -> BackupScope:
        config_paths: list[Path] = []
        env_file = self.repo_root / "deployment/systemd/binance-futures-collector.env"
        if env_file.exists():
            config_paths.append(env_file)
        return BackupScope(
            data_dir=self.settings.data_dir,
            log_dir=self.settings.log_dir,
            readme_path=self.repo_root / "README.md",
            config_paths=tuple(config_paths),
        )

    def _count_scope(self, scope: BackupScope) -> tuple[int, int]:
        files = 0
        total_bytes = 0
        for path in (scope.data_dir, scope.log_dir):
            if not path.exists():
                continue
            for file_path in path.rglob("*"):
                if not file_path.is_file() or self._is_excluded(file_path):
                    continue
                files += 1
                total_bytes += file_path.stat().st_size
        for config_path in scope.config_paths:
            if config_path.is_file():
                files += 1
                total_bytes += config_path.stat().st_size
        if scope.readme_path.is_file():
            files += 1
            total_bytes += scope.readme_path.stat().st_size
        return files, total_bytes

    def _sync_path(self, local_path: Path, gcs_destination: str) -> None:
        if not local_path.exists():
            logger.warning("Backup source missing, skipping: %s", local_path)
            return
        command = [
            "gcloud",
            "storage",
            "rsync",
            "-r",
            str(local_path),
            gcs_destination,
        ]
        for pattern in BACKUP_EXCLUDE_PATTERNS:
            command.extend(["--exclude", pattern])
        self._run_command(command)

    def _copy_file(self, local_path: Path, gcs_destination: str) -> None:
        command = ["gcloud", "storage", "cp", str(local_path), gcs_destination]
        self._run_command(command)

    def _write_config_snapshot(self, scope: BackupScope, dest_root: str) -> None:
        snapshot = {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "repo_root": str(self.repo_root),
            "data_dir": str(scope.data_dir),
            "log_dir": str(scope.log_dir),
            "config_files": [str(path) for path in scope.config_paths],
            "environment": {
                key: value
                for key, value in os.environ.items()
                if key.startswith(
                    (
                        "COLLECTOR_",
                        "BACKUP_",
                        "BINANCE_",
                        "TELEGRAM_",
                        "HEALTH_",
                        "QUALITY_",
                        "FUNDING_",
                        "OI_",
                    )
                )
            },
        }
        temp_path = self.settings.backup_report_dir / ".config_snapshot.json"
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        self._copy_file(temp_path, f"{dest_root}/config/snapshot.json")
        for config_path in scope.config_paths:
            relative_name = config_path.name
            self._copy_file(config_path, f"{dest_root}/config/{relative_name}")

    def _verify_destination(self, dest_root: str) -> tuple[int, int]:
        list_command = ["gcloud", "storage", "ls", "-r", f"{dest_root}/**"]
        output = self._run_command(list_command, check=False)
        objects = [
            line.strip()
            for line in output.splitlines()
            if line.strip().startswith("gs://")
        ]
        du_command = ["gcloud", "storage", "du", "-s", dest_root]
        du_output = self._run_command(du_command, check=False)
        bytes_verified = _parse_du_bytes(du_output)
        return len(objects), bytes_verified

    def _write_latest_pointer(self, gcs_base: str, report_day: str) -> None:
        temp_path = self.settings.backup_report_dir / ".latest_pointer.txt"
        temp_path.write_text(report_day, encoding="utf-8")
        self._copy_file(temp_path, f"{gcs_base}/LATEST")

    def _write_report(self, report: BackupReport) -> None:
        report_dir = self.settings.backup_report_dir
        report_dir.mkdir(parents=True, exist_ok=True)
        path = report_dir / f"{report.report_day}.json"
        path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")

    def _write_status(self, report: BackupReport) -> None:
        status_path = self.settings.backup_status_file
        status_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run": report.timestamp,
            "success": report.success,
            "report_day": report.report_day,
            "files_uploaded": report.files_uploaded,
            "bytes_uploaded": report.bytes_uploaded,
        }
        if report.error_message:
            payload["error"] = report.error_message
        status_path.write_text(json.dumps(payload), encoding="utf-8")

    def _is_excluded(self, path: Path) -> bool:
        normalized = path.as_posix()
        return bool(_EXCLUDE_RE.search(normalized))

    def _run_command(
        self, command: list[str], *, check: bool = True
    ) -> str:
        logger.info("Running: %s", " ".join(command))
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if check and completed.returncode != 0:
            stderr = completed.stderr.strip()
            stdout = completed.stdout.strip()
            detail = stderr or stdout or f"exit code {completed.returncode}"
            raise RuntimeError(detail)
        return completed.stdout

    def _skipped_report(self, report_day: str | None, reason: str) -> BackupReport:
        day = report_day or datetime.now(UTC).date().isoformat()
        return BackupReport(
            timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            report_day=day,
            files_uploaded=0,
            bytes_uploaded=0,
            duration_seconds=0.0,
            success=False,
            error_message=reason,
        )

    def _failed_report(
        self, report_day: str | None, reason: str, *, duration: float
    ) -> BackupReport:
        day = report_day or datetime.now(UTC).date().isoformat()
        return BackupReport(
            timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            report_day=day,
            files_uploaded=0,
            bytes_uploaded=0,
            duration_seconds=duration,
            success=False,
            error_message=reason,
        )


def _parse_du_bytes(output: str) -> int:
    for line in output.splitlines():
        parts = line.strip().split()
        if len(parts) >= 1 and parts[0].isdigit():
            return int(parts[0])
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    runner = CloudBackupRunner(
        settings,
        repo_root=Path(os.getenv("COLLECTOR_REPO_ROOT", str(DEFAULT_REPO_ROOT))),
        max_retries=int(os.getenv("BACKUP_MAX_RETRIES", "3")),
    )
    report = runner.run()
    if report.success:
        logger.info(
            "Backup complete: %d files, %d bytes",
            report.files_uploaded,
            report.bytes_uploaded,
        )
        return 0
    logger.error("Backup failed: %s", report.error_message)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
