import json
from pathlib import Path
from unittest.mock import patch

from collector.cloud_backup import (
    CloudBackupRunner,
    notify_telegram_backup,
    _parse_du_bytes,
)
from collector.config import Settings


def test_parse_du_bytes() -> None:
    assert _parse_du_bytes("12345  gs://bucket/path") == 12345


def test_destination_root_uses_weekly_prefix() -> None:
    settings = Settings(
        backup_gcs_uri="gs://binance-futures-research-data",
        backup_gcs_prefix="weekly",
    )
    runner = CloudBackupRunner(settings, repo_root=Path("/tmp/repo"))
    assert (
        runner._destination_root("2026-06-15")
        == "gs://binance-futures-research-data/weekly/2026-06-15"
    )


def test_cloud_backup_runner_success(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/gcloud")
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"
    report_dir = tmp_path / "backup_reports"
    data_dir.mkdir()
    log_dir.mkdir()
    (data_dir / "trades.parquet").write_bytes(b"x" * 100)
    (log_dir / "collector.log").write_text("ok", encoding="utf-8")

    settings = Settings(
        data_dir=data_dir,
        log_dir=log_dir,
        backup_enabled=True,
        backup_gcs_uri="gs://binance-futures-research-data",
        backup_gcs_prefix="weekly",
        backup_report_dir=report_dir,
        backup_status_file=tmp_path / "backup_status.json",
    )

    calls: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool = True) -> str:
        calls.append(command)
        if command[:3] == ["gcloud", "storage", "ls"]:
            return (
                "gs://binance-futures-research-data/weekly/2026-06-11/data/trades.parquet\n"
            )
        if command[:3] == ["gcloud", "storage", "du"]:
            return "999999\n"
        return ""

    runner = CloudBackupRunner(settings, repo_root=tmp_path, max_retries=1)
    with patch.object(runner, "_run_command", side_effect=fake_run):
        with patch("collector.cloud_backup.notify_telegram_backup") as notify:
            report = runner.run(report_day="2026-06-11")

    assert report.success is True
    assert report.backup_uri == "gs://binance-futures-research-data/weekly/2026-06-11"
    assert report.files_uploaded >= 2
    assert (report_dir / "2026-06-11.json").exists()
    rsync_dests = [
        arg
        for call in calls
        if call[:3] == ["gcloud", "storage", "rsync"]
        for arg in call
        if str(arg).startswith("gs://")
    ]
    assert rsync_dests == [
        "gs://binance-futures-research-data/weekly/2026-06-11/data",
        "gs://binance-futures-research-data/weekly/2026-06-11/logs",
    ]
    notify.assert_called_once()

    status = json.loads((tmp_path / "backup_status.json").read_text(encoding="utf-8"))
    assert status["success"] is True
    assert status["bytes_uploaded"] >= 100
    assert status["duration_seconds"] >= 0
    assert status["backup_uri"] == "gs://binance-futures-research-data/weekly/2026-06-11"
    assert "timestamp" in status
    assert status["error_message"] is None


def test_cloud_backup_retries_on_failure(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/gcloud")
    settings = Settings(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        backup_enabled=True,
        backup_gcs_uri="gs://binance-futures-research-data",
        backup_report_dir=tmp_path / "backup_reports",
        backup_status_file=tmp_path / "backup_status.json",
    )
    (tmp_path / "data").mkdir()
    (tmp_path / "logs").mkdir()

    attempts = {"count": 0}

    def flaky_run(command: list[str], *, check: bool = True) -> str:
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise RuntimeError("temporary failure")
        if command[:3] == ["gcloud", "storage", "ls"]:
            return "gs://x\n"
        if command[:3] == ["gcloud", "storage", "du"]:
            return "1\n"
        return ""

    runner = CloudBackupRunner(
        settings,
        repo_root=tmp_path,
        max_retries=3,
        base_backoff_seconds=0.01,
    )
    with patch.object(runner, "_run_command", side_effect=flaky_run):
        with patch("collector.cloud_backup.notify_telegram_backup"):
            report = runner.run(report_day="2026-06-12")

    assert report.success is True
    assert attempts["count"] >= 2


def test_cloud_backup_failure_writes_status_and_notifies(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/gcloud")
    settings = Settings(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        backup_enabled=True,
        backup_gcs_uri="gs://binance-futures-research-data",
        backup_report_dir=tmp_path / "backup_reports",
        backup_status_file=tmp_path / "backup_status.json",
    )
    (tmp_path / "data").mkdir()
    (tmp_path / "logs").mkdir()

    runner = CloudBackupRunner(settings, repo_root=tmp_path, max_retries=1)
    with patch.object(runner, "_run_command", side_effect=RuntimeError("rsync failed")):
        with patch("collector.cloud_backup.notify_telegram_backup") as notify:
            report = runner.run(report_day="2026-06-13")

    assert report.success is False
    status = json.loads((tmp_path / "backup_status.json").read_text(encoding="utf-8"))
    assert status["success"] is False
    assert status["error_message"] == "rsync failed"
    assert status["error"] == "rsync failed"
    notify.assert_called_once()


def test_excluded_paths_are_not_counted(tmp_path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "ok.parquet").write_bytes(b"1234")
    (data_dir / "__pycache__").mkdir()
    (data_dir / "__pycache__" / "x.pyc").write_bytes(b"9999")
    (data_dir / ".segment.abc.parquet").write_bytes(b"9999")

    settings = Settings(
        data_dir=data_dir,
        log_dir=tmp_path / "logs",
        backup_enabled=True,
        backup_gcs_uri="gs://bucket/path",
        backup_report_dir=tmp_path / "backup_reports",
    )
    runner = CloudBackupRunner(settings, repo_root=tmp_path)
    scope = runner._build_scope()
    files, total_bytes = runner._count_scope(scope)
    assert files == 1
    assert total_bytes == 4


def test_notify_telegram_backup_success(tmp_path) -> None:
    script = tmp_path / "deployment/scripts/telegram-notify.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/sh\necho \"$@\"\n", encoding="utf-8")
    script.chmod(0o755)

    from collector.cloud_backup import BackupReport

    report = BackupReport(
        timestamp="2026-06-11T03:00:00Z",
        report_day="2026-06-11",
        files_uploaded=10,
        bytes_uploaded=123456,
        duration_seconds=42.5,
        success=True,
        backup_uri="gs://binance-futures-research-data/weekly/2026-06-11",
    )
    notify_telegram_backup(tmp_path, report)
