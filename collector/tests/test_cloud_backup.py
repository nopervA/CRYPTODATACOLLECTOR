import json
from pathlib import Path
from unittest.mock import patch

from collector.cloud_backup import CloudBackupRunner, _parse_du_bytes
from collector.config import Settings


def test_parse_du_bytes() -> None:
    assert _parse_du_bytes("12345  gs://bucket/path") == 12345


def test_cloud_backup_runner_success(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/gcloud")
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"
    report_dir = tmp_path / "backup_reports"
    repo_root = tmp_path / "repo"
    data_dir.mkdir()
    log_dir.mkdir()
    (data_dir / "trades.parquet").write_bytes(b"x" * 100)
    (log_dir / "collector.log").write_text("ok", encoding="utf-8")
    (repo_root / "deployment/systemd").mkdir(parents=True)
    (repo_root / "deployment/systemd/binance-futures-collector.env").write_text(
        "BACKUP_ENABLED=1\n", encoding="utf-8"
    )
    (repo_root / "README.md").write_text("# readme", encoding="utf-8")

    settings = Settings(
        data_dir=data_dir,
        log_dir=log_dir,
        backup_enabled=True,
        backup_gcs_uri="gs://test-bucket/binance-futures",
        backup_report_dir=report_dir,
        backup_status_file=tmp_path / "backup_status.json",
    )

    calls: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool = True) -> str:
        calls.append(command)
        if command[:3] == ["gcloud", "storage", "ls"]:
            return "gs://test-bucket/binance-futures/2026-06-11/data/trades.parquet\n"
        if command[:3] == ["gcloud", "storage", "du"]:
            return "999999\n"
        return ""

    runner = CloudBackupRunner(settings, repo_root=repo_root, max_retries=1)
    with patch.object(runner, "_run_command", side_effect=fake_run):
        report = runner.run(report_day="2026-06-11")

    assert report.success is True
    assert report.files_uploaded >= 3
    assert (report_dir / "2026-06-11.json").exists()
    payload = json.loads((report_dir / "2026-06-11.json").read_text(encoding="utf-8"))
    assert payload["success"] is True
    assert payload["bytes_uploaded"] >= 100


def test_cloud_backup_retries_on_failure(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/gcloud")
    settings = Settings(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        backup_enabled=True,
        backup_gcs_uri="gs://test-bucket/binance-futures",
        backup_report_dir=tmp_path / "backup_reports",
        backup_status_file=tmp_path / "backup_status.json",
    )
    (tmp_path / "data").mkdir()

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
        report = runner.run(report_day="2026-06-12")

    assert report.success is True
    assert attempts["count"] >= 2


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
