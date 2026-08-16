"""Tests for the Yamtrack SQLite backup container.

Covers the snapshot/verify/compress pipeline against a real WAL-mode database
with a concurrent writer, plus the scheduling arithmetic and the failure paths
that decide whether a bad run is reported or silently swallowed.

Run with the container's dependencies installed:

    pip install -r backup/requirements.txt pytest
    pytest backup/test_backup.py
"""

from __future__ import annotations

import gzip
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# The module imports its cloud dependencies at import time; skip cleanly rather
# than erroring collection when they are not installed in the current env.
pytest.importorskip("httpx")
pytest.importorskip("google.cloud.storage")

sys.path.insert(0, str(Path(__file__).parent))

import backup  # noqa: E402


@pytest.fixture
def live_database(tmp_path):
    """Create a WAL-mode database with an open writer, mimicking a running app.

    Yields:
        tuple[Path, sqlite3.Connection]: The database path and the still-open
        writer connection, so tests exercise the concurrent-access case rather
        than a quiescent file.
    """
    path = tmp_path / "db.sqlite3"
    writer = sqlite3.connect(path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE media (id INTEGER PRIMARY KEY, title TEXT)")
    writer.executemany(
        "INSERT INTO media VALUES (?, ?)",
        [(index, f"title {index}") for index in range(1000)],
    )
    writer.commit()

    yield path, writer

    writer.close()


def test_snapshot_round_trips_through_compression(live_database, tmp_path):
    """A snapshot survives gzip and reopens as a database with all its rows."""
    source_path, _writer = live_database
    snapshot_path = tmp_path / "snapshot.sqlite3"
    archive_path = tmp_path / "snapshot.sqlite3.gz"
    restored_path = tmp_path / "restored.sqlite3"

    backup.snapshot_database(source_path, snapshot_path)
    backup.verify_snapshot(snapshot_path)
    backup.compress(snapshot_path, archive_path)

    with gzip.open(archive_path, "rb") as compressed, open(restored_path, "wb") as raw:
        shutil.copyfileobj(compressed, raw)

    connection = sqlite3.connect(restored_path)
    assert connection.execute("SELECT COUNT(*) FROM media").fetchone()[0] == 1000
    connection.close()


def test_snapshot_excludes_writes_made_after_it_started(live_database, tmp_path):
    """The snapshot is a point-in-time copy, not a live view of the database."""
    source_path, writer = live_database
    snapshot_path = tmp_path / "snapshot.sqlite3"

    backup.snapshot_database(source_path, snapshot_path)

    writer.execute("INSERT INTO media VALUES (99999, 'added after snapshot')")
    writer.commit()

    connection = sqlite3.connect(snapshot_path)
    assert connection.execute("SELECT COUNT(*) FROM media").fetchone()[0] == 1000
    connection.close()


def test_verify_rejects_a_corrupt_snapshot(tmp_path):
    """Integrity failures raise, so a bad copy never reaches the bucket."""
    corrupt_path = tmp_path / "corrupt.sqlite3"

    # A valid header followed by garbage: openable, but structurally broken.
    connection = sqlite3.connect(corrupt_path)
    connection.execute("CREATE TABLE media (id INTEGER PRIMARY KEY, title TEXT)")
    connection.executemany(
        "INSERT INTO media VALUES (?, ?)",
        [(index, f"title {index}") for index in range(500)],
    )
    connection.commit()
    connection.close()

    with open(corrupt_path, "r+b") as handle:
        handle.seek(4096)
        handle.write(b"\xde\xad\xbe\xef" * 512)

    with pytest.raises((RuntimeError, sqlite3.DatabaseError)):
        backup.verify_snapshot(corrupt_path)


@pytest.mark.parametrize(
    ("schedule", "now", "expected_hours"),
    [
        ("04:00", datetime(2026, 8, 16, 1, 0), 3.0),  # later today
        ("04:00", datetime(2026, 8, 16, 9, 0), 19.0),  # already passed, roll over
        ("00:30", datetime(2026, 8, 16, 23, 30), 1.0),  # crosses midnight
    ],
)
def test_seconds_until_picks_the_next_occurrence(
    monkeypatch, schedule, now, expected_hours
):
    """The scheduler targets the next occurrence, rolling to tomorrow if needed."""

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(backup, "datetime", FrozenDatetime)

    delay = backup.seconds_until(schedule)
    assert delay == pytest.approx(timedelta(hours=expected_hours).total_seconds())


@pytest.mark.parametrize("schedule", ["25:00", "04:99", "not-a-time", "4"])
def test_seconds_until_rejects_malformed_schedules(schedule):
    """A bad BACKUP_SCHEDULE fails fast instead of scheduling something wrong."""
    with pytest.raises(ValueError):
        backup.seconds_until(schedule)


def test_missing_database_reports_failure(monkeypatch, tmp_path):
    """A missing database fails the run and alerts, rather than exiting clean."""
    pings = []
    monkeypatch.setattr(backup, "DATABASE_PATH", tmp_path / "absent.sqlite3")
    monkeypatch.setattr(backup, "BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(backup, "HEALTHCHECK_URL", "https://example.invalid/ping")
    monkeypatch.setattr(
        backup,
        "ping_healthcheck",
        lambda url, failed=False: pings.append(failed),
    )

    assert backup.run_backup() is False
    assert pings == [True]


def test_upload_failure_pings_the_fail_endpoint(monkeypatch, live_database):
    """An upload error alerts immediately instead of waiting for a missed ping."""
    source_path, _writer = live_database
    pings = []

    def failing_upload(*args, **kwargs):
        raise RuntimeError("bucket unreachable")

    monkeypatch.setattr(backup, "DATABASE_PATH", source_path)
    monkeypatch.setattr(backup, "BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(backup, "upload", failing_upload)
    monkeypatch.setattr(
        backup,
        "ping_healthcheck",
        lambda url, failed=False: pings.append(failed),
    )

    assert backup.run_backup() is False
    assert pings == [True]


def test_healthcheck_errors_never_propagate(monkeypatch):
    """Monitoring must not be able to fail a backup that already succeeded."""
    import httpx

    def failing_get(*args, **kwargs):
        raise httpx.ConnectError("no route to host")

    monkeypatch.setattr(httpx, "get", failing_get)

    backup.ping_healthcheck("https://example.invalid/ping")  # must not raise
