#!/usr/bin/env python3
"""Back up and restore the Yamtrack SQLite database with Google Cloud Storage.

Takes a consistent online snapshot of the live database using SQLite's backup
API, verifies its integrity, compresses it, and uploads the result to GCS.
Safe to run while Yamtrack is serving traffic and writing to the database.

Runs as a long-lived scheduler by default, taking one backup per day at
BACKUP_SCHEDULE. Pass --once to take a single backup and exit.
"""

from __future__ import annotations

import argparse
import gzip
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from google.cloud import storage

# --- Configuration (all overridable via environment variables) ---------------
DATABASE_PATH = Path(os.environ.get("YAMTRACK_DB", "/db/db.sqlite3"))
BUCKET_NAME = os.environ.get("BACKUP_BUCKET", "")
OBJECT_PREFIX = os.environ.get("BACKUP_PREFIX", "daily")
SCHEDULE_TIME = os.environ.get("BACKUP_SCHEDULE", "04:00")
HEALTHCHECK_URL = os.environ.get("BACKUP_HEALTHCHECK_URL")  # optional dead-man switch

# Seconds to wait for a competing writer (a Celery task mid-transaction) to
# release its lock before giving up on the snapshot.
BUSY_TIMEOUT_SECONDS = 30

# Seconds allowed for a healthcheck ping. Monitoring should never be the thing
# that stalls a backup run, so this is deliberately short.
HEALTHCHECK_TIMEOUT_SECONDS = 10.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("yamtrack-backup")


def snapshot_database(source_path: Path, destination_path: Path) -> None:
    """Copy a live SQLite database to a new file using the online backup API.

    Unlike a filesystem copy, this is safe while the database is being written
    to: SQLite coordinates through the same file locks the application holds and
    produces a snapshot that is internally consistent as of a single point in
    time. Required here because Yamtrack runs SQLite in WAL mode, where a naive
    copy can capture a torn state.

    Args:
        source_path: Path to the live db.sqlite3 file.
        destination_path: Path to write the snapshot to. Must not already exist.

    Raises:
        sqlite3.OperationalError: If the database stays locked past the timeout.
    """
    source = sqlite3.connect(source_path, timeout=BUSY_TIMEOUT_SECONDS)
    destination = sqlite3.connect(destination_path)

    def log_progress(status: int, remaining: int, total: int) -> None:
        """Report snapshot progress; called by SQLite after each batch of pages."""
        logger.info("Snapshot progress: %d/%d pages", total - remaining, total)

    try:
        # pages=-1 copies the whole database in a single step, holding a read
        # lock only for the duration of the copy.
        source.backup(destination, pages=-1, progress=log_progress)
    finally:
        destination.close()
        source.close()


def verify_snapshot(snapshot_path: Path) -> None:
    """Run SQLite's integrity check against a snapshot file.

    Verifying the copy rather than the original means a corrupt or truncated
    snapshot is caught here, before it can be uploaded and mistaken for a good
    backup.

    Args:
        snapshot_path: Path to the snapshot produced by snapshot_database().

    Raises:
        RuntimeError: If the integrity check reports anything other than "ok".
    """
    connection = sqlite3.connect(snapshot_path)
    try:
        result = connection.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        connection.close()

    if result != "ok":
        raise RuntimeError(f"Snapshot failed integrity check: {result}")

    logger.info("Snapshot passed integrity check")


def compress(source_path: Path, destination_path: Path) -> Path:
    """Gzip a file, streaming so memory use stays flat regardless of database size.

    Args:
        source_path: File to compress.
        destination_path: Path to write the .gz to.

    Returns:
        The destination path, for chaining.
    """
    with open(source_path, "rb") as raw, gzip.open(destination_path, "wb") as compressed:
        shutil.copyfileobj(raw, compressed)

    original_size = source_path.stat().st_size
    compressed_size = destination_path.stat().st_size
    logger.info(
        "Compressed %.1f MiB to %.1f MiB (%.0f%%)",
        original_size / 1024**2,
        compressed_size / 1024**2,
        compressed_size / max(original_size, 1) * 100,
    )
    return destination_path


def decompress(source_path: Path, destination_path: Path) -> Path:
    """Decompress a gzip archive to a SQLite database file.

    The caller should verify the resulting database before making it live.
    """
    with gzip.open(source_path, "rb") as compressed, open(destination_path, "wb") as raw:
        shutil.copyfileobj(compressed, raw)
    return destination_path


def upload(local_path: Path, bucket_name: str, object_name: str) -> str:
    """Upload a file to Google Cloud Storage.

    Authenticates via Application Default Credentials, so the service account
    key is supplied through GOOGLE_APPLICATION_CREDENTIALS rather than being
    referenced here.

    Args:
        local_path: File to upload.
        bucket_name: Destination GCS bucket, without the gs:// scheme.
        object_name: Full object path within the bucket.

    Returns:
        The gs:// URI of the uploaded object.
    """
    blob = storage.Client().bucket(bucket_name).blob(object_name)

    # if_generation_match=0 means "create only, never overwrite". Combined with
    # an objectCreator-only service account, existing backups cannot be
    # destroyed even if this host is compromised.
    blob.upload_from_filename(
        local_path,
        content_type="application/gzip",
        if_generation_match=0,
    )

    uri = f"gs://{bucket_name}/{object_name}"
    logger.info("Uploaded %s", uri)
    return uri


def download(bucket_name: str, object_name: str, destination_path: Path) -> Path:
    """Download a GCS object to a local file and return its path."""
    blob = storage.Client().bucket(bucket_name).blob(object_name)
    blob.download_to_filename(destination_path)
    logger.info("Downloaded gs://%s/%s", bucket_name, object_name)
    return destination_path


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Split a gs:// URI into its bucket and object name.

    Raises:
        ValueError: If *uri* does not identify a GCS object.
    """
    if not uri.startswith("gs://"):
        raise ValueError("Restore source must be a gs://bucket/object URI")

    bucket_name, separator, object_name = uri[5:].partition("/")
    if not bucket_name or not separator or not object_name:
        raise ValueError("Restore source must include both a bucket and object name")
    return bucket_name, object_name


def restore_database(archive_path: Path, destination_path: Path) -> None:
    """Validate and atomically restore a compressed SQLite backup.

    The replacement is performed only after decompression and SQLite's
    integrity check succeed. SQLite WAL sidecar files from the old database are
    removed after the replacement so they cannot be replayed against it.
    """
    if not destination_path.parent.is_dir():
        raise RuntimeError(f"Database directory does not exist: {destination_path.parent}")

    with tempfile.NamedTemporaryFile(
        prefix=f".{destination_path.name}.restore-",
        suffix=".sqlite3",
        dir=destination_path.parent,
        delete=False,
    ) as temporary:
        restored_path = Path(temporary.name)

    try:
        decompress(archive_path, restored_path)
        verify_snapshot(restored_path)
        os.replace(restored_path, destination_path)
    except Exception:
        restored_path.unlink(missing_ok=True)
        raise

    for sidecar_suffix in ("-wal", "-shm"):
        destination_path.with_name(destination_path.name + sidecar_suffix).unlink(
            missing_ok=True
        )
    logger.info("Restored database to %s", destination_path)


def run_restore(source_uri: str) -> bool:
    """Download a backup from GCS and restore it to ``DATABASE_PATH``."""
    try:
        bucket_name, object_name = parse_gcs_uri(source_uri)
        with tempfile.TemporaryDirectory(prefix="yamtrack-restore-") as workspace:
            archive_path = Path(workspace) / "backup.sqlite3.gz"
            download(bucket_name, object_name, archive_path)
            restore_database(archive_path, DATABASE_PATH)
    except Exception:
        logger.exception("Restore failed; the existing database was not replaced")
        return False
    return True


def ping_healthcheck(url: str | None, *, failed: bool = False) -> None:
    """Report run status to an external dead-man switch, if one is configured.

    Ping failures are logged but never propagate: monitoring must not be able to
    turn a successful backup into a reported failure.

    Args:
        url: Base healthcheck ping URL, or None to skip pinging entirely.
        failed: If True, ping the /fail endpoint to alert immediately rather
            than waiting for the missed-ping grace period to elapse.
    """
    if not url:
        return

    endpoint = f"{url.rstrip('/')}/fail" if failed else url
    try:
        response = httpx.get(endpoint, timeout=HEALTHCHECK_TIMEOUT_SECONDS)
        response.raise_for_status()
        logger.info("Pinged healthcheck (%s)", "failure" if failed else "success")
    except httpx.HTTPError as error:
        logger.warning("Healthcheck ping failed: %s", error)


def run_backup() -> bool:
    """Run one full backup cycle: snapshot, verify, compress, upload, ping.

    Returns:
        True if the backup completed and uploaded successfully, False otherwise.
    """
    if not BUCKET_NAME:
        logger.error("BACKUP_BUCKET is not set")
        return False

    if not DATABASE_PATH.exists():
        logger.error("Database not found at %s", DATABASE_PATH)
        ping_healthcheck(HEALTHCHECK_URL, failed=True)
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    object_name = f"{OBJECT_PREFIX}/yamtrack-{timestamp}.sqlite3.gz"

    # Everything lands in a temp dir that is removed on the way out, whether
    # this run succeeds or fails.
    with tempfile.TemporaryDirectory(prefix="yamtrack-backup-") as workspace:
        snapshot_path = Path(workspace) / "snapshot.sqlite3"
        archive_path = Path(workspace) / "snapshot.sqlite3.gz"

        try:
            logger.info("Snapshotting %s", DATABASE_PATH)
            snapshot_database(DATABASE_PATH, snapshot_path)
            verify_snapshot(snapshot_path)
            compress(snapshot_path, archive_path)
            upload(archive_path, BUCKET_NAME, object_name)
        except Exception:
            logger.exception("Backup failed")
            ping_healthcheck(HEALTHCHECK_URL, failed=True)
            return False

    ping_healthcheck(HEALTHCHECK_URL)
    return True


def seconds_until(target_time: str) -> float:
    """Calculate the delay until the next occurrence of a daily wall-clock time.

    Uses local time, which inside the container is governed by the TZ
    environment variable, so the schedule matches the timezone configured on the
    yamtrack service itself.

    Args:
        target_time: Time of day in 24-hour "HH:MM" form.

    Returns:
        Seconds from now until that time next occurs.

    Raises:
        ValueError: If target_time is not valid "HH:MM".
    """
    hour, minute = (int(part) for part in target_time.split(":", 1))
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise ValueError(f"BACKUP_SCHEDULE out of range: {target_time}")

    now = datetime.now()
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)

    return (next_run - now).total_seconds()


def main(argv: list[str] | None = None) -> int:
    """Entry point: run a single backup or start the daily scheduler.

    Args:
        argv: Command-line arguments, defaulting to sys.argv[1:].

    Returns:
        Process exit code. The scheduler only returns on an unrecoverable
        configuration error; routine backup failures keep it running so a
        transient problem does not take the service down.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once",
        action="store_true",
        help="Take a single backup and exit instead of scheduling daily runs.",
    )
    mode.add_argument(
        "--restore",
        metavar="GS_URI",
        help="Restore this gs://bucket/object backup to YAMTRACK_DB and exit.",
    )
    parser.add_argument(
        "--confirm-restore",
        action="store_true",
        help="Required with --restore because it replaces the current database.",
    )
    args = parser.parse_args(argv)

    if args.confirm_restore and not args.restore:
        parser.error("--confirm-restore can only be used with --restore")

    if args.once:
        return 0 if run_backup() else 1

    if args.restore:
        if not args.confirm_restore:
            logger.error("--restore requires --confirm-restore")
            return 1
        logger.warning("Restoring %s to %s", args.restore, DATABASE_PATH)
        return 0 if run_restore(args.restore) else 1

    try:
        seconds_until(SCHEDULE_TIME)  # fail fast on a malformed schedule
    except ValueError as error:
        logger.error("%s", error)
        return 1

    logger.info("Scheduler started; daily backup at %s local time", SCHEDULE_TIME)
    while True:
        delay = seconds_until(SCHEDULE_TIME)
        logger.info("Next backup in %.1f hours", delay / 3600)
        time.sleep(delay)
        run_backup()


if __name__ == "__main__":
    sys.exit(main())
