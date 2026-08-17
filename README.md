# yamtrack-backup

[![Build](https://github.com/SShah7433/yamtrack-backup/actions/workflows/build.yml/badge.svg)](https://github.com/SShah7433/yamtrack-backup/actions/workflows/build.yml)

Daily off-site backup of a [Yamtrack](https://github.com/FuzzyGrim/Yamtrack)
SQLite database to Google Cloud Storage.

Runs as a sidecar container alongside `yamtrack`. Each night it takes a
consistent snapshot of the live database, verifies it, compresses it, and
uploads it to a GCS bucket under a timestamped object name.

This is only for SQLite deployments. If you run `docker-compose.postgres.yml`,
use `pg_dump` against the `yamtrack-db` container instead.

## How it works

1. **Snapshot** — opens the live `db.sqlite3` through SQLite's online backup
   API. Yamtrack runs SQLite in WAL mode, so a plain file copy can capture a
   torn state; the backup API cannot. The container reads the same bind-mounted
   file the app writes to, and the two processes coordinate through ordinary
   POSIX file locks.
2. **Verify** — runs `PRAGMA integrity_check` against the *snapshot*, so a bad
   copy fails here rather than silently replacing good history in the bucket.
3. **Compress** — streams the snapshot through gzip, so memory use stays flat
   no matter how large the database grows.
4. **Upload** — writes to GCS with `if_generation_match=0`, meaning create-only.
   Nothing this container does can overwrite an existing backup.
5. **Ping** — optionally notifies a dead-man switch so you hear about a backup
   that stopped running.

Retention is handled by a bucket lifecycle rule rather than by this container,
so old backups are still pruned even if the host disappears.

## Quick start

```yaml
services:
  backup:
    image: ghcr.io/sshah7433/yamtrack-backup:latest
    container_name: yamtrack-backup
    restart: unless-stopped
    environment:
      - TZ=Europe/Berlin
      - BACKUP_BUCKET=your-yamtrack-backups
      - YAMTRACK_DB=/db/db.sqlite3
      - GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcs-key.json
    volumes:
      - ./db:/db
      - ./gcs-key.json:/secrets/gcs-key.json:ro
```

See [docker-compose.example.yml](docker-compose.example.yml) for the fully
commented version.

Take a backup immediately, without waiting for the schedule:

```bash
docker compose run --rm backup --once
```

Watch the scheduler:

```bash
docker logs -f yamtrack-backup
```

A failed run logs the traceback, pings the healthcheck `/fail` endpoint, and
leaves the scheduler running — a transient GCS error should not take the
service down and start a restart loop.

## Google Cloud setup

You need a bucket and a service account with `roles/storage.objectCreator`
**only** on that bucket. That grants write-new and nothing else — no read, no
overwrite, no delete. If the host is ever compromised, an attacker can add junk
objects but cannot destroy your backup history.

```bash
PROJECT=$(gcloud config get-value project)
BUCKET=your-yamtrack-backups
SA=yamtrack-backup@$PROJECT.iam.gserviceaccount.com

gcloud storage buckets create "gs://$BUCKET" \
  --location=us-central1 --uniform-bucket-level-access \
  --default-storage-class=NEARLINE

# Retention: delete objects older than 30 days, server-side.
echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":30}}]}' > lifecycle.json
gcloud storage buckets update "gs://$BUCKET" --lifecycle-file=lifecycle.json

gcloud iam service-accounts create yamtrack-backup
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA" --role=roles/storage.objectCreator
gcloud iam service-accounts keys create ./gcs-key.json --iam-account="$SA"
chmod 600 ./gcs-key.json
```

Nearline is roughly half the price of Standard and its 30-day minimum storage
duration lines up exactly with the lifecycle rule above, so there is no
early-deletion charge.

The backup service account needs only `roles/storage.objectCreator`. A restore
needs read access as well, so use separate credentials with
`roles/storage.objectViewer` when performing one.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BACKUP_BUCKET` | *(required)* | Destination bucket, no `gs://` prefix. |
| `BACKUP_PREFIX` | `daily` | Object path prefix within the bucket. |
| `BACKUP_SCHEDULE` | `04:00` | Daily run time, 24-hour local clock. |
| `YAMTRACK_DB` | `/db/db.sqlite3` | Path to the database inside the container. |
| `TZ` | container default | Timezone `BACKUP_SCHEDULE` is interpreted in. |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to the mounted service account key. |
| `BACKUP_HEALTHCHECK_URL` | unset | Dead-man switch ping URL, e.g. healthchecks.io. |

## Restoring

Stop Yamtrack first, then run the restore command using credentials that can
read the selected backup object:

```bash
docker compose stop yamtrack
docker compose run --rm backup \
  --restore gs://your-yamtrack-backups/daily/yamtrack-2026-08-15T040000Z.sqlite3.gz \
  --confirm-restore
docker compose start yamtrack
```

The restore downloads, decompresses, and integrity-checks the backup before
atomically replacing `YAMTRACK_DB`; it also removes stale `-wal` and `-shm`
sidecars. `--confirm-restore` is required because this permanently replaces
the database. Never restore while Yamtrack is running.

Do this once into a throwaway copy of the stack now, while nothing is broken,
so you know the chain works end to end.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest
pytest -q
```

The suite builds a real WAL-mode database with a concurrent writer and asserts
the snapshot round-trips through gzip intact, that writes made after the
snapshot started are correctly excluded, that corrupt snapshots are rejected
before upload, and that the schedule arithmetic rolls over midnight correctly.

Build the image locally:

```bash
docker build -t yamtrack-backup .
```

## Releases

Pushes to `main` publish `ghcr.io/sshah7433/yamtrack-backup:main` and `:latest`.
Tagging a release publishes semver tags:

```bash
git tag v1.0.0 && git push --tags
```

produces `:1.0.0`, `:1.0`, and `:latest`. Images are built for `linux/amd64`
and `linux/arm64`, so the same tag runs on a Raspberry Pi.

Pull requests build the image but never push it.

> **Note:** GHCR packages are private by default. After the first successful
> build, open the package under your GitHub profile → Packages → Package
> settings and set visibility to public if you want to pull it without
> authenticating.

## Limitations

- **SQLite only.** Postgres deployments need `pg_dump`.
- **Local filesystem only.** SQLite's locking is unreliable over NFS, so the
  database must be on a local disk for the snapshot to be trustworthy.
- **Not a substitute for Yamtrack's CSV export.** The snapshot is complete and
  is the right disaster-recovery tool, but a periodic export from
  `/settings/export` is worth keeping as a format-independent escape hatch
  against silent corruption or a bad migration replicating into every snapshot.
