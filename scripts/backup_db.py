"""
Database backup script — pg_dump → S3.

Usage:
    python scripts/backup_db.py           # daily backup
    python scripts/backup_db.py --weekly  # weekly backup (run on Sundays)
    python scripts/backup_db.py --restore --file forcedaction_2026-04-06_0100.dump

Cron (add to crontab):
    0 1 * * *   cd /app && python scripts/backup_db.py          >> /var/log/fa-backup.log 2>&1
    0 1 * * 0   cd /app && python scripts/backup_db.py --weekly >> /var/log/fa-backup.log 2>&1

Required .env:
    BACKUP_S3_BUCKET, BACKUP_AWS_ACCESS_KEY_ID, BACKUP_AWS_SECRET_ACCESS_KEY
    DATABASE_URL (already set)

Optional .env:
    BACKUP_S3_PREFIX      (default: db-backups)
    BACKUP_S3_REGION      (default: us-east-1)
    BACKUP_RETENTION_DAILY  (default: 7)
    BACKUP_RETENTION_WEEKLY (default: 4)
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import get_settings

logging.basicConfig(
    format="%(asctime)s [backup] %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_db_url(database_url: str) -> dict:
    """Extract pg_dump connection args from DATABASE_URL."""
    parsed = urlparse(database_url)
    return {
        "host":     parsed.hostname or "localhost",
        "port":     str(parsed.port or 5432),
        "user":     parsed.username or "postgres",
        "password": parsed.password or "",
        "dbname":   parsed.path.lstrip("/"),
    }


def _s3_client(settings):
    return boto3.client(
        "s3",
        region_name=settings.backup_s3_region,
        aws_access_key_id=settings.backup_aws_access_key_id,
        aws_secret_access_key=(
            settings.backup_aws_secret_access_key.get_secret_value()
            if settings.backup_aws_secret_access_key else None
        ),
    )


def run_backup(weekly: bool = False) -> str:
    """
    Dump the database, upload to S3, prune old backups.
    Returns the S3 key of the uploaded file.
    """
    settings = get_settings()

    if not settings.backup_s3_bucket:
        logger.error("BACKUP_S3_BUCKET is not set — aborting")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    tag = "weekly" if weekly else "daily"
    filename = f"forcedaction_{now.strftime('%Y-%m-%d_%H%M')}_{tag}.dump"
    s3_key = f"{settings.backup_s3_prefix}/{tag}/{filename}"

    db = _parse_db_url(settings.database_url)
    env = {**os.environ, "PGPASSWORD": db["password"]}

    logger.info("Starting %s backup → s3://%s/%s", tag, settings.backup_s3_bucket, s3_key)

    with tempfile.NamedTemporaryFile(suffix=".dump", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # --- dump ---
        cmd = [
            "pg_dump",
            "--format=custom",
            "--compress=9",
            f"--host={db['host']}",
            f"--port={db['port']}",
            f"--username={db['user']}",
            f"--dbname={db['dbname']}",
            f"--file={tmp_path}",
        ]
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("pg_dump failed: %s", result.stderr)
            sys.exit(1)

        size_mb = os.path.getsize(tmp_path) / 1_048_576
        logger.info("Dump complete — %.1f MB", size_mb)

        # --- upload ---
        s3 = _s3_client(settings)
        s3.upload_file(tmp_path, settings.backup_s3_bucket, s3_key)
        logger.info("Uploaded to s3://%s/%s", settings.backup_s3_bucket, s3_key)

    finally:
        os.unlink(tmp_path)

    # --- prune old backups ---
    _prune(s3, settings, tag)

    logger.info("Backup complete — %s", s3_key)
    return s3_key


def _prune(s3, settings, tag: str):
    """Delete backups beyond retention window."""
    retention = (
        settings.backup_retention_weekly if tag == "weekly"
        else settings.backup_retention_daily
    )
    prefix = f"{settings.backup_s3_prefix}/{tag}/"

    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=settings.backup_s3_bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))

    # sort oldest first
    objects.sort(key=lambda o: o["LastModified"])

    to_delete = objects[:-retention] if len(objects) > retention else []
    for obj in to_delete:
        s3.delete_object(Bucket=settings.backup_s3_bucket, Key=obj["Key"])
        logger.info("Pruned old backup: %s", obj["Key"])

    logger.info(
        "Retention: keeping %d/%d %s backups",
        min(len(objects), retention), len(objects), tag,
    )


def run_restore(filename: str):
    """
    Download a backup from S3 and restore it to the database.

    WARNING: This will DROP and recreate all tables. Run only on a test instance
    or when recovering from data loss. Always confirm before running in production.

    Usage:
        python scripts/backup_db.py --restore --file forcedaction_2026-04-06_0100_daily.dump
    """
    settings = get_settings()

    if not settings.backup_s3_bucket:
        logger.error("BACKUP_S3_BUCKET is not set — aborting")
        sys.exit(1)

    # search daily then weekly prefix
    s3 = _s3_client(settings)
    s3_key = None
    for tag in ("daily", "weekly"):
        candidate = f"{settings.backup_s3_prefix}/{tag}/{filename}"
        try:
            s3.head_object(Bucket=settings.backup_s3_bucket, Key=candidate)
            s3_key = candidate
            break
        except ClientError:
            continue

    if not s3_key:
        logger.error("File %s not found in s3://%s", filename, settings.backup_s3_bucket)
        sys.exit(1)

    db = _parse_db_url(settings.database_url)
    env = {**os.environ, "PGPASSWORD": db["password"]}

    with tempfile.NamedTemporaryFile(suffix=".dump", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        logger.info("Downloading s3://%s/%s", settings.backup_s3_bucket, s3_key)
        s3.download_file(settings.backup_s3_bucket, s3_key, tmp_path)

        logger.info("Restoring to %s@%s/%s", db["user"], db["host"], db["dbname"])
        cmd = [
            "pg_restore",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
            f"--host={db['host']}",
            f"--port={db['port']}",
            f"--username={db['user']}",
            f"--dbname={db['dbname']}",
            tmp_path,
        ]
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            # pg_restore exits non-zero on warnings too — log but don't fail on warnings
            if "error" in result.stderr.lower():
                logger.error("pg_restore errors: %s", result.stderr)
                sys.exit(1)
            logger.warning("pg_restore warnings (non-fatal): %s", result.stderr)

        logger.info("Restore complete")

    finally:
        os.unlink(tmp_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Forced Action DB backup/restore")
    parser.add_argument("--weekly",  action="store_true", help="Tag as weekly backup")
    parser.add_argument("--restore", action="store_true", help="Restore mode")
    parser.add_argument("--file",    type=str,            help="Filename to restore (--restore only)")
    args = parser.parse_args()

    if args.restore:
        if not args.file:
            logger.error("--restore requires --file <filename>")
            sys.exit(1)
        run_restore(args.file)
    else:
        run_backup(weekly=args.weekly)
