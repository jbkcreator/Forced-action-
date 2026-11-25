"""R1 0.7: nightly logical backup of the app database (pg_dump custom format), seven days kept.

    PYTHONPATH=. python scripts/pg_backup.py [--dest /var/backups/postgres] [--keep-days 7]

Writes ``<dest>/<database>_<UTC timestamp>.dump`` plus ``<same name>.counts.json``: the row counts of the append-only
audit tables at dump time, which ``scripts/verify_backup_restore.py`` checks a restore against.  The dump is written
to a temporary name, listed with pg_restore to prove it is a readable archive, then renamed, so a failed run never
leaves a file that looks like a backup.  Exits non-zero on any failure (cron mails it).

A dump on the same server does not survive losing the disk; the provider's disk backups cover that.
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, make_url

from config.settings import get_settings

logger = logging.getLogger("pg_backup")

DEFAULT_DEST = "/var/backups/postgres"
DEFAULT_KEEP_DAYS = 7
DUMP_TIMEOUT_SECONDS = 3 * 3600

# Append-only tables whose counts a restore must reproduce (a table that does not exist yet is skipped).
AUDIT_TABLES = (
    "lending.lender_rule_snapshots",
    "lending.file_routing_events",
    "lending.card_edits",
    "lending.loan_stage_events",
    "lending.deal_audit_events",
)


def pg_env(url: URL) -> dict[str, str]:
    """Connection settings for the pg_* tools as environment variables, so the password is never on a command line."""
    env = {"PGHOST": url.host or "localhost", "PGPORT": str(url.port or 5432), "PGUSER": url.username or "",
           "PGDATABASE": url.database or ""}
    if url.password:
        env["PGPASSWORD"] = url.password
    return env


def audit_counts(url: URL) -> dict[str, int]:
    """Row count of each audit table that exists in the database at ``url``."""
    engine = create_engine(url, pool_pre_ping=True)
    counts: dict[str, int] = {}
    try:
        with engine.connect() as conn:
            for table in AUDIT_TABLES:
                if conn.execute(text("SELECT to_regclass(:name)"), {"name": table}).scalar() is not None:
                    counts[table] = int(conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())
    finally:
        engine.dispose()
    return counts


def prune(dest: Path, database: str, keep_days: int, now: Optional[float] = None) -> list[Path]:
    """Delete dumps (and their counts files) older than ``keep_days``; the newest dump is never deleted."""
    now = now or time.time()
    dumps = sorted(dest.glob(f"{database}_*.dump"), key=lambda p: p.stat().st_mtime, reverse=True)
    removed = []
    for dump in dumps[1:]:
        if now - dump.stat().st_mtime > keep_days * 86400:
            dump.unlink()
            dump.with_name(dump.name + ".counts.json").unlink(missing_ok=True)
            removed.append(dump)
    return removed


def run(dest: Path, keep_days: int) -> Path:
    url = make_url(str(get_settings().database_url))
    dest.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    final = dest / f"{url.database}_{stamp}.dump"
    partial = final.with_suffix(".dump.partial")
    env = pg_env(url)

    counts = audit_counts(url)  # taken just before the dump: a restore holds at least these rows
    try:
        subprocess.run(["pg_dump", "--format=custom", "--no-owner", "--file", str(partial)], env=env, check=True,
                       capture_output=True, text=True, timeout=DUMP_TIMEOUT_SECONDS)
        subprocess.run(["pg_restore", "--list", str(partial)], env=env, check=True, capture_output=True, text=True,
                       timeout=600)
    except subprocess.CalledProcessError as exc:
        partial.unlink(missing_ok=True)
        raise SystemExit(f"backup failed: {exc.cmd[0]} exit {exc.returncode}: {(exc.stderr or '').strip()[-400:]}") from None
    except subprocess.TimeoutExpired:
        partial.unlink(missing_ok=True)
        raise SystemExit("backup failed: timed out") from None
    partial.chmod(0o600)
    partial.rename(final)
    final.with_name(final.name + ".counts.json").write_text(
        json.dumps({"taken_at": stamp, "counts": counts}, indent=1), encoding="utf-8")
    removed = prune(dest, url.database or "", keep_days)
    logger.info("backup written %s (%.1f MB); %d old dump(s) removed", final.name, final.stat().st_size / 1e6, len(removed))
    return final


def parse(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dest", default=DEFAULT_DEST)
    parser.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS)
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse()
    run(Path(args.dest), args.keep_days)
    sys.exit(0)
