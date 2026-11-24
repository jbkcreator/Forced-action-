"""R1 0.7: prove the latest backup restores and keeps the append-only audit rows intact.

    PYTHONPATH=. python scripts/verify_backup_restore.py --scratch-url postgresql://user:pass@host/restore_check_test

Restores the newest dump from ``scripts/pg_backup.py`` into a SCRATCH database (never production: the name must
contain "test" and differ from the app database; it is overwritten) and checks that

  * the dump is recent (default: under 26 hours old),
  * every audit table holds at least the rows counted when the dump was taken and no more than production holds now
    (append-only tables only grow, so a restore sits between the two),
  * the restored ``lender_rule_snapshots`` still refuses UPDATE and DELETE (the append-only triggers came back).

Exits non-zero on any failure.  Production is only read (row counts).
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, make_url

from config.settings import get_settings
from scripts.pg_backup import DEFAULT_DEST, audit_counts, pg_env

logger = logging.getLogger("verify_backup_restore")

MAX_AGE_HOURS = 26
RESTORE_TIMEOUT_SECONDS = 3 * 3600


def latest_dump(dest: Path, database: str) -> Path:
    dumps = sorted(dest.glob(f"{database}_*.dump"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not dumps:
        raise SystemExit(f"no dump found in {dest}")
    return dumps[0]


def check_scratch(scratch: URL, prod: URL) -> None:
    if "test" not in (scratch.database or "").lower():
        raise SystemExit("refusing: the scratch database name must contain 'test'")
    if (scratch.host, scratch.port or 5432, scratch.database) == (prod.host, prod.port or 5432, prod.database):
        raise SystemExit("refusing: the scratch database is the production database")


def compare_counts(taken: dict[str, int], restored: dict[str, int], prod_now: dict[str, int]) -> list[str]:
    """Problems with the restored audit counts; empty when each table sits between its dump-time and current count."""
    problems = []
    for table, at_dump in taken.items():
        if table not in restored:
            problems.append(f"{table}: missing after restore")
        elif restored[table] < at_dump:
            problems.append(f"{table}: restored {restored[table]} rows, {at_dump} existed when the dump was taken")
        elif table in prod_now and restored[table] > prod_now[table]:
            problems.append(f"{table}: restored {restored[table]} rows, production holds only {prod_now[table]}")
    return problems


def append_only_holds(scratch: URL) -> bool:
    """True when the restored snapshot table refuses UPDATE and DELETE (checked in transactions that roll back)."""
    engine = create_engine(scratch)
    try:
        with engine.connect() as conn:
            if conn.execute(text("SELECT to_regclass('lending.lender_rule_snapshots')")).scalar() is None:
                return True  # not installed yet: nothing to check
            for statement in ("UPDATE lending.lender_rule_snapshots SET reason = reason",
                              "DELETE FROM lending.lender_rule_snapshots"):
                tx = conn.begin()
                try:
                    conn.execute(text(statement))
                    return False
                except Exception:
                    pass
                finally:
                    tx.rollback()
    finally:
        engine.dispose()
    return True


def run(dest: Path, scratch_url: str, max_age_hours: int) -> list[str]:
    prod = make_url(str(get_settings().database_url))
    scratch = make_url(scratch_url)
    check_scratch(scratch, prod)
    dump = latest_dump(dest, prod.database or "")
    age_hours = (time.time() - dump.stat().st_mtime) / 3600
    problems: list[str] = []
    if age_hours > max_age_hours:
        problems.append(f"{dump.name} is {age_hours:.0f} hours old (limit {max_age_hours})")

    counts_file = dump.with_name(dump.name + ".counts.json")
    if not counts_file.exists():
        raise SystemExit(f"{counts_file.name} is missing: cannot compare the restore")
    taken = json.loads(counts_file.read_text(encoding="utf-8"))["counts"]

    restore = subprocess.run(
        ["pg_restore", "--clean", "--if-exists", "--no-owner", "--dbname", scratch.database or "", str(dump)],
        env=pg_env(scratch), capture_output=True, text=True, timeout=RESTORE_TIMEOUT_SECONDS)
    if restore.returncode != 0:
        problems.append(f"pg_restore exit {restore.returncode}: {restore.stderr.strip()[-300:]}")

    problems += compare_counts(taken, audit_counts(scratch), audit_counts(prod))
    if not append_only_holds(scratch):
        problems.append("lender_rule_snapshots accepted an UPDATE or DELETE after the restore")
    logger.info("verified %s: %d audit table(s) checked, %d problem(s)", dump.name, len(taken), len(problems))
    return problems


def parse(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scratch-url", required=True, help="a throwaway database to restore into (its name must contain 'test')")
    parser.add_argument("--dest", default=DEFAULT_DEST)
    parser.add_argument("--max-age-hours", type=int, default=MAX_AGE_HOURS)
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse()
    found = run(Path(args.dest), args.scratch_url, args.max_age_hours)
    for problem in found:
        logger.error("PROBLEM: %s", problem)
    sys.exit(1 if found else 0)
