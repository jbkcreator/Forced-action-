"""Stored call rows in line with two fixes (2026-11-12). Dry run unless ``--apply``.

Steps (each idempotent):

1. ``bd_unrecorded_link``: BatchDialer calls that were not recorded lose ``recording_view_url`` (it was set on every
   call; the code now sets it only when ``recording_status = 'recorded'``)
2. ``ghl_caller_case``: GHL caller names that differ from a GHL_USER_NAMES name only in case take that spelling
   ("david willis", the outcome webhook's {{user.name}} for user 57m9LlZrO1rV2A5cTKxI, -> "David Willis")
3. ``ghl_hari_k_dot``: the calls of GHL user 0mboWYLanQkMz4RG3IbL ("Hari K.", a different user from "Hari K") that
   were stored as "Hari K" by the old id map. The rows are listed by id: their call messages' ``userId`` was read
   from the GHL API on 2026-11-12 (the stored row keeps no user id)

Nothing is posted: Slack posts and Sheet rows already delivered keep their text.

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_link_and_caller_fix.py           # dry run
    PYTHONPATH=. python migrations/apply_lending_call_link_and_caller_fix.py --apply
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_dispositions import GHL_USER_NAMES, RECORDING_RECORDED
from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HARI_K_DOT_ROW_IDS = (8722, 8723, 8952)

STEPS: tuple[tuple[str, object], ...] = (
    ("bd_unrecorded_link", text(
        "UPDATE lending.call_dispositions SET recording_view_url = NULL, updated_at = now() "
        "WHERE call_source = 'batchdialer' AND recording_view_url IS NOT NULL "
        "AND recording_status IS DISTINCT FROM :recorded")),
    ("ghl_caller_case", text(
        "UPDATE lending.call_dispositions d SET caller_name = n.name, updated_at = now() "
        "FROM unnest(CAST(:names AS text[])) AS n(name) "
        "WHERE d.call_source = 'ghl' AND lower(d.caller_name) = lower(n.name) AND d.caller_name <> n.name")),
    ("ghl_hari_k_dot", text(
        "UPDATE lending.call_dispositions SET caller_name = 'Hari K.', updated_at = now() "
        "WHERE call_source = 'ghl' AND caller_name = 'Hari K' AND id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))),
)


def run_steps(conn: Connection) -> dict[str, int]:
    """Run every step on ``conn`` (the caller commits or rolls back); returns rows changed per step."""
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    params = {"recorded": RECORDING_RECORDED, "names": sorted(set(GHL_USER_NAMES.values())),
              "ids": list(HARI_K_DOT_ROW_IDS)}
    return {name: int(conn.execute(sql, params).rowcount or 0) for name, sql in STEPS}


def apply(engine: Engine | None = None, *, commit: bool = False) -> dict[str, int]:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        with conn.begin() as tx:
            counts = run_steps(conn)
            if commit:
                tx.commit()
            else:
                tx.rollback()
    for name, count in counts.items():
        logger.info("%s %-20s %d row(s)", "applied" if commit else "would change", name, count)
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="commit the changes (default: dry run, rolled back)")
    commit = parser.parse_args().apply
    try:
        apply(commit=commit)
    except Exception as exc:
        if commit:
            raise
        # deploy.sh runs every new migration file without --apply: a dry run changes nothing, so a busy table
        # (lock timeout) must not abort the deploy
        logger.warning("dry run skipped (%s); nothing was changed", type(exc).__name__)
