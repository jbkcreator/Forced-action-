"""Replay saved webhook failures (``lending.webhook_failures``) by hand: the dead-letter queue CLI (R1 0.5, D8).

Output carries ids, endpoints, GHL contact ids, error classes and payload KEYS only: never a payload value (phones,
emails, message text). A replay runs the row once now through the same path as the 5-minute sweep
(``webhook_failures.replay_one``); a row that had given up is reopened first.

Usage:
    python -m src.lending.inbound.dlq_handler --list [--limit 50]
    python -m src.lending.inbound.dlq_handler --show ID
    python -m src.lending.inbound.dlq_handler --replay ID
    python -m src.lending.inbound.dlq_handler --replay-all ENDPOINT
"""
from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from typing import Optional

from sqlalchemy import text

from src.lending import webhook_failures
from src.lending.db import lending_session

logger = logging.getLogger(__name__)

DEFAULT_LIST_LIMIT = 50
REPLAY_BATCH_SIZE = 50

_COLUMNS = ("id, endpoint, payload, ghl_contact_id, attempts, last_error, created_at, gave_up_at, resolved_at "
            "FROM lending.webhook_failures")
_LIST = text(f"SELECT {_COLUMNS} ORDER BY id DESC LIMIT :limit")
_SHOW = text(f"SELECT {_COLUMNS} WHERE id = :id")
_ONE = text(f"SELECT {_COLUMNS} WHERE id = :id FOR UPDATE")
# Open or given up, not resolved; a row the sweep is replaying right now is skipped, not waited for.
_OF_ENDPOINT = text(f"SELECT {_COLUMNS} WHERE endpoint = :endpoint AND resolved_at IS NULL AND id > :after ORDER BY id LIMIT :limit "
                    "FOR UPDATE SKIP LOCKED")
_REOPEN = text("UPDATE lending.webhook_failures SET gave_up_at = NULL WHERE id = :id")


def _state(row) -> str:
    if row.resolved_at is not None:
        return f"resolved {row.resolved_at}"
    if row.gave_up_at is not None:
        return f"gave up {row.gave_up_at}"
    return "open"


def _payload_keys(payload) -> str:
    """Key names only: the values carry phones, emails and message text."""
    if isinstance(payload, str):
        payload = json.loads(payload)
    return ", ".join(sorted(payload)) if isinstance(payload, dict) else type(payload).__name__


def _describe(row) -> None:
    logger.info("[dlq] #%s %s | GHL contact %s | attempts %s | last error %s | created %s | %s",
                row.id, row.endpoint, row.ghl_contact_id or "unknown", row.attempts, row.last_error or "-",
                row.created_at, _state(row))


def list_failures(db, limit: int) -> bool:
    rows = db.execute(_LIST, {"limit": limit}).all()
    for row in rows:
        _describe(row)
    logger.info("[dlq] %d failure(s) shown (newest first, limit %d)", len(rows), limit)
    return True


def show(db, failure_id: int) -> bool:
    row = db.execute(_SHOW, {"id": failure_id}).first()
    if row is None:
        logger.warning("[dlq] #%s not found", failure_id)
        return False
    _describe(row)
    logger.info("[dlq] #%s payload keys: %s", row.id, _payload_keys(row.payload))
    return True


def _replay(db, row) -> bool:
    """Reopen ``row`` and run it once now; True when it no longer needs replaying (resolved or superseded)."""
    if row.gave_up_at is not None:
        db.execute(_REOPEN, {"id": row.id})
    result = webhook_failures.replay_one(db, row)
    ok = bool(result.resolved or result.superseded)
    outcome = "resolved" if result.resolved else "superseded" if result.superseded else (
        "failed, given up" if result.gave_up else "failed, retried by the sweep")
    logger.info("[dlq] #%s %s: %s", row.id, row.endpoint, outcome)
    return ok


def replay_id(db, failure_id: int) -> bool:
    row = db.execute(_ONE, {"id": failure_id}).first()
    if row is None:
        logger.warning("[dlq] #%s not found", failure_id)
        return False
    if row.resolved_at is not None:
        logger.info("[dlq] #%s already resolved: not replayed", row.id)
        return True
    return _replay(db, row)


def replay_endpoint(db, endpoint: str) -> bool:
    """Replay each open or given-up failure of ``endpoint`` once; True only when every one succeeded."""
    after = succeeded = failed = 0
    while rows := db.execute(_OF_ENDPOINT, {"endpoint": endpoint, "after": after, "limit": REPLAY_BATCH_SIZE}).all():
        for row in rows:
            if _replay(db, row):
                succeeded += 1
            else:
                failed += 1
        after = rows[-1].id
        db.commit()
    logger.info("[dlq] %s: %d replayed, %d still failing", endpoint, succeeded, failed)
    return failed == 0


def main(argv: Optional[list[str]] = None, *, session_factory: Callable = lending_session) -> int:
    parser = argparse.ArgumentParser(description="Replay saved lending webhook failures (R1 0.5)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="the latest saved failures, newest first")
    group.add_argument("--show", type=int, metavar="ID", help="one saved failure (payload keys only)")
    group.add_argument("--replay", type=int, metavar="ID", help="run one saved failure once now (reopens a given-up one)")
    group.add_argument("--replay-all", metavar="ENDPOINT", help="run every open or given-up failure of ENDPOINT once")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIST_LIMIT, help="rows shown by --list")
    args = parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.replay is not None or args.replay_all is not None:
        from src.lending.inbound import journal

        journal.load_handlers()
    with session_factory() as db:
        if args.list:
            ok = list_failures(db, args.limit)
        elif args.show is not None:
            ok = show(db, args.show)
        elif args.replay is not None:
            ok = replay_id(db, args.replay)
        else:
            ok = replay_endpoint(db, args.replay_all)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
