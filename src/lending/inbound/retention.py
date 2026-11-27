"""Inbound retention purge (R1 0.5 D11): webhook bodies are kept only while they may still be needed.

Daily (cron, ``python -m src.lending.inbound.retention``), while ``LENDING_INBOUND_JOURNAL_ENABLED`` is on:
- ``lending.inbound_events`` rows ``processed`` more than 30 days ago: ``raw`` set to NULL (metadata kept);
- ``lending.webhook_failures`` rows resolved more than 30 days ago: ``payload`` emptied to ``'{}'`` (NOT NULL).
Received / failed journal rows and open or given-up failures keep their body until they are resolved.
Each batch is its own transaction (``FOR UPDATE SKIP LOCKED``) so the 5-minute retry is never blocked.
Logs carry counts and exception classes only.
"""
from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text

from src.lending.db import lending_session
from src.lending.inbound.journal import journal_enabled

logger = logging.getLogger(__name__)

RETENTION_DAYS = 30
BATCH_SIZE = 1000
MAX_BATCHES = 100  # per table per run: a backlog larger than this finishes on the next day's run

_PURGE_EVENTS = text("""
    UPDATE lending.inbound_events SET raw = NULL
    WHERE id IN (
        SELECT id FROM lending.inbound_events
        WHERE status = 'processed' AND raw IS NOT NULL
          AND processed_at < now() - make_interval(days => :days)
        ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED)
""")

_PURGE_FAILURES = text("""
    UPDATE lending.webhook_failures SET payload = '{}'::jsonb
    WHERE id IN (
        SELECT id FROM lending.webhook_failures
        WHERE resolved_at < now() - make_interval(days => :days)
          AND payload <> '{}'::jsonb
        ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED)
""")


@dataclass(frozen=True)
class PurgeResult:
    events_purged: int = 0
    failures_purged: int = 0


def purge(*, session_factory: Callable = lending_session, retention_days: int = RETENTION_DAYS,
          batch_size: int = BATCH_SIZE, max_batches: int = MAX_BATCHES) -> PurgeResult:
    params = {"days": retention_days, "limit": batch_size}
    return PurgeResult(
        events_purged=_purge_table(session_factory, _PURGE_EVENTS, params, max_batches),
        failures_purged=_purge_table(session_factory, _PURGE_FAILURES, params, max_batches),
    )


def _purge_table(session_factory: Callable, statement, params: dict, max_batches: int) -> int:
    total = 0
    for _ in range(max_batches):
        with session_factory() as db:
            count = db.execute(statement, params).rowcount or 0
        total += count
        if count < params["limit"]:
            break
    return total


def main(argv: Optional[list[str]] = None, *, session_factory: Callable = lending_session) -> int:
    parser = argparse.ArgumentParser(description="Purge old inbound webhook bodies (R1 0.5 D11)")
    parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not journal_enabled():
        return 0
    try:
        result = purge(session_factory=session_factory)
    except Exception as exc:  # class only
        logger.error("[lending-inbound-retention] purge failed: %s", type(exc).__name__)
        return 1
    logger.info("[lending-inbound-retention] raw purged: events=%d failures=%d",
                result.events_purged, result.failures_purged)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
