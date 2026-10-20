"""Ops check: which launch queue the caller should be on now, and its line setting.

Runbook Principle 1 (strict order: partners first, cold maturity last) and Principle 4
(max lines per campaign). Counts are contacts dialable now per queue: active in
``lending.dialer_load_records``, no open ``lending.dialer_holds`` hold, and not blocked by the
compliance rules right now (suppression, attempt caps, call window, stale scrub); BatchDialer itself is not called. Read only.

Usage:
    python -m src.tasks.lending_dial_now
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from sqlalchemy import text

from config.lending_queues import DIAL_ORDER, MAX_LINES
from src.core.database import get_db_context
from src.lending.compliance import blocked_from_dialing
from src.lending.dial_order import queue_to_dial_now

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_NOTHING_TO_DIAL = 5


def active_by_queue(db, *, now: Optional[datetime] = None) -> dict[str, int]:
    """Dialable contacts per queue: active, no open hold, and not blocked by the rules right now
    (a failed dialer removal leaves an attempt-capped contact active with no hold)."""
    rows = db.execute(text(
        "SELECT r.pool, r.phone FROM lending.dialer_load_records r "
        "WHERE r.active AND NOT EXISTS (SELECT 1 FROM lending.dialer_holds h "
        "WHERE h.phone = r.phone AND h.released_at IS NULL)"
    )).all()
    blocked = blocked_from_dialing(db, [phone for _, phone in rows], now or datetime.now(timezone.utc))
    return dict(Counter(str(pool) for pool, phone in rows if phone not in blocked))


def dial_now_summary(active: Mapping[str, int]) -> dict[str, Any]:
    queue = queue_to_dial_now(active)
    return {
        "queue": queue,
        "max_lines": MAX_LINES.get(queue) if queue else None,
        "active_by_queue": {q: int(active.get(q, 0)) for q in DIAL_ORDER},
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args(argv)
    with get_db_context() as session:
        summary = dial_now_summary(active_by_queue(session))
    logger.info("[dial-now] %s", json.dumps(summary))
    return EXIT_OK if summary["queue"] else EXIT_NOTHING_TO_DIAL


if __name__ == "__main__":
    raise SystemExit(main())
