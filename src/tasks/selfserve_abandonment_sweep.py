"""WP-7 — mark stale self-serve sessions abandoned.

A borrower who starts the flow and doesn't finish still has real data worth
keeping (plan §7 Q8) — this sweep just makes the session's status honest so
anything downstream (reporting, a future rescue-outreach agent) can find it.
Never touches 'confirmed' sessions — a borrower who finished the form but
whose handoff is on hold (suppression, plan §7 Q7) is not abandoned.

Dry-run unless --apply, matching this codebase's sweep-task convention
(src/tasks/learning_hygiene_sweep.py).

Usage:
    PYTHONPATH=. python -m src.tasks.selfserve_abandonment_sweep
    PYTHONPATH=. python -m src.tasks.selfserve_abandonment_sweep --apply
"""
from __future__ import annotations

import argparse
import logging

from src.core.database import get_db_context
from src.services.selfserve_sessions import list_stale_session_tokens, mark_abandoned

logger = logging.getLogger(__name__)

ABANDON_AFTER_HOURS = 24


def run(dry_run: bool = True, older_than_hours: int = ABANDON_AFTER_HOURS) -> dict:
    with get_db_context() as db:
        tokens = list_stale_session_tokens(db, older_than_hours)
        if not dry_run:
            for token in tokens:
                mark_abandoned(db, token)
            db.commit()

    logger.info(
        "selfserve_abandonment_sweep: %d session(s) %s",
        len(tokens),
        "marked abandoned" if not dry_run else "would be marked abandoned (dry run)",
    )
    return {"count": len(tokens), "dry_run": dry_run, "tokens": tokens}


def main() -> None:
    parser = argparse.ArgumentParser(description="Mark stale self-serve sessions abandoned.")
    parser.add_argument("--apply", action="store_true", help="Actually mark sessions abandoned (default: dry run)")
    parser.add_argument("--older-than-hours", type=int, default=ABANDON_AFTER_HOURS)
    args = parser.parse_args()
    result = run(dry_run=not args.apply, older_than_hours=args.older_than_hours)
    print(result)


if __name__ == "__main__":
    main()
