"""
Stage 12 — Bankruptcy-alert invite sweep.

Sends bankruptcy-alert invite emails that were scheduled at signup and are now
due (T + BANKRUPTCY_INVITE_DELAY_MINUTES). Mints a fresh Stripe checkout session
per invite at send time and emails the link.

Idempotent + resumable: only processes message_outcomes rows with
send_status='scheduled' and scheduled_send_at <= now; flips them to 'sent'.

Usage:
    python -m src.tasks.bankruptcy_alert_invite_sweep
    python -m src.tasks.bankruptcy_alert_invite_sweep --dry-run

Cron: every 5 minutes.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Optional

from sqlalchemy import text as sa_text

from config.bankruptcy_alert_config import INVITE_TEMPLATE_ID
from src.core.database import get_db_context
from src.services.bankruptcy_alert.invite import send_due_invites

logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> dict:
    with get_db_context() as db:
        if dry_run:
            row = db.execute(sa_text("""
                SELECT COUNT(*) AS c FROM message_outcomes
                WHERE template_id = :tpl AND send_status = 'scheduled'
                  AND scheduled_send_at <= NOW()
            """), {"tpl": INVITE_TEMPLATE_ID}).first()
            summary = {"dry_run": True, "due_now": int(row.c) if row else 0}
        else:
            res = send_due_invites(db)
            summary = {
                "dry_run": False,
                "due": res.due,
                "sent": res.sent,
                "failed": res.failed,
                "skipped": res.skipped,
                "gave_up": res.gave_up,
            }
    logger.info("[bk-invite-sweep] %s", json.dumps(summary, default=str))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    dry_run = "--dry-run" in set(argv or sys.argv[1:])
    summary = run(dry_run=dry_run)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
