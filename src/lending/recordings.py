"""Is each call's recording readable with our key? 403 means the permission is off: keep checking, never give up."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from sqlalchemy import text

from config.lending_dispositions import RECORDING_BATCH_LIMIT, RECORDING_RETRY_MINUTES

logger = logging.getLogger(__name__)


@dataclass
class CheckStats:
    readable: int = 0
    forbidden: int = 0
    missing: int = 0
    skipped: int = 0


def _outcome(status: int) -> str:
    if status == 200:
        return "readable"
    if status in (401, 403):
        return "forbidden"
    if status == 404:
        return "missing"
    return "skipped"


def check_pending(db, head_status: Callable[[str], int], *, now: Optional[datetime] = None,
                  limit: int = RECORDING_BATCH_LIMIT) -> CheckStats:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=RECORDING_RETRY_MINUTES)
    rows = db.execute(
        text("SELECT id, dialer_call_id, recording_ref, recording_status FROM lending.call_dispositions "
             "WHERE recording_ref IS NOT NULL AND recording_status IN ('pending', 'forbidden') "
             "AND (recording_checked_at IS NULL OR recording_checked_at <= :cutoff) "
             "ORDER BY call_ended_at LIMIT :n FOR UPDATE SKIP LOCKED"),
        {"cutoff": cutoff, "n": limit},
    ).mappings().all()
    stats = CheckStats()
    for row in rows:
        try:
            outcome = _outcome(head_status(row["recording_ref"]))
        except Exception:
            logger.warning("[lending] recording check error call=%s", row["dialer_call_id"], exc_info=True)
            outcome = "skipped"
        if outcome == "forbidden" and row["recording_status"] != "forbidden":
            logger.warning("[lending] recording permission missing for this key, call=%s", row["dialer_call_id"])
        new_status = outcome if outcome != "skipped" else row["recording_status"]
        db.execute(text("UPDATE lending.call_dispositions SET recording_status = :s, recording_checked_at = :now WHERE id = :id"),
                   {"s": new_status, "now": now, "id": row["id"]})
        setattr(stats, outcome, getattr(stats, outcome) + 1)
    db.commit()
    return stats
