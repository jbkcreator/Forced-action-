"""Which LendingFlow leads qualify for replacement credit (Runbook § 4.1, Item 1).

Runbook: "leads with 5+ attempts over 5+ days are eligible for replacement credit". For each LendingFlow lead this
counts the outbound call attempts on its phone since it arrived, from any source (BatchDialer and GoHighLevel, see
``src.lending.ghl_call_import``), and the distinct Eastern call days. Thresholds live in config/lending_followup.py.

A query rather than a database view: apply_lending_call_dispositions_dialer.py re-applies column types on every run,
and PostgreSQL refuses that while a view reads the column.

Report (ids, counts and dates only):

    python -m src.lending.replacement_eligibility [--eligible-only]
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from sqlalchemy import text

from config.lending_followup import (
    CALL_SOURCE_GHL,
    REPLACEMENT_DAY_TIMEZONE,
    REPLACEMENT_MIN_ATTEMPTS,
    REPLACEMENT_MIN_CALL_DAYS,
)

logger = logging.getLogger(__name__)

_ELIGIBILITY = text("""
    WITH attempts AS (
        SELECT l.id AS lead_id, d.call_source, d.talk_duration_sec,
               COALESCE(d.call_started_at, d.call_ended_at) AS attempted_at
          FROM lending.lendingflow_leads l
          JOIN lending.call_dispositions d
            ON d.phone = l.phone
           AND d.direction = 'outbound'
           AND COALESCE(d.call_started_at, d.call_ended_at) >= l.received_at
         WHERE (CAST(:lead_ids AS bigint[]) IS NULL OR l.id = ANY(CAST(:lead_ids AS bigint[])))
    )
    SELECT l.id AS lead_id,
           l.received_at,
           count(a.lead_id) AS attempts,
           count(DISTINCT (a.attempted_at AT TIME ZONE :tz)::date) AS call_days,
           count(a.lead_id) FILTER (WHERE a.call_source = :ghl) AS ghl_attempts,
           count(a.lead_id) FILTER (WHERE COALESCE(a.talk_duration_sec, 0) > 0) AS connected,
           min(a.attempted_at) AS first_attempt_at,
           max(a.attempted_at) AS last_attempt_at
      FROM lending.lendingflow_leads l
      LEFT JOIN attempts a ON a.lead_id = l.id
     WHERE (CAST(:lead_ids AS bigint[]) IS NULL OR l.id = ANY(CAST(:lead_ids AS bigint[])))
     GROUP BY l.id, l.received_at
     ORDER BY l.received_at DESC
""")


@dataclass(frozen=True)
class LeadAttempts:
    lead_id: int
    received_at: datetime
    attempts: int
    call_days: int
    ghl_attempts: int
    connected: int
    first_attempt_at: Optional[datetime]
    last_attempt_at: Optional[datetime]

    @property
    def eligible(self) -> bool:
        return self.attempts >= REPLACEMENT_MIN_ATTEMPTS and self.call_days >= REPLACEMENT_MIN_CALL_DAYS


def lead_attempts(db, lead_ids: Optional[Sequence[int]] = None) -> list[LeadAttempts]:
    """Attempt counts per LendingFlow lead, newest lead first; all leads when ``lead_ids`` is None."""
    rows = db.execute(_ELIGIBILITY, {"lead_ids": list(lead_ids) if lead_ids is not None else None,
                                     "tz": REPLACEMENT_DAY_TIMEZONE, "ghl": CALL_SOURCE_GHL}).all()
    return [LeadAttempts(*row) for row in rows]


def main(argv: Optional[Sequence[str]] = None) -> int:
    from src.lending.db import lending_session

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="LendingFlow leads and the replacement rule "
                                                 f"({REPLACEMENT_MIN_ATTEMPTS}+ attempts over "
                                                 f"{REPLACEMENT_MIN_CALL_DAYS}+ days)")
    parser.add_argument("--eligible-only", action="store_true")
    args = parser.parse_args(argv)
    with lending_session() as db:
        leads = lead_attempts(db)
    for lead in leads:
        if lead.eligible or not args.eligible_only:
            logger.info("[replacement] lead=%s received=%s attempts=%d call_days=%d ghl=%d connected=%d first=%s "
                        "last=%s eligible=%s", lead.lead_id, lead.received_at, lead.attempts, lead.call_days,
                        lead.ghl_attempts, lead.connected, lead.first_attempt_at, lead.last_attempt_at,
                        lead.eligible)
    logger.info("[replacement] leads=%d eligible=%d", len(leads), sum(lead.eligible for lead in leads))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
