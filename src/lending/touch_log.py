"""Every call, text decision and outcome we hold for one phone, newest first (Q66).

Nothing new is stored: each event is already written by the job that handles it, so this reads those tables together
instead of copying them (and never puts a view on ``call_dispositions``, which would block its migrations).

* calls: ``lending.call_dispositions`` (BatchDialer and GHL, outbound and inbound, answered or not)
* texts: ``lending.missed_call_events`` (the missed-call text written by ``text_back.py``: its status is the decision,
  sent or why not; ``lending.missed_call_texts`` is the superseded #320 table and is not read)
* outcomes: ``lending.ghl_stage_events`` (pipeline stage entries such as Booked or Funded) and
  ``lending.file_routing_events`` (routed, declined, approved, funded) of the phone's LendingFlow leads

Read-only. Test rows are not hidden here: this is a per-phone history, not a report.

    python -m src.lending.touch_log --phone +17275550123
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 200

_TOUCHES = text("""
    SELECT at, kind, direction, outcome, ref FROM (
        SELECT COALESCE(call_started_at, call_ended_at, created_at) AS at, 'call' AS kind, direction,
               COALESCE(disposition, disposition_raw) AS outcome, dialer_call_id AS ref
          FROM lending.call_dispositions WHERE phone = :phone
        UNION ALL
        SELECT COALESCE(decided_at, created_at), 'text', 'outbound', status, dialer_call_id
          FROM lending.missed_call_events WHERE phone = :phone
        UNION ALL
        SELECT event_at, 'stage', NULL, stage_name, ghl_opportunity_id
          FROM lending.ghl_stage_events WHERE phone = :phone
        UNION ALL
        SELECT e.created_at, 'routing', NULL, e.action, e.lead_id::text
          FROM lending.file_routing_events e JOIN lending.lendingflow_leads l ON l.id = e.lead_id
         WHERE l.phone = :phone
    ) t
    ORDER BY at DESC NULLS LAST
    LIMIT :limit
""")


@dataclass(frozen=True)
class Touch:
    at: Optional[datetime]
    kind: str  # call | text | stage | routing
    direction: Optional[str]  # inbound | outbound | None
    outcome: Optional[str]
    ref: Optional[str]  # call id, opportunity id or lead id: never a name, number or message text


def touches(db, phone: str, *, limit: int = DEFAULT_LIMIT) -> list[Touch]:
    """The touches of ``phone`` (any format phone_utils understands), newest first; empty for an invalid number."""
    normalized = normalize(phone) if phone else None
    if not normalized:
        return []
    rows = db.execute(_TOUCHES, {"phone": normalized, "limit": limit}).all()
    return [Touch(row.at, row.kind, row.direction, row.outcome, row.ref) for row in rows]


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Every call, text decision and outcome held for one phone (Q66)")
    parser.add_argument("--phone", required=True)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from src.lending.db import lending_session

    with lending_session() as db:
        found = touches(db, args.phone, limit=args.limit)
    for touch in found:
        logger.info("[touch-log] %s %-7s %-8s %s %s", touch.at.isoformat() if touch.at else "-", touch.kind,
                    touch.direction or "-", touch.outcome or "-", touch.ref or "-")
    logger.info("[touch-log] %d touch(es)", len(found))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
