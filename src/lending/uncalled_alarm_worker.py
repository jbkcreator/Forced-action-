"""T-10: alarm Josh when a LendingFlow lead has not been reached by phone 2 and 5 minutes after it arrived.

    python -m src.lending.uncalled_alarm_worker            # loop
    python -m src.lending.uncalled_alarm_worker --once     # one cycle and exit

Each cycle copies every new, non-suppressed row of T-11's ``lending.lendingflow_leads`` into
``lending.uncalled_alarms`` (the stopwatch starts at the lead's ``received_at``; no age limit), then decides the
due ones. Outbound calls to the lead's phone since arrival are classified from BatchDialer CDRs
(``lending.call_dispositions``) and the lead's GHL call messages: connected -> closed, nothing sent; attempted ->
"NOT REACHED" alarms; none -> "no call" alarms. At 120 s an SMS to Josh; at 300 s an URGENT SMS plus a Slack post
to #lendingops. SMS go through GoHighLevel (``ghl_sms.GhlSmsSender``, single attempt).

Calls are only visible once they END, so a long call that starts before 2:00 still alarms; the wording says so.
A GHL lookup that fails counts as GHL finding nothing: a false alarm is better than a silent miss.
Each alarm is claimed (committed) before it is sent and is never re-sent. Logs carry lead ids only.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from sqlalchemy import text

from config.lending_alarms import (
    BATCH_SIZE,
    ESCALATION_SECONDS,
    FIRST_ALARM_SECONDS,
    GHL_CONNECTED_STATUSES,
    GHL_TIMEOUT_SECONDS,
    KIND_NO_CALL,
    KIND_NOT_REACHED,
    NOT_CONNECTED_CODES,
    POLL_SECONDS,
    SLACK_TEXT,
    SMS_TEXT,
)
from config.settings import get_settings
from src.lending.db import lending_session
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CONNECTED, ATTEMPTED, NONE = "connected", "attempted", "none"
GhlCallStatus = Callable[[str, datetime], str]  # (ghl_contact_id, since) -> CONNECTED / ATTEMPTED / NONE

_PICKUP = text("""
    INSERT INTO lending.uncalled_alarms (lendingflow_lead_id, lead_uuid, phone, arrived_at)
    SELECT l.id, l.lead_uuid::text, l.phone, l.received_at FROM lending.lendingflow_leads l
     WHERE NOT l.suppressed
       AND NOT EXISTS (SELECT 1 FROM lending.uncalled_alarms a WHERE a.lendingflow_lead_id = l.id)
    ON CONFLICT (lendingflow_lead_id) DO NOTHING
""")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def pick_up_new_leads(db) -> int:
    """Start a stopwatch for every non-suppressed LendingFlow lead not yet watched (no age limit)."""
    count = db.execute(_PICKUP).rowcount
    db.commit()
    return count
