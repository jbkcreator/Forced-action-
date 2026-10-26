"""Record calls made in GoHighLevel in lending.call_dispositions (Runbook § 4.1, Item 1).

The Runbook asks that every call attempt in the 14-day follow-up is written to lending.call_dispositions, so that
LendingFlow leads with 5+ attempts over 5+ days can be claimed for replacement credit. BatchDialer calls already
arrive through the CDR poller; LendingFlow leads go straight to Josh, who calls from GoHighLevel, and those calls
were recorded nowhere. This import reads the call messages (Josh's outbound attempts and the lead's inbound calls
to the Next Deal Lending number, answered and missed) of every LendingFlow and website lead still in its follow-up
window and stores each one once. When more contacts are in the window than one run reads
(IMPORT_CONTACT_LIMIT), successive runs read successive slices, so every contact is covered.

* ``dialer_call_id`` = ``ghl:<message id>`` (unique, so a re-run never duplicates a call); a call seen again with
  a new outcome or talk time (imported while it was still in progress) is updated in place
* ``direction`` = ``outbound`` or ``inbound`` (Q66). Only outbound rows count as attempts (attempt caps, the replacement
  rule, the scoreboard and the uncalled alarm all filter on direction); an inbound row is a record of the call
* ``call_source`` = ``ghl``, ``caller_seat`` / ``campaign_tag`` = ``ghl`` (its own scoreboard line)
* ``disposition`` stays NULL (GHL outcomes are not lending disposition codes); GHL's outcome is kept in
  ``disposition_raw`` as ``ghl:<callStatus>``, which also keeps it out of the missing-disposition alert
* ``raw_event`` holds ids, the outcome and the time only: no message body, names or numbers

Dialer-only jobs (Slack/sheet delivery, missing-disposition alert, soft approval cards) skip ``call_source = 'ghl'``.
Attempt caps, the scoreboard, Cora and the replacement rule (src/lending/replacement_eligibility.py) count GHL
calls like any other.

A failed lookup for one contact is logged (class only) and the others carry on. Off until
LENDING_GHL_CALL_IMPORT_ENABLED=true. Cron, every 15 minutes:

    python -m src.lending.ghl_call_import
"""
from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_followup import (
    CALL_SOURCE_GHL,
    GHL_CALL_ID_PREFIX,
    GHL_CALLER_SEAT,
    GHL_CAMPAIGN_TAG,
    IMPORT_CONTACT_LIMIT,
    IMPORT_RUN_INTERVAL_MINUTES,
    IMPORT_WINDOW_DAYS,
)
from src.lending.ghl_calls import call_duration_seconds, call_state, message_time
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CallReader = Callable[[str], list[dict[str, Any]]]  # ghl_contact_id -> call messages
CALL_DIRECTIONS = ("outbound", "inbound")

# One row per GHL contact: a contact can hold both a LendingFlow and a website lead; calls count from the
# earliest arrival, and the lead's own normalised phone is the one stored on the call.
_ELIGIBLE = """
    SELECT ghl_contact_id, min(received_at) AS since, (array_agg(phone ORDER BY received_at))[1] AS phone
      FROM (SELECT ghl_contact_id, received_at, phone FROM lending.lendingflow_leads
             WHERE ghl_contact_id IS NOT NULL AND received_at >= :window_start
            UNION ALL
            SELECT ghl_contact_id, received_at, phone FROM lending.web_leads
             WHERE ghl_contact_id IS NOT NULL AND received_at >= :window_start) leads
     GROUP BY ghl_contact_id
"""
_ELIGIBLE_COUNT = text(f"SELECT count(*) FROM ({_ELIGIBLE}) eligible")
# ghl_contact_id breaks ties, so the slices of one rotation do not overlap.
_CONTACTS = text(_ELIGIBLE + " ORDER BY min(received_at) DESC, ghl_contact_id LIMIT :limit OFFSET :offset")

# A call already stored is updated only when GHL now reports something new (a call imported while in progress
# gains its final outcome and talk time); an unknown outcome or a missing duration never erases a known one.
# Only rows this import owns (call_source 'ghl') are touched. ``inserted`` is false for an update; an unchanged
# call returns no row.
_UPSERT = text("""
    INSERT INTO lending.call_dispositions AS d
        (dialer_call_id, call_source, direction, phone, caller_seat, campaign_tag, disposition_raw,
         talk_duration_sec, call_started_at, call_ended_at, raw_event)
    VALUES (:call_id, :source, :direction, :phone, :seat, :tag, :raw_outcome,
            :talk, :started, :ended, CAST(:raw_event AS jsonb))
    ON CONFLICT (dialer_call_id) DO UPDATE SET
        disposition_raw = CASE WHEN EXCLUDED.disposition_raw = :unknown_outcome THEN d.disposition_raw
                               ELSE EXCLUDED.disposition_raw END,
        talk_duration_sec = COALESCE(EXCLUDED.talk_duration_sec, d.talk_duration_sec),
        call_ended_at = CASE WHEN EXCLUDED.talk_duration_sec IS NULL THEN d.call_ended_at
                             ELSE EXCLUDED.call_ended_at END,
        raw_event = EXCLUDED.raw_event
     WHERE d.call_source = :source
       AND ((EXCLUDED.disposition_raw <> :unknown_outcome AND EXCLUDED.disposition_raw IS DISTINCT FROM d.disposition_raw)
            OR (EXCLUDED.talk_duration_sec IS NOT NULL
                AND EXCLUDED.talk_duration_sec IS DISTINCT FROM d.talk_duration_sec))
    RETURNING (xmax = 0) AS inserted
""")
UNKNOWN_OUTCOME = "ghl:unknown"


@dataclass
class ImportStats:
    contacts: int = 0
    imported: int = 0
    updated: int = 0
    already_recorded: int = 0
    lookup_failures: int = 0
    eligible: int = 0
    offset: int = 0


def _call_row(message: Mapping[str, Any], contact_id: str, phone: str, since: datetime) -> Optional[dict[str, Any]]:
    """The insert parameters for one GHL call message, or None when it is not an outbound or inbound call to record."""
    message_id = message.get("id")
    started = message_time(message)
    direction = message.get("direction")
    if not message_id or direction not in CALL_DIRECTIONS or started is None or started < since:
        return None
    outcome = call_state(message)
    talk = call_duration_seconds(message)
    return {
        "call_id": f"{GHL_CALL_ID_PREFIX}{message_id}"[:100],
        "source": CALL_SOURCE_GHL,
        "direction": direction,
        "phone": phone,
        "seat": GHL_CALLER_SEAT,
        "tag": GHL_CAMPAIGN_TAG,
        "raw_outcome": f"ghl:{outcome}"[:100] if outcome else UNKNOWN_OUTCOME,
        "talk": talk,
        "started": started,
        "ended": started + timedelta(seconds=talk) if talk else started,
        "raw_event": json.dumps({"source": CALL_SOURCE_GHL, "message_id": message_id,
                                 "conversation_id": message.get("conversationId"), "contact_id": contact_id,
                                 "call_status": outcome, "date_added": message.get("dateAdded")}),
    }


def rotation_offset(eligible: int, limit: int, now: datetime,
                    interval_minutes: int = IMPORT_RUN_INTERVAL_MINUTES) -> int:
    """The first contact this run reads: run N (numbered by the cron interval) takes slice N mod the slice count,
    so successive runs walk every slice of the newest-first list instead of re-reading the newest ``limit``."""
    slices = math.ceil(eligible / limit) if limit > 0 else 0
    if slices <= 1:
        return 0
    run_number = int(now.timestamp() // (interval_minutes * 60))
    return (run_number % slices) * limit


def import_calls(db, read_calls: CallReader, *, now: Optional[datetime] = None,
                 window_days: int = IMPORT_WINDOW_DAYS, limit: int = IMPORT_CONTACT_LIMIT) -> ImportStats:
    """Store the outbound GHL calls of this run's slice of the leads in the window (see :func:`rotation_offset`).
    Each contact runs in its own savepoint, so a failed lookup or insert never loses the calls already stored for
    other contacts. Does not commit."""
    now = now or datetime.now(timezone.utc)
    stats = ImportStats()
    window = {"window_start": now - timedelta(days=window_days)}
    stats.eligible = db.execute(_ELIGIBLE_COUNT, window).scalar() or 0
    stats.offset = rotation_offset(stats.eligible, limit, now)
    contacts = db.execute(_CONTACTS, {**window, "limit": limit, "offset": stats.offset}).all()
    for contact_id, since, raw_phone in contacts:
        stats.contacts += 1
        phone = normalize(raw_phone) if raw_phone else None
        if not phone:
            continue
        try:
            messages = read_calls(contact_id)
        except Exception as exc:  # class only: GHL errors can echo request detail
            stats.lookup_failures += 1
            logger.warning("[ghl-call-import] contact %s: GHL call lookup failed (%s)", contact_id, type(exc).__name__)
            continue
        rows = [row for row in (_call_row(m, contact_id, phone, since) for m in messages) if row]
        if not rows:
            continue
        try:
            with db.begin_nested():
                for row in rows:
                    stored = db.execute(_UPSERT, {**row, "unknown_outcome": UNKNOWN_OUTCOME}).first()
                    if stored is None:
                        stats.already_recorded += 1
                    elif stored.inserted:
                        stats.imported += 1
                    else:
                        stats.updated += 1
        except Exception as exc:  # class only: SQL errors embed bound parameters
            logger.error("[ghl-call-import] contact %s: calls not stored (%s)", contact_id, type(exc).__name__)
    return stats


def main() -> int:
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_calls import call_messages_reader

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not get_settings().lending_ghl_call_import_enabled:
        logger.info("[ghl-call-import] off (LENDING_GHL_CALL_IMPORT_ENABLED is not true)")
        return 0
    account = lending_ghl_account()
    if account is None:
        logger.error("[ghl-call-import] not run: the Next Deal Lending GHL account is not configured")
        return 1
    with lending_session() as db:
        stats = import_calls(db, call_messages_reader(account))
    logger.info("[ghl-call-import] eligible=%d offset=%d contacts=%d imported=%d updated=%d already_recorded=%d "
                "lookup_failures=%d", stats.eligible, stats.offset, stats.contacts, stats.imported, stats.updated,
                stats.already_recorded, stats.lookup_failures)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
