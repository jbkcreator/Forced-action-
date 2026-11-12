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
* ``call_source`` = ``ghl``, ``caller_seat`` / ``campaign_tag`` = ``ghl``; ``caller_name`` from the message ``userId``
  (GHL_USER_NAMES), else "GHL (unknown)" until the outcome webhook's user name fills it in on match
* ``disposition`` is NULL on import: GHL's call API has no disposition. GHL's own call status is kept in
  ``disposition_raw`` as ``ghl:<callStatus>``. The outcome the caller picked arrives separately (GHL workflow ->
  POST /webhooks/lending/ghl-disposition) and is attached by src.lending.ghl_disposition_match, which every run
  calls after storing the calls; matched calls are then delivered to the Sheet and Slack
* ``raw_event`` holds ids, the outcome and the time only: no message body, names or numbers
* ``recording_status`` / ``recording_view_url`` / ``recording_ref`` are set by :func:`record_recordings` after the
  calls are stored: GHL is asked whether the call message has a recording (the audio needs our API key, so it is
  never linked). ``recorded`` stores the GHL conversation page (where the recording plays, opened by a logged-in
  GHL user) as the view URL and the contact page as the ref; ``none`` stores no links. Earlier GHL rows are filled
  in the same way, newest first, a batch per run

Sheet/Slack delivery (and its retry) and the missing-disposition alert cover GHL calls (G3, 2026-11-03: outbound,
connected, no outcome after GHL_MISSING_DISPOSITION_GRACE_MINUTES); only soft approval cards skip
``call_source = 'ghl'`` (they need a dialer transcript).
Attempt caps, the scoreboard, Cora and the replacement rule (src/lending/replacement_eligibility.py) count GHL
calls like any other.

Callers who are not leads (Q66): the import also lists the GHL conversations whose last message is a call (newer than
INBOUND_LOOKBACK_HOURS, one search page) and stores those callers' INBOUND calls only, so an outbound call to a
non-lead is never counted as a follow-up attempt.

A failed lookup for one contact is logged (class only) and the others carry on. Off until
LENDING_GHL_CALL_IMPORT_ENABLED=true. Cron, every 15 minutes:

    python -m src.lending.ghl_call_import
"""
from __future__ import annotations

import json
import logging
import math
from collections import Counter
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
    INBOUND_LOOKBACK_HOURS,
    IMPORT_RUN_INTERVAL_MINUTES,
    IMPORT_WINDOW_DAYS,
    RECORDING_CHECK_LIMIT,
    RECORDING_SETTLE_MINUTES,
)
from config.lending_dispositions import (
    GHL_CONNECTED_STATES,
    GHL_UNKNOWN_CALLER,
    RECORDING_NONE,
    RECORDING_RECORDED,
    ghl_caller_name,
)
from config.lending_queues import LENDINGFLOW_POOL, WEBSITE_POOL
from src.lending.attempt_log import FORBIDDEN, MISSING, READABLE, RecordingCheck, ghl_recording_checker
from src.lending.first_contact import snapshot_reached_call
from src.lending.ghl_calls import call_duration_seconds, call_state, message_time
from src.lending.ghl_missed_call import adopt_missed_call
from src.lending.lead_arrival_alert import ghl_contact_url
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CallReader = Callable[[str], list[dict[str, Any]]]  # ghl_contact_id -> call messages
CALL_DIRECTIONS = ("outbound", "inbound")
INBOUND_ONLY = ("inbound",)
RecentCallers = Callable[[datetime], list[tuple[str, str]]]  # since -> [(ghl_contact_id, phone)]

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
# gains its final outcome and talk time, or its caller is named); an unknown outcome or a missing duration never erases a known one.
# Only rows this import owns (call_source 'ghl') are touched. ``inserted`` is false for an update; an unchanged
# call returns no row.
# A caller named for the first time (the call's userId resolves now, or was missing before) upgrades the stored
# unknown name even when outcome and talk time are unchanged; a known name is never replaced. The delivery markers
# are cleared with it, so the retry job updates the Sheet row and the Slack card in place.
_CALLER_NAMED = ("(d.caller_name IS NULL OR d.caller_name = :unknown_caller) "
                 "AND EXCLUDED.caller_name IS NOT NULL AND EXCLUDED.caller_name <> :unknown_caller")
_UPSERT = text("""
    INSERT INTO lending.call_dispositions AS d
        (dialer_call_id, call_source, direction, phone, caller_seat, caller_name, campaign_tag, disposition_raw,
         talk_duration_sec, call_started_at, call_ended_at, raw_event)
    VALUES (:call_id, :source, :direction, :phone, :seat, :caller_name, :tag, :raw_outcome,
            :talk, :started, :ended, CAST(:raw_event AS jsonb))
    ON CONFLICT (dialer_call_id) DO UPDATE SET
        caller_name = CASE WHEN d.caller_name IS NULL OR d.caller_name = :unknown_caller THEN EXCLUDED.caller_name
                           ELSE d.caller_name END,
        sheet_synced_disposition = CASE WHEN {named} THEN NULL ELSE d.sheet_synced_disposition END,
        slack_posted_disposition = CASE WHEN {named} THEN NULL ELSE d.slack_posted_disposition END,
        disposition_raw = CASE WHEN EXCLUDED.disposition_raw = :unknown_outcome THEN d.disposition_raw
                               ELSE EXCLUDED.disposition_raw END,
        talk_duration_sec = COALESCE(EXCLUDED.talk_duration_sec, d.talk_duration_sec),
        call_ended_at = CASE WHEN EXCLUDED.talk_duration_sec IS NULL THEN d.call_ended_at
                             ELSE EXCLUDED.call_ended_at END,
        raw_event = EXCLUDED.raw_event
     WHERE d.call_source = :source
       AND ((EXCLUDED.disposition_raw <> :unknown_outcome AND EXCLUDED.disposition_raw IS DISTINCT FROM d.disposition_raw)
            OR (EXCLUDED.talk_duration_sec IS NOT NULL
                AND EXCLUDED.talk_duration_sec IS DISTINCT FROM d.talk_duration_sec)
            OR ({named}))
    RETURNING (xmax = 0) AS inserted
""".replace("{named}", _CALLER_NAMED))
UNKNOWN_OUTCOME = "ghl:unknown"


@dataclass
class ImportStats:
    contacts: int = 0
    imported: int = 0
    updated: int = 0
    already_recorded: int = 0
    lookup_failures: int = 0
    other_callers: int = 0  # callers who are not leads (their inbound calls only)
    eligible: int = 0
    offset: int = 0


def _call_row(message: Mapping[str, Any], contact_id: str, phone: str, since: datetime,
              directions: tuple[str, ...] = CALL_DIRECTIONS) -> Optional[dict[str, Any]]:
    """The insert parameters for one GHL call message, or None when it is not an outbound or inbound call to record."""
    message_id = message.get("id")
    started = message_time(message)
    direction = message.get("direction")
    if not message_id or direction not in directions or started is None or started < since:
        return None
    outcome = call_state(message)
    talk = call_duration_seconds(message)
    return {
        "call_id": f"{GHL_CALL_ID_PREFIX}{message_id}"[:100],
        "source": CALL_SOURCE_GHL,
        "direction": direction,
        "phone": phone,
        "seat": GHL_CALLER_SEAT,
        "caller_name": ghl_caller_name(message.get("userId")),
        "tag": GHL_CAMPAIGN_TAG,
        "raw_outcome": f"ghl:{outcome}"[:100] if outcome else UNKNOWN_OUTCOME,
        "talk": talk,
        "started": started,
        "ended": started + timedelta(seconds=talk) if talk else started,
        "raw_event": json.dumps({"source": CALL_SOURCE_GHL, "message_id": message_id,
                                 "conversation_id": message.get("conversationId"), "contact_id": contact_id,
                                 "call_status": outcome, "date_added": message.get("dateAdded")}),
    }


_IS_LENDINGFLOW = text("SELECT EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p)")


def _snapshot_if_reached(db, row: Mapping[str, Any]) -> None:
    """First-contact snapshot on an outbound GHL call that connected (the snapshot keeps only the first per phone)."""
    outcome = row["raw_outcome"].removeprefix("ghl:")
    if row["direction"] != "outbound" or outcome not in GHL_CONNECTED_STATES or not (row["talk"] or 0) > 0:
        return
    try:
        with db.begin_nested():
            pool = LENDINGFLOW_POOL if db.execute(_IS_LENDINGFLOW, {"p": row["phone"]}).scalar() else WEBSITE_POOL
    except Exception as exc:  # class only: a snapshot problem must never stop the calls being stored
        logger.error("[ghl-call-import] call %s: lead lookup for the snapshot failed (%s)", row["call_id"],
                     type(exc).__name__)
        return
    snapshot_reached_call(db, phone=row["phone"], contacted_at=row["started"], caller_seat=row["caller_name"],
                          queue=pool, source_tag=row["tag"], call_ref=row["call_id"])


def rotation_offset(eligible: int, limit: int, now: datetime,
                    interval_minutes: int = IMPORT_RUN_INTERVAL_MINUTES) -> int:
    """The first contact this run reads: run N (numbered by the cron interval) takes slice N mod the slice count,
    so successive runs walk every slice of the newest-first list instead of re-reading the newest ``limit``."""
    slices = math.ceil(eligible / limit) if limit > 0 else 0
    if slices <= 1:
        return 0
    run_number = int(now.timestamp() // (interval_minutes * 60))
    return (run_number % slices) * limit


def _store_contact(db, read_calls: CallReader, contact_id: str, phone: str, since: datetime, stats: ImportStats,
                   directions: tuple[str, ...]) -> None:
    """Read one contact's GHL calls and store the ones in ``directions`` newer than ``since``, in a savepoint. A
    failed lookup or insert is logged (class only) and counted; it never stops the other contacts."""
    try:
        messages = read_calls(contact_id)
    except Exception as exc:  # class only: GHL errors can echo request detail
        stats.lookup_failures += 1
        logger.warning("[ghl-call-import] contact %s: GHL call lookup failed (%s)", contact_id, type(exc).__name__)
        return
    rows = [row for row in (_call_row(m, contact_id, phone, since, directions) for m in messages) if row]
    if not rows:
        return
    try:
        with db.begin_nested():
            for row in rows:
                if row["direction"] == "outbound":
                    adopt_missed_call(db, row["call_id"], row["phone"], row["started"], row["raw_outcome"])
                stored = db.execute(_UPSERT, {**row, "unknown_outcome": UNKNOWN_OUTCOME, "unknown_caller": GHL_UNKNOWN_CALLER}).first()
                if stored is None:
                    stats.already_recorded += 1
                    continue
                if stored.inserted:
                    stats.imported += 1
                else:
                    stats.updated += 1
                _snapshot_if_reached(db, row)  # new or changed calls only: an unchanged call was seen before
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[ghl-call-import] contact %s: calls not stored (%s)", contact_id, type(exc).__name__)


def import_calls(db, read_calls: CallReader, *, now: Optional[datetime] = None,
                 window_days: int = IMPORT_WINDOW_DAYS, limit: int = IMPORT_CONTACT_LIMIT,
                 recent_callers: Optional[RecentCallers] = None) -> ImportStats:
    """Store the GHL calls (outbound and inbound) of this run's slice of the leads in the window (see
    :func:`rotation_offset`). With ``recent_callers``, also store the INBOUND calls of recent callers who are not
    leads (Q66). Each contact runs in its own savepoint, so a failed lookup or insert never loses the calls already
    stored for other contacts. Does not commit."""
    now = now or datetime.now(timezone.utc)
    stats = ImportStats()
    window = {"window_start": now - timedelta(days=window_days)}
    stats.eligible = db.execute(_ELIGIBLE_COUNT, window).scalar() or 0
    stats.offset = rotation_offset(stats.eligible, limit, now)
    contacts = db.execute(_CONTACTS, {**window, "limit": limit, "offset": stats.offset}).all()
    seen = {contact_id for contact_id, _, _ in contacts}
    for contact_id, since, raw_phone in contacts:
        stats.contacts += 1
        phone = normalize(raw_phone) if raw_phone else None
        if phone:
            _store_contact(db, read_calls, contact_id, phone, since, stats, CALL_DIRECTIONS)
    if recent_callers is None:
        return stats
    since = now - timedelta(hours=INBOUND_LOOKBACK_HOURS)
    try:
        callers = recent_callers(since)
    except Exception as exc:  # class only
        stats.lookup_failures += 1
        logger.warning("[ghl-call-import] recent callers lookup failed (%s)", type(exc).__name__)
        return stats
    for contact_id, raw_phone in callers:
        phone = normalize(raw_phone) if raw_phone else None
        if contact_id in seen or not phone:
            continue  # a lead is already read in full above
        seen.add(contact_id)
        stats.other_callers += 1
        _store_contact(db, read_calls, contact_id, phone, since, stats, INBOUND_ONLY)
    return stats


_UNCHECKED_RECORDINGS = text("""
    SELECT id, raw_event->>'message_id' AS message_id, raw_event->>'conversation_id' AS conversation_id,
           raw_event->>'contact_id' AS contact_id, coalesce(call_ended_at, call_started_at) < :settled AS settled
      FROM lending.call_dispositions
     WHERE call_source = :source AND recording_status IS NULL AND raw_event->>'message_id' IS NOT NULL
       AND (talk_duration_sec IS NOT NULL OR call_started_at < :settled)
     ORDER BY call_started_at DESC
     LIMIT :limit
""")
_SET_RECORDING = text("""
    UPDATE lending.call_dispositions SET recording_status = :status, recording_ref = :ref, recording_view_url = :url
     WHERE id = :id AND recording_status IS NULL
""")


def ghl_conversation_url(app_url: str, location_id: Optional[str], conversation_id: Optional[str]) -> Optional[str]:
    """The conversation's page in the GHL web app (where its call recordings play), or None when an id is missing."""
    if not (app_url and location_id and conversation_id):
        return None
    return f"{app_url.rstrip('/')}/v2/location/{location_id}/conversations/conversations/{conversation_id}"


def record_recordings(db, check: RecordingCheck, *, app_url: str, location_id: str,
                      now: Optional[datetime] = None, limit: int = RECORDING_CHECK_LIMIT) -> Counter:
    """Set the recording status and links of up to ``limit`` GHL calls not yet checked, newest first (this also
    fills in the calls stored before recordings were tracked). A call GHL holds a recording for is ``recorded``
    with its conversation page (else contact page) as the view URL and the contact page as the ref; a call with no
    recording is ``none`` once it is RECORDING_SETTLE_MINUTES old; anything else is checked again next run. A
    refused key or a failed request stops the batch. Returns counts by result. Does not commit."""
    now = now or datetime.now(timezone.utc)
    counts: Counter = Counter()
    settled = now - timedelta(minutes=RECORDING_SETTLE_MINUTES)
    updates: list[dict[str, Any]] = []
    try:
        with db.begin_nested():  # a failed read must not abort the run's transaction (the imported calls)
            rows = db.execute(_UNCHECKED_RECORDINGS, {"source": CALL_SOURCE_GHL, "settled": settled,
                                                      "limit": limit}).all()
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[ghl-call-import] recording check: unchecked calls not read (%s)", type(exc).__name__)
        return counts
    for row in rows:
        try:
            result = check(row.message_id)
        except Exception as exc:  # class only: GHL errors can echo request detail
            counts["failed"] += 1
            logger.warning("[ghl-call-import] call row %s: recording check failed (%s); batch stopped", row.id,
                           type(exc).__name__)
            break
        if result == FORBIDDEN:
            counts["forbidden"] += 1
            logger.error("[ghl-call-import] GHL refused the recording check (API key access); batch stopped")
            break
        if result == READABLE:
            contact_url = ghl_contact_url(app_url, location_id, row.contact_id)
            updates.append({"id": row.id, "status": RECORDING_RECORDED, "ref": contact_url,
                            "url": ghl_conversation_url(app_url, location_id, row.conversation_id) or contact_url})
        elif result == MISSING and row.settled:
            updates.append({"id": row.id, "status": RECORDING_NONE, "ref": None, "url": None})
        else:
            counts["pending"] += 1
    if not updates:
        return counts
    try:
        with db.begin_nested():
            db.execute(_SET_RECORDING, updates)
    except Exception as exc:  # class only
        logger.error("[ghl-call-import] recording status not stored for %d calls (%s)", len(updates),
                     type(exc).__name__)
        return counts
    counts.update(u["status"] for u in updates)
    return counts


def main() -> int:
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_calls import call_messages_reader, recent_callers_reader

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not get_settings().lending_ghl_call_import_enabled:
        logger.info("[ghl-call-import] off (LENDING_GHL_CALL_IMPORT_ENABLED is not true)")
        return 0
    account = lending_ghl_account()
    if account is None:
        logger.error("[ghl-call-import] not run: the Next Deal Lending GHL account is not configured")
        return 1
    from src.lending import ghl_disposition_match

    with lending_session() as db:
        stats = import_calls(db, call_messages_reader(account), recent_callers=recent_callers_reader(account))
        # before matching, so a call delivered in this run carries its recording link
        recordings = record_recordings(db, ghl_recording_checker(account),
                                       app_url=get_settings().lending_ghl_app_url, location_id=account.location_id)
        matched = ghl_disposition_match.attach_pending(db, datetime.now(timezone.utc))
    ghl_disposition_match.deliver_matched(matched)  # after the session's commit: delivery reads the stored outcome
    logger.info("[ghl-call-import] eligible=%d offset=%d contacts=%d other_callers=%d imported=%d updated=%d "
                "already_recorded=%d lookup_failures=%d recordings=%s", stats.eligible, stats.offset, stats.contacts,
                stats.other_callers, stats.imported, stats.updated, stats.already_recorded, stats.lookup_failures,
                dict(recordings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
