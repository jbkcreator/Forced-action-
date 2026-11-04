"""GHL call results on the BatchDialer contact card, so the ISA sees Josh's GHL calls before dialing (the other half of
``bd_result_to_ghl``).

BatchDialer contacts have no tags, so the marker is the card custom field ``card_ghl_call`` (BD_CARD_FIELD_GHL_CALL),
e.g. "2026-11-04 10:15 ET, no answer, 2 GHL calls". It is written on the phone's active BatchDialer contact (an active
``lending.dialer_load_records`` row) when a GHL call for that phone is stored in ``lending.call_dispositions``
(``call_source = 'ghl'``: the 15-minute GHL call import, and the GHL outcome webhook that adds its result code). A call is
written once, and again when its result code changes; only the phone's latest GHL call is written, so a retried
older call never overwrites a newer one. Only that one custom field changes: ``set_custom_fields`` reads
the contact and sends every other field back as it was (BatchDialer's update is a full replace).

Runs from the LendingFlow sweep (every 5 minutes); a failed write is retried up to GHL_NOTE_MAX_ATTEMPTS times within
GHL_NOTE_LOOKBACK_HOURS. Skipped while the lending emergency halt is on. Off until LENDING_GHL_RESULT_TO_BD_ENABLED=true.
"""
from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_followup import CALL_SOURCE_GHL
from config.lending_lendingflow import BD_CARD_FIELD_GHL_CALL, GHL_NOTE_LOOKBACK_HOURS, GHL_NOTE_MAX_ATTEMPTS
from config.settings import get_settings
from src.lending.global_halt import is_globally_halted

logger = logging.getLogger(__name__)

NOTE_TIMEZONE = ZoneInfo("America/New_York")
SWEEP_LIMIT = 50
GHL_OUTCOME_PREFIX = "ghl:"

SessionFactory = Callable[[], AbstractContextManager[Session]]


class CardFieldWriter(Protocol):
    def set_custom_fields(self, contact_id: Any, values: Mapping[str, str]) -> Any: ...


def ghl_call_text(ended: datetime, disposition: Optional[str], raw_outcome: Optional[str], calls: int) -> str:
    """The card line: when, the result code (else GHL's own call status), and how many GHL calls to this phone."""
    if disposition:
        result = disposition.replace("_", " ").lower()
    elif raw_outcome and raw_outcome.startswith(GHL_OUTCOME_PREFIX):
        result = raw_outcome[len(GHL_OUTCOME_PREFIX):].replace("-", " ")
    else:
        result = "no result yet"
    return (f"{ended.astimezone(NOTE_TIMEZONE):%Y-%m-%d %H:%M} ET, {result}, "
            f"{calls} GHL call{'s' if calls != 1 else ''}")


# Due GHL calls on a phone that is live in BatchDialer, locked so two runs never write the same call. Phones with no
# active BatchDialer contact are left out before the LIMIT, so they can never fill a batch.
_DUE = text("""
    WITH due AS (
        SELECT d.id, d.phone, d.disposition, d.disposition_raw, d.call_ended_at
          FROM lending.call_dispositions d
         WHERE d.call_source = :source AND d.call_ended_at IS NOT NULL AND d.phone IS NOT NULL
           AND d.call_ended_at > :since AND d.bd_note_attempts < :max_attempts
           AND (d.bd_note_at IS NULL OR d.disposition IS DISTINCT FROM d.bd_note_disposition)
           AND EXISTS (SELECT 1 FROM lending.dialer_load_records r
                        WHERE r.active AND r.phone = d.phone AND r.dialer_contact_id IS NOT NULL)
         ORDER BY d.call_ended_at LIMIT :limit
           FOR UPDATE OF d SKIP LOCKED)
    SELECT due.id, due.disposition, due.disposition_raw, due.call_ended_at, r.dialer_contact_id,
           due.id = (SELECT x.id FROM lending.call_dispositions x
                      WHERE x.phone = due.phone AND x.call_source = :source AND x.call_ended_at IS NOT NULL
                      ORDER BY x.call_ended_at DESC, x.id DESC LIMIT 1) AS is_latest,
           (SELECT count(*) FROM lending.call_dispositions x
             WHERE x.phone = due.phone AND x.call_source = :source) AS calls
      FROM due
      JOIN LATERAL (SELECT dialer_contact_id FROM lending.dialer_load_records
                     WHERE active AND phone = due.phone AND dialer_contact_id IS NOT NULL
                     ORDER BY id DESC LIMIT 1) r ON true
""")


def write_due_card_notes(db: Session, writer: CardFieldWriter, *, now: Optional[datetime] = None) -> int:
    """Write the due GHL call results onto their BatchDialer contacts. Commits once. Returns how many were written.
    Never raises per call; a failure counts an attempt."""
    now = now or datetime.now(timezone.utc)
    rows = db.execute(_DUE, {"source": CALL_SOURCE_GHL, "since": now - timedelta(hours=GHL_NOTE_LOOKBACK_HOURS),
                             "max_attempts": GHL_NOTE_MAX_ATTEMPTS, "limit": SWEEP_LIMIT}).mappings().all()
    written = 0
    for row in rows:
        line = ghl_call_text(row["call_ended_at"], row["disposition"], row["disposition_raw"], int(row["calls"]))
        try:
            # Only the phone's latest GHL call writes the card: a retried older call is marked done without a write.
            if row["is_latest"]:
                writer.set_custom_fields(row["dialer_contact_id"], {BD_CARD_FIELD_GHL_CALL: line})
        except Exception as exc:  # class only: dialer errors can carry request detail
            logger.warning("[ghl-result-to-bd] call row %s not written to BatchDialer: %s", row["id"],
                           type(exc).__name__)
            db.execute(text("UPDATE lending.call_dispositions SET bd_note_attempts = bd_note_attempts + 1 "
                            "WHERE id = :id"), {"id": row["id"]})
            continue
        db.execute(text("UPDATE lending.call_dispositions SET bd_note_at = :now, bd_note_disposition = :code, "
                        "bd_note_attempts = 0 WHERE id = :id"),
                   {"now": now, "code": row["disposition"], "id": row["id"]})
        written += 1
    db.commit()
    return written


def _live_writer() -> Optional[CardFieldWriter]:
    from src.lending.dialer_port import get_dialer

    return get_dialer()


def _default_sessions() -> AbstractContextManager[Session]:
    from src.lending.db import lending_session

    return lending_session()


def write_card_notes(*, sessions: SessionFactory = _default_sessions,
                     writer_factory: Callable[[], Optional[CardFieldWriter]] = _live_writer) -> int:
    """Sweep entry point: own session, committed. Never raises."""
    if not get_settings().lending_ghl_result_to_bd_enabled or is_globally_halted():
        return 0
    writer = writer_factory()
    if writer is None:
        logger.warning("[ghl-result-to-bd] on, but BatchDialer is not configured: nothing written")
        return 0
    try:
        with sessions() as db:
            return write_due_card_notes(db, writer)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[ghl-result-to-bd] write failed: %s", type(exc).__name__)
        return 0
