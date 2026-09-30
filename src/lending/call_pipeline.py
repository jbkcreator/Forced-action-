"""Shared call-event pipeline: save the row, run the compliance hooks, schedule follow-ups.

Used by the webhook route and the CDR poller so both behave identically.
"""
from __future__ import annotations

import dataclasses
import logging
from typing import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_dispositions import DNC_CODE
from config.settings import get_settings
from src.lending.compliance import on_attempt_recorded, propagate_opt_out
from src.lending.disposition_delivery import (
    alert_dnc_removal_pending,
    alert_unknown_code,
    alert_unpropagated_dnc,
    deliver_disposition,
)
from src.lending.dispositions import DialerCallEvent, RecordedCall, record_dialer_event

logger = logging.getLogger(__name__)


def lending_campaign_ids() -> frozenset[str]:
    raw = get_settings().lending_dialer_campaign_ids
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def _propagate_dnc(db: Session, *, row_id, phone: str, call_id: str, seat) -> bool:
    """Opt the number out and stamp the row. Returns True when the dialer removal is still pending."""
    event_id = propagate_opt_out(db, phone=phone, source_ref=call_id, actor=seat)
    db.execute(
        text("UPDATE lending.call_dispositions SET opt_out_propagated_at = now() WHERE id = :id"),
        {"id": row_id},
    )
    return bool(event_id and db.execute(
        text("SELECT status FROM lending.opt_out_events WHERE id = :id"), {"id": event_id}
    ).scalar() == "dialer_pending")


def process_event(db: Session, ev: DialerCallEvent) -> RecordedCall:
    recorded = record_dialer_event(db, ev)
    db.commit()

    if recorded.call_ended and recorded.phone and ev.direction != "inbound":
        on_attempt_recorded(db, recorded.phone)
    if recorded.dnc_requested and not recorded.opt_out_propagated:
        if recorded.phone:
            if _propagate_dnc(db, row_id=recorded.row_id, phone=recorded.phone,
                              call_id=recorded.call_id, seat=recorded.caller_seat):
                recorded = dataclasses.replace(recorded, dnc_removal_pending=True)
        else:
            logger.error("[lending] call %s: DNC_REQUEST without a usable phone number", recorded.call_id)
    db.commit()
    return recorded


def retry_unpropagated_dnc(db: Session) -> int:
    """DB-only sweep: re-run the opt-out for DNC rows whose propagation failed, whatever the CDR window."""
    rows = db.execute(
        text("SELECT id, phone, dialer_call_id, caller_seat FROM lending.call_dispositions "
             "WHERE disposition = :dnc AND opt_out_propagated_at IS NULL AND phone IS NOT NULL"),
        {"dnc": DNC_CODE},
    ).all()
    done = 0
    for row_id, phone, call_id, seat in rows:
        try:
            _propagate_dnc(db, row_id=row_id, phone=phone, call_id=call_id, seat=seat)
            db.commit()
            done += 1
        except Exception as exc:  # class only: SQL errors embed bound params (phones)
            db.rollback()
            logger.error("[lending] DNC retry for call %s failed: %s", call_id, type(exc).__name__)
    return done


def follow_up(recorded: RecordedCall, add_task: Callable[..., None]) -> None:
    add_task(deliver_disposition, recorded.row_id)
    if recorded.unknown_code:
        add_task(alert_unknown_code, recorded.call_id, recorded.unknown_code, recorded.caller_seat)
    if recorded.dnc_requested and not recorded.phone:
        add_task(alert_unpropagated_dnc, recorded.call_id, recorded.caller_seat)
    if recorded.dnc_removal_pending:
        add_task(alert_dnc_removal_pending, recorded.call_id, recorded.caller_seat)
