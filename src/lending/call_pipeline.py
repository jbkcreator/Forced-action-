"""Shared call-event pipeline: save the row, run the compliance hooks, schedule follow-ups.

Used by the webhook route and the CDR poller so both behave identically.
"""
from __future__ import annotations

import dataclasses
import logging
from collections.abc import Mapping
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_dispositions import DNC_CODE, GATE_FAILED_FIELD, GATE_FAILED_TRUE, TEXT_CONSENT_FIELD, TEXT_CONSENT_YES
from config.settings import get_settings
from src.lending.compliance import on_attempt_recorded, propagate_opt_out
from src.lending.disposition_delivery import (
    alert_dnc_removal_pending,
    alert_unknown_code,
    alert_unpropagated_dnc,
    deliver_disposition,
)
from src.lending.bd_result_to_ghl import write_note_in_background
from src.lending.consent import record_consent
from src.lending.dialer_port import get_dialer
from src.lending.dispositions import DialerCallEvent, RecordedCall, lookup_load_record, record_dialer_event
from src.lending.first_contact import snapshot_reached_call
from src.lending.soft_approval.slack_card import post_soft_approval_card

logger = logging.getLogger(__name__)

CONSENT_CHECK_WINDOW_SECONDS = 2 * 60 * 60


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


def read_contact_fields(ev: DialerCallEvent, dialer) -> Optional[Mapping[str, Any]]:
    """The caller-set contact custom fields of an outbound call (one quick dialer read), or None when it failed."""
    try:
        return dialer.get_contact_customfields(ev.contact_id, quick=True)
    except Exception as exc:  # class only: the message may carry request detail
        logger.warning("[lending] call %s: could not read contact custom fields: %s", ev.call_id, type(exc).__name__)
        return None


def _reads_contact_fields(ev: DialerCallEvent, row_phone: Optional[str], dialer) -> bool:
    return bool(row_phone) and ev.direction == "outbound" and bool(ev.contact_id) and dialer is not None


def capture_consent(db: Session, ev: DialerCallEvent, row_phone: Optional[str], caller_name: Optional[str], dialer,
                    *, fields: Optional[Mapping[str, Any]] = None) -> bool:
    """Answered inbound call = consent; outbound = the caller-set ``text_consent`` contact field (``fields`` when the
    caller already read them, else read here). Returns False only when the dialer read failed, so the caller can
    retry instead of marking it checked."""
    if not row_phone:
        return True
    if ev.direction == "inbound" and (ev.duration or 0) > 0:
        record_consent(db, row_phone, "inbound_call", call_id=ev.call_id)
        return True
    if _reads_contact_fields(ev, row_phone, dialer):
        if fields is None:
            fields = read_contact_fields(ev, dialer)
            if fields is None:
                return False
        if str(fields.get(TEXT_CONSENT_FIELD, "")).strip().lower() == TEXT_CONSENT_YES:
            record_consent(db, row_phone, "on_call_yes", call_id=ev.call_id, captured_by=caller_name)
    return True


def capture_gate_flag(db: Session, row_id: int, fields: Optional[Mapping[str, Any]]) -> None:
    """Sets the call's ``gate_failed`` when the caller ticked the ``gate_failed`` contact field."""
    if fields and str(fields.get(GATE_FAILED_FIELD, "")).strip().lower() in GATE_FAILED_TRUE:
        db.execute(text("UPDATE lending.call_dispositions SET gate_failed = true WHERE id = :id"), {"id": row_id})


def _check_consent(db: Session, ev: DialerCallEvent, row_id: int, dialer) -> None:
    """Once per connected, recently ended call, and again when a disposition lands after the last check
    (callers often set the field during wrap-up, after the hang-up). The contact fields are read once and feed both
    consent and the gate flag. A failed dialer read is not stamped."""
    row = db.execute(
        text("SELECT phone, caller_name, direction, dialer_contact_id, talk_duration_sec "
             "FROM lending.call_dispositions WHERE id = :id AND talk_duration_sec > 0 "
             "AND call_ended_at > now() - make_interval(secs => :window) "
             "AND (consent_checked_at IS NULL OR consent_checked_at < disposition_at)"),
        {"id": row_id, "window": CONSENT_CHECK_WINDOW_SECONDS},
    ).first()
    if row is None:
        return
    phone, caller_name, direction, contact_id, duration = row
    if dialer is None:
        dialer = get_dialer()
    call = dataclasses.replace(ev, direction=direction, contact_id=contact_id, duration=duration)
    fields: Optional[Mapping[str, Any]] = None
    if _reads_contact_fields(call, phone, dialer):
        fields = read_contact_fields(call, dialer)
        if fields is None:
            return
    capture_consent(db, call, phone, caller_name, dialer, fields=fields if fields is not None else {})
    capture_gate_flag(db, row_id, fields)
    db.execute(text("UPDATE lending.call_dispositions SET consent_checked_at = clock_timestamp() WHERE id = :id"), {"id": row_id})


def process_event(db: Session, ev: DialerCallEvent, dialer=None) -> RecordedCall:
    recorded = record_dialer_event(db, ev)
    db.commit()
    try:
        with db.begin_nested():
            _check_consent(db, ev, recorded.row_id, dialer)
        db.commit()
    except Exception as exc:  # consent capture must never block attempt counting or opt-out propagation
        logger.error("[lending] call %s: consent capture failed: %s", ev.call_id, type(exc).__name__)

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
    _snapshot_first_contact(db, ev, recorded)  # after the commit: a snapshot problem can never undo the call's writes
    return recorded


def _snapshot_first_contact(db: Session, ev: DialerCallEvent, recorded: RecordedCall) -> None:
    """The lead's first-contact snapshot, on the first outbound call that reached someone (talk time)."""
    if ev.direction == "inbound" or not recorded.phone or not (ev.duration or 0) > 0:
        return
    try:
        with db.begin_nested():
            record = lookup_load_record(db, ev.contact_id, recorded.phone, ev.started_at) or {}
    except Exception as exc:  # class only
        logger.error("[lending] call %s: load record lookup for the snapshot failed: %s", ev.call_id, type(exc).__name__)
        record = {}
    if snapshot_reached_call(db, phone=recorded.phone, contacted_at=ev.started_at or ev.ended_at,
                             caller_seat=recorded.caller_seat, queue=record.get("pool"),
                             source_tag=record.get("campaign_tag"), call_ref=ev.call_id):
        db.commit()


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
    if recorded.call_ended:
        add_task(post_soft_approval_card, recorded.row_id)
        add_task(write_note_in_background, recorded.row_id)
    if recorded.unknown_code:
        add_task(alert_unknown_code, recorded.call_id, recorded.unknown_code, recorded.caller_seat)
    if recorded.dnc_requested and not recorded.phone:
        add_task(alert_unpropagated_dnc, recorded.call_id, recorded.caller_seat)
    if recorded.dnc_removal_pending:
        add_task(alert_dnc_removal_pending, recorded.call_id, recorded.caller_seat)
