"""Shared call-event pipeline: save the row, run the compliance hooks, schedule follow-ups.

Used by the webhook route and the CDR poller so both behave identically.
"""
from __future__ import annotations

import dataclasses
import logging
from typing import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

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


def process_event(db: Session, ev: DialerCallEvent) -> RecordedCall:
    recorded = record_dialer_event(db, ev)
    db.commit()

    if recorded.call_ended and recorded.phone:
        on_attempt_recorded(db, recorded.phone)
    if recorded.dnc_requested and not recorded.opt_out_propagated:
        if recorded.phone:
            event_id = propagate_opt_out(db, phone=recorded.phone, source_ref=recorded.call_id, actor=recorded.caller_seat)
            db.execute(
                text("UPDATE lending.call_dispositions SET opt_out_propagated_at = now() WHERE id = :id"),
                {"id": recorded.row_id},
            )
            if event_id and db.execute(
                text("SELECT status FROM lending.opt_out_events WHERE id = :id"), {"id": event_id}
            ).scalar() == "dialer_pending":
                recorded = dataclasses.replace(recorded, dnc_removal_pending=True)
        else:
            logger.error("[lending] call %s: DNC_REQUEST without a usable phone number", recorded.call_id)
    db.commit()
    return recorded


def follow_up(recorded: RecordedCall, add_task: Callable[..., None]) -> None:
    add_task(deliver_disposition, recorded.row_id)
    if recorded.unknown_code:
        add_task(alert_unknown_code, recorded.call_id, recorded.unknown_code, recorded.caller_seat)
    if recorded.dnc_requested and not recorded.phone:
        add_task(alert_unpropagated_dnc, recorded.call_id, recorded.caller_seat)
    if recorded.dnc_removal_pending:
        add_task(alert_dnc_removal_pending, recorded.call_id, recorded.caller_seat)
