"""T-07 Minute-5 pre-qualification letter: queue once per lead, then render and send.

Order matters: the letter row is committed before anything is rendered or sent, so a GHL outage
never loses it and a repeat of the same lead never sends twice (unique lead_source + lead_ref).
Sending is one function used by both the lead's background task and the retry sweep.
Logs carry the letter id only, never names, amounts or credit bands.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_prequal import GIVE_UP_AFTER_HOURS, RETRY_BACKOFF_MINUTES, SWEEP_BATCH_SIZE
from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK, render_pdf
from src.lending.prequal import TEMPLATE, PrequalLead, PrequalSink, build_context, should_generate
from src.lending.prequal_fit import FitEvaluator
from src.lending.web_leads import DeliveryError

logger = logging.getLogger(__name__)


def enqueue(db, *, lead_source: str, lead_ref: str, contact_id: str, lead: PrequalLead) -> Optional[int]:
    """Queue the letter for a lead with all 4 core fields. Returns the new letter id, or None when the
    lead is incomplete or already has a letter."""
    if not should_generate(lead) or not contact_id:
        return None
    row = db.execute(
        text("INSERT INTO lending.prequal_letters "
             "(lead_source, lead_ref, ghl_contact_id, credit_band, loan_amount, property_state, loan_type) "
             "VALUES (:src, :ref, :cid, :band, :amount, :state, :type) "
             "ON CONFLICT (lead_source, lead_ref) DO NOTHING RETURNING id"),
        {"src": lead_source, "ref": lead_ref, "cid": contact_id, "band": lead.credit_band.strip(),
         "amount": lead.loan_amount, "state": lead.property_state.strip(), "type": lead.loan_type.strip()},
    ).first()
    return int(row[0]) if row else None


def _backoff_minutes_sql() -> str:
    """Minutes to wait after N failures, from config (ints only, never user input)."""
    steps = RETRY_BACKOFF_MINUTES
    whens = " ".join(f"WHEN {n} THEN {int(minutes)}" for n, minutes in enumerate(steps[:-1], start=1))
    return f"CASE attempts WHEN 0 THEN 0 {whens} ELSE {int(steps[-1])} END"


_SENDABLE = (
    "SELECT id, ghl_contact_id, credit_band, loan_amount, property_state, loan_type, attempts "
    "FROM lending.prequal_letters WHERE status IN ('pending', 'failed') AND created_at > :oldest {extra} "
    "ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED"
)


def send_pending(db, sink: Optional[PrequalSink], evaluator: Optional[FitEvaluator], *,
                 enabled: bool, pct: int, letter_id: Optional[int] = None,
                 now: Optional[datetime] = None) -> int:
    """Send queued letters; returns how many went out. One letter (``letter_id``, right after the
    lead arrives) or the retry backlog. With the flag off, no sink or no fit evaluator nothing is
    attempted and rows stay pending. Never raises per letter."""
    now = now or datetime.now(timezone.utc)
    if not enabled:
        return 0
    if sink is None or evaluator is None:
        logger.warning("[prequal] not sending: %s not configured", "GHL" if sink is None else "lender fit engine")
        return 0
    params: dict[str, Any] = {"limit": SWEEP_BATCH_SIZE, "oldest": now - timedelta(hours=GIVE_UP_AFTER_HOURS)}
    if letter_id is not None:
        extra = "AND id = :letter_id"
        params["letter_id"] = letter_id
    else:
        extra = (f"AND (last_attempt_at IS NULL OR "
                 f"last_attempt_at < CAST(:now AS timestamptz) - make_interval(mins => {_backoff_minutes_sql()}))")
        params["now"] = now
    rows = db.execute(text(_SENDABLE.format(extra=extra)), params).mappings().all()
    return sum(_send_one(db, sink, evaluator, dict(row), pct, now) for row in rows)


def _send_one(db, sink: PrequalSink, evaluator: FitEvaluator, row: dict[str, Any], pct: int, now: datetime) -> int:
    lead = PrequalLead(credit_band=row["credit_band"], loan_amount=int(row["loan_amount"]),
                       property_state=row["property_state"], loan_type=row["loan_type"])
    try:
        ctx = build_context(lead, evaluator(lead), pct)
        if ctx is None:
            _mark(db, row["id"], "skipped", now, skip_reason="no fitting lender")
            logger.info("[prequal] letter id=%s skipped: no fitting lender", row["id"])
            return 0
        pdf = render_pdf(TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
        sink.deliver(row["id"], row["ghl_contact_id"], pdf)
    except DeliveryError as exc:
        _record_failure(db, row, str(exc)[:200], now, config_error=exc.config_error)
        return 0
    except Exception as exc:
        _record_failure(db, row, f"unexpected {type(exc).__name__}", now)
        return 0
    _mark(db, row["id"], "sent", now, attempts=int(row["attempts"]) + 1)
    logger.info("[prequal] letter id=%s sent", row["id"])
    return 1


def _mark(db, letter_id: int, status: str, now: datetime, *, skip_reason: Optional[str] = None,
          attempts: Optional[int] = None) -> None:
    db.execute(
        text("UPDATE lending.prequal_letters SET status = :s, skip_reason = :r, last_attempt_at = :now, "
             "last_error = NULL, attempts = COALESCE(:a, attempts), "
             "sent_at = CASE WHEN :s = 'sent' THEN CAST(:now AS timestamptz) ELSE sent_at END WHERE id = :id"),
        {"s": status, "r": skip_reason, "now": now, "a": attempts, "id": letter_id},
    )


def _record_failure(db, row: dict[str, Any], reason: str, now: datetime, *, config_error: bool = False) -> None:
    """A rejected key or setup keeps the attempt count where it was: the letter is not at fault."""
    attempts = int(row["attempts"]) + (0 if config_error else 1)
    db.execute(
        text("UPDATE lending.prequal_letters SET status = 'failed', attempts = :a, last_attempt_at = :now, "
             "last_error = :err WHERE id = :id"),
        {"a": attempts, "now": now, "err": reason, "id": row["id"]},
    )
    logger.log(logging.ERROR if config_error else logging.WARNING,
               "[prequal] letter id=%s send failed failures=%d config_error=%s reason=%s",
               row["id"], attempts, config_error, reason)


def queue_and_send_in_background(*, lead_source: str, lead_ref: str, contact_id: str, lead: PrequalLead) -> None:
    """Entry point for a lead source (e.g. the LendingFlow webhook) to schedule as a background task once the
    lead and its GHL contact exist. Queues and commits first, then sends; never raises (the sweep retries)."""
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.prequal_fit import get_fit_evaluator
    from src.lending.prequal_ghl import get_live_sink

    settings = get_settings()
    if not settings.lending_prequal_pdf_enabled:
        return
    try:
        with lending_session() as db:
            letter_id = enqueue(db, lead_source=lead_source, lead_ref=lead_ref, contact_id=contact_id, lead=lead)
        if letter_id is None:
            return
        with lending_session() as db:
            send_pending(db, get_live_sink(), get_fit_evaluator(), enabled=True,
                         pct=settings.lending_prequal_range_pct, letter_id=letter_id)
    except Exception as exc:
        logger.error("[prequal] background send crashed source=%s: %s", lead_source, type(exc).__name__)
