"""
Hold-lifecycle service — single seam for all deal_rooms × zip_territories state transitions.

Four public operations:
  create_deal_room       — guard ZIP available; write DealRoom record.
  apply_hold_payment     — atomic first-payment-wins flip to 'held'; refund the loser.
  expire_holds           — sweep expired held ZIPs back to 'available'; forfeit the $97.
  refund_on_conversion   — refund hold deposit once after subscription charge clears.

One shared refund helper (_refund_hold) handles all three refund call sites.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import DealRoom
from src.services.lifecycle_slack import post_incident_alert

logger = logging.getLogger(__name__)

_HOLD_DURATION_HOURS = 48


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _slack_alert(metric_name: str, severity: str, action_summary: str, **extra: Any) -> None:
    """Fire a Slack / email alert using the lifecycle_slack poster."""
    incident = SimpleNamespace(
        metric_name=metric_name,
        severity=severity,
        observed_value=extra.get("observed_value"),
        threshold_value=extra.get("threshold_value"),
        county_id=extra.get("county_id"),
        feature_name="hold_lifecycle",
        action_taken=extra.get("action_taken", "no_op"),
        duration_hours=extra.get("duration_hours"),
        breach_started=extra.get("breach_started"),
    )
    try:
        post_incident_alert(incident, kind="action_taken", action_summary=action_summary)
    except Exception:
        logger.warning("[hold_lifecycle] Slack alert failed", exc_info=True)


def _issue_refund(stripe_client: Any, pi_id: str) -> None:
    """Issue an idempotent Stripe refund for a PaymentIntent.

    The idempotency_key is derived from the PI, so retrying a refund (e.g. the
    durable pending-refund sweep re-running after a crash) never double-refunds.
    Raises on Stripe failure — callers decide how to record it.
    """
    stripe_client.refunds.create(
        payment_intent=pi_id,
        idempotency_key=f"hold-refund-{pi_id}",
    )


def _refund_hold(db: Session, stripe_client: Any, deal_room: DealRoom) -> None:
    """Issue a Stripe refund for the hold PaymentIntent.

    On success: marks refund_status='refunded'.
    On failure: marks refund_status='refund_failed', fires Slack, does NOT re-raise.
    ZIP release is the caller's responsibility — this helper never touches zip_territories.
    """
    pi_id = deal_room.stripe_payment_intent_id
    if not pi_id:
        logger.warning(
            "[hold_lifecycle] _refund_hold: deal_room %s has no stripe_payment_intent_id — skipping refund",
            deal_room.token,
        )
        deal_room.refund_status = "refund_failed"
        db.flush()
        _slack_alert(
            metric_name="hold_refund",
            severity="warning",
            action_summary=(
                f"Refund skipped for deal_room {deal_room.token} (ZIP {deal_room.zip_code}): "
                "no stripe_payment_intent_id on record."
            ),
            action_taken="refund_skipped_no_pi",
        )
        return

    try:
        _issue_refund(stripe_client, pi_id)
        deal_room.refund_status = "refunded"
        db.flush()
        logger.info(
            "[hold_lifecycle] Refund issued for deal_room %s (PI %s)",
            deal_room.token,
            pi_id,
        )
    except Exception:
        logger.error(
            "[hold_lifecycle] Stripe refund failed for deal_room %s (PI %s)",
            deal_room.token,
            pi_id,
            exc_info=True,
        )
        deal_room.refund_status = "refund_failed"
        db.flush()
        _slack_alert(
            metric_name="hold_refund_failure",
            severity="critical",
            action_summary=(
                f"Stripe refund FAILED for deal_room {deal_room.token} "
                f"(ZIP {deal_room.zip_code}, PI {pi_id}). Manual refund required."
            ),
            action_taken="refund_failed",
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_deal_room(
    db: Session,
    *,
    prospect_name: str,
    prospect_email: str,
    zip_code: str,
    vertical: str,
    county_id: str,
    tier: str,
    job_value: float,
    close_rate: float,
    properties_snapshot: dict,
) -> DealRoom:
    """Create a deal-room record for a prospect.

    A hold is scoped to one (zip_code, vertical, county_id) territory row — other
    verticals/counties for the same ZIP are independent and must be unaffected.
    Raises HTTPException(409) if THAT specific territory row is not 'available'.
    The caller is responsible for building the properties_snapshot before calling.
    """
    territory = db.execute(
        text(
            "SELECT status FROM zip_territories "
            "WHERE zip_code = :zip AND vertical = :v AND county_id = :c LIMIT 1"
        ),
        {"zip": zip_code, "v": vertical, "c": county_id},
    ).fetchone()

    if territory is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"ZIP {zip_code} ({vertical}/{county_id}) is not available for a "
                "hold deposit (current status: not_found)."
            ),
        )

    if territory.status != "available":
        raise HTTPException(
            status_code=409,
            detail=(
                f"ZIP {zip_code} ({vertical}/{county_id}) is not available for a "
                f"hold deposit (current status: {territory.status})."
            ),
        )

    deal_room = DealRoom(
        token=str(uuid4()),
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        zip_code=zip_code,
        vertical=vertical,
        county_id=county_id,
        tier=tier,
        job_value=job_value,
        close_rate=close_rate,
        properties_snapshot=properties_snapshot,
    )
    db.add(deal_room)
    db.flush()
    logger.info(
        "[hold_lifecycle] DealRoom created: token=%s ZIP=%s vertical=%s county=%s tier=%s",
        deal_room.token,
        zip_code,
        vertical,
        county_id,
        tier,
    )
    return deal_room


def apply_hold_payment(
    db: Session, stripe_client: Any, *, token: str, payment_intent_id: str | None = None
) -> bool:
    """Process a confirmed hold-deposit payment.

    Atomically flips the (zip_code, vertical, county_id) territory row from
    'available' → 'held' — never the whole ZIP.
    Winner (rowcount=1): sets held_at + expires_at, fires Slack, returns True.
    Loser (rowcount=0): refunds the deposit (race carve-out), returns False.

    Idempotency / double-charge guard (bug #1): if the room is already held,
    compare the incoming PaymentIntent to the recorded one. Same PI (duplicate
    webhook) → no-op True. A *different* paid PI is an OVERPAYMENT — refund the
    incoming PI without clobbering the original — and still returns True (the
    hold itself stands).
    """
    deal_room: DealRoom | None = db.execute(
        text("SELECT * FROM deal_rooms WHERE token = :token"),
        {"token": token},
    ).fetchone()

    if deal_room is None:
        logger.error("[hold_lifecycle] apply_hold_payment: unknown token %s", token)
        return False

    # Re-fetch as ORM instance to mutate
    deal_room_obj: DealRoom = db.get(DealRoom, deal_room.id)  # type: ignore[arg-type]

    # Idempotency / overpayment guard — room already held
    if deal_room_obj.held_at is not None:
        recorded_pi = deal_room_obj.stripe_payment_intent_id
        if (
            payment_intent_id
            and recorded_pi
            and payment_intent_id != recorded_pi
        ):
            logger.warning(
                "[hold_lifecycle] apply_hold_payment: token %s already held with PI %s — "
                "incoming PI %s is an OVERPAYMENT, refunding it (original untouched)",
                token, recorded_pi, payment_intent_id,
            )
            _slack_alert(
                metric_name="hold_overpayment",
                severity="warning",
                action_summary=(
                    f"Overpayment on deal_room {token} (ZIP {deal_room_obj.zip_code}): "
                    f"second PaymentIntent {payment_intent_id} refunded; original hold "
                    f"PI {recorded_pi} kept."
                ),
                action_taken="overpayment_refund",
            )
            try:
                _issue_refund(stripe_client, payment_intent_id)
            except Exception:
                logger.error(
                    "[hold_lifecycle] apply_hold_payment: overpayment refund FAILED for "
                    "token %s PI %s — manual refund required",
                    token, payment_intent_id, exc_info=True,
                )
                _slack_alert(
                    metric_name="hold_overpayment_refund_failure",
                    severity="critical",
                    action_summary=(
                        f"Overpayment refund FAILED for deal_room {token} "
                        f"(PI {payment_intent_id}). Manual refund required."
                    ),
                    action_taken="overpayment_refund_failed",
                )
            return True
        logger.info(
            "[hold_lifecycle] apply_hold_payment: token %s already held — idempotent no-op",
            token,
        )
        return True

    # Record the PI that is paying for this hold if not already stored.
    if payment_intent_id and not deal_room_obj.stripe_payment_intent_id:
        deal_room_obj.stripe_payment_intent_id = payment_intent_id

    now = _now()
    result = db.execute(
        text(
            "UPDATE zip_territories SET status = 'held' "
            "WHERE zip_code = :zip AND vertical = :v AND county_id = :c "
            "AND status = 'available'"
        ),
        {
            "zip": deal_room_obj.zip_code,
            "v": deal_room_obj.vertical,
            "c": deal_room_obj.county_id,
        },
    )
    rows_affected = result.rowcount

    if rows_affected > 0:
        # We won the race — record the hold
        deal_room_obj.held_at = now
        deal_room_obj.expires_at = now + timedelta(hours=_HOLD_DURATION_HOURS)
        db.flush()
        logger.info(
            "[hold_lifecycle] ZIP %s held by deal_room %s; expires %s",
            deal_room_obj.zip_code,
            token,
            deal_room_obj.expires_at.isoformat(),
        )
        _slack_alert(
            metric_name="hold_paid",
            severity="info",
            action_summary=(
                f"ZIP {deal_room_obj.zip_code} held by prospect {deal_room_obj.prospect_email} "
                f"(deal_room {token}). Expires {deal_room_obj.expires_at.isoformat()}."
            ),
            action_taken="zip_held",
        )
        return True
    else:
        # Someone else beat us — ZIP is no longer available; refund this payment.
        # Guard against retry double-refund: if refund_status is already set, skip.
        if deal_room_obj.refund_status is not None:
            logger.info(
                "[hold_lifecycle] apply_hold_payment: race-loss for token %s already processed (refund_status=%s)",
                token,
                deal_room_obj.refund_status,
            )
            return False
        logger.warning(
            "[hold_lifecycle] apply_hold_payment: ZIP %s race-loss for token %s — issuing refund",
            deal_room_obj.zip_code,
            token,
        )
        _slack_alert(
            metric_name="hold_paid_race_loss",
            severity="warning",
            action_summary=(
                f"Race-loss for ZIP {deal_room_obj.zip_code}: deal_room {token} "
                "did not secure the ZIP — refunding hold deposit."
            ),
            action_taken="race_loss_refund",
        )
        _refund_hold(db, stripe_client, deal_room_obj)
        return False


def expire_holds(db: Session, stripe_client: Any) -> int:
    """Release all expired, unconverted holds back to 'available'.

    Forfeits the $97 deposit — no refund is issued.
    Fires a Slack alert per expired hold.
    Returns the number of holds processed.
    """
    now = _now()
    expired_rows = db.execute(
        text(
            """
            SELECT id, token, zip_code, vertical, county_id, prospect_email, expires_at
            FROM deal_rooms
            WHERE expires_at < :now
              AND held_at IS NOT NULL
              AND converted_at IS NULL
              AND refund_status IS NULL
            """
        ),
        {"now": now},
    ).fetchall()

    if not expired_rows:
        return 0

    # Release only the exact (zip, vertical, county) each hold reserved — never
    # every row for the ZIP (other verticals/counties are independent holds).
    for row in expired_rows:
        db.execute(
            text(
                "UPDATE zip_territories SET status = 'available' "
                "WHERE zip_code = :zip AND vertical = :v AND county_id = :c "
                "AND status = 'held'"
            ),
            {"zip": row.zip_code, "v": row.vertical, "c": row.county_id},
        )

    db.flush()

    # Slack alert per expired row (no refund — $97 forfeited)
    for row in expired_rows:
        logger.info(
            "[hold_lifecycle] Expired hold released: deal_room %s ZIP %s (forfeit $97)",
            row.token,
            row.zip_code,
        )
        _slack_alert(
            metric_name="hold_expired",
            severity="info",
            action_summary=(
                f"Hold expired for ZIP {row.zip_code} (deal_room {row.token}, "
                f"prospect {row.prospect_email}). $97 deposit forfeited. ZIP released."
            ),
            action_taken="hold_expired_zip_released",
        )

    return len(expired_rows)


def refund_on_conversion(db: Session, stripe_client: Any, *, token: str) -> None:
    """Refund the hold deposit after the prospect subscribes.

    Idempotent: no-op if refund_status is already 'refunded'.
    Sets converted_at then delegates to _refund_hold.
    """
    deal_room: DealRoom | None = db.execute(
        text("SELECT * FROM deal_rooms WHERE token = :token"),
        {"token": token},
    ).fetchone()

    if deal_room is None:
        logger.error("[hold_lifecycle] refund_on_conversion: unknown token %s", token)
        return

    deal_room_obj: DealRoom = db.get(DealRoom, deal_room.id)  # type: ignore[arg-type]

    if deal_room_obj.refund_status == "refunded":
        logger.info(
            "[hold_lifecycle] refund_on_conversion: token %s already refunded — no-op",
            token,
        )
        return

    deal_room_obj.converted_at = _now()
    db.flush()
    _refund_hold(db, stripe_client, deal_room_obj)


def mark_conversion_pending(db: Session, *, token: str) -> bool:
    """Durably mark a hold as converted with a refund owed — no Stripe call.

    Called SYNCHRONOUSLY in the committed webhook path when a hold converts, so
    the refund obligation survives a crash/deploy that kills the best-effort
    deferred BackgroundTask (bug #2). The actual refund is issued either by the
    deferred task (fast path) or by pending_refund_sweep (durable retry).

    Sets converted_at (if unset) and refund_status='pending' unless the deposit
    is already 'refunded'. Returns True if a pending refund is now owed.
    """
    deal_room: DealRoom | None = db.execute(
        text("SELECT * FROM deal_rooms WHERE token = :token"),
        {"token": token},
    ).fetchone()

    if deal_room is None:
        logger.error("[hold_lifecycle] mark_conversion_pending: unknown token %s", token)
        return False

    deal_room_obj: DealRoom = db.get(DealRoom, deal_room.id)  # type: ignore[arg-type]

    if deal_room_obj.converted_at is None:
        deal_room_obj.converted_at = _now()

    if deal_room_obj.refund_status == "refunded":
        db.flush()
        return False

    deal_room_obj.refund_status = "pending"
    db.flush()
    logger.info(
        "[hold_lifecycle] mark_conversion_pending: token %s marked pending refund",
        token,
    )
    return True


def sweep_pending_refunds(db: Session, stripe_client: Any) -> int:
    """Durably retry hold-deposit refunds owed but not yet confirmed.

    Picks up every converted deal_room stuck in refund_status IN
    ('pending', 'refund_failed') and re-attempts the refund. Idempotent: the
    Stripe refund carries a PI-derived idempotency_key, so a refund that already
    succeeded (but whose 'refunded' write was lost) is never double-issued.
    Returns the number of rows processed.
    """
    pending_rows = db.execute(
        text(
            """
            SELECT id, token
            FROM deal_rooms
            WHERE refund_status IN ('pending', 'refund_failed')
              AND converted_at IS NOT NULL
            """
        ),
    ).fetchall()

    if not pending_rows:
        return 0

    processed = 0
    for row in pending_rows:
        deal_room_obj: DealRoom = db.get(DealRoom, row.id)  # type: ignore[arg-type]
        _refund_hold(db, stripe_client, deal_room_obj)
        processed += 1

    db.flush()
    logger.info("[hold_lifecycle] sweep_pending_refunds: processed %d row(s)", processed)
    return processed
