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
        stripe_client.refunds.create(payment_intent=pi_id)
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
    tier: str,
    job_value: float,
    close_rate: float,
    properties_snapshot: dict,
) -> DealRoom:
    """Create a deal-room record for a prospect.

    Raises HTTPException(409) if the ZIP is not 'available'.
    The caller is responsible for building the properties_snapshot before calling.
    """
    # Block if the ZIP doesn't exist at all, or if ANY territory row is non-available
    # (held/locked/grace means someone already has or is closing on this ZIP).
    any_row = db.execute(
        text("SELECT status FROM zip_territories WHERE zip_code = :zip LIMIT 1"),
        {"zip": zip_code},
    ).fetchone()

    if any_row is None:
        raise HTTPException(
            status_code=409,
            detail=f"ZIP {zip_code} is not available for a hold deposit (current status: not_found).",
        )

    blocked_row = db.execute(
        text(
            "SELECT status FROM zip_territories "
            "WHERE zip_code = :zip AND status != 'available' LIMIT 1"
        ),
        {"zip": zip_code},
    ).fetchone()

    if blocked_row is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"ZIP {zip_code} is not available for a hold deposit "
                f"(current status: {blocked_row.status})."
            ),
        )

    deal_room = DealRoom(
        token=str(uuid4()),
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        zip_code=zip_code,
        tier=tier,
        job_value=job_value,
        close_rate=close_rate,
        properties_snapshot=properties_snapshot,
    )
    db.add(deal_room)
    db.flush()
    logger.info(
        "[hold_lifecycle] DealRoom created: token=%s ZIP=%s tier=%s",
        deal_room.token,
        zip_code,
        tier,
    )
    return deal_room


def apply_hold_payment(db: Session, stripe_client: Any, *, token: str) -> bool:
    """Process a confirmed hold-deposit payment.

    Atomically flips zip_territories.status from 'available' → 'held'.
    Winner (rowcount=1): sets held_at + expires_at, fires Slack, returns True.
    Loser (rowcount=0): refunds the deposit (race carve-out), returns False.
    Idempotent: if held_at is already set (duplicate webhook), returns True immediately.
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

    # Idempotency guard — duplicate webhook
    if deal_room_obj.held_at is not None:
        logger.info(
            "[hold_lifecycle] apply_hold_payment: token %s already held — idempotent no-op",
            token,
        )
        return True

    now = _now()
    result = db.execute(
        text(
            "UPDATE zip_territories SET status = 'held' "
            "WHERE zip_code = :zip AND status = 'available'"
        ),
        {"zip": deal_room_obj.zip_code},
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
            SELECT id, token, zip_code, prospect_email, expires_at
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

    zip_codes = [row.zip_code for row in expired_rows]
    expired_ids = [row.id for row in expired_rows]

    # Batch-release all expired ZIPs in one UPDATE
    db.execute(
        text(
            "UPDATE zip_territories SET status = 'available' "
            "WHERE zip_code = ANY(:zips) AND status = 'held'"
        ),
        {"zips": zip_codes},
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
