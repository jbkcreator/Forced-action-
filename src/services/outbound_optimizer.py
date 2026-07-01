"""
Outbound optimization service — adaptive pacing and channel routing (fa5.3).

Three public functions:
  calculate_pacing_delay(drop_rate)              — exponential backoff formula
  check_outbound_delivery_backpressure(db)        — live engagement drop-off rate
  process_new_outbound_targets(event_payload, db) — route contact to channel
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.direct_mail import flag_direct_mail_eligible

logger = logging.getLogger(__name__)

_BASE_DELAY_SECONDS: float = 5.0
_BACKPRESSURE_WINDOW_HOURS: int = 1
_MIN_SAMPLE_SIZE: int = 10


def calculate_pacing_delay(drop_rate: float) -> float:
    """
    Exponential backoff relative to conversion drop-off rate.

    drop_rate in [0.0, 1.0]:
      <= 0.15 → 5s baseline (no throttling)
      0.20    → ~20s  |  0.50 → ~160s  |  1.00 → ~5120s

    Formula: base * (2 ^ (drop_rate * 10)) when drop_rate > 0.15
    """
    if drop_rate > 0.15:
        return _BASE_DELAY_SECONDS * (2 ** (drop_rate * 10))
    return _BASE_DELAY_SECONDS


def check_outbound_delivery_backpressure(db: Session) -> float:
    """
    Return the current SMS conversion drop-off rate as a float in [0.0, 1.0].

    Queries message_outcomes over the last hour. Engagement = any message with
    a reply or link click. Returns 0.0 when sample size < _MIN_SAMPLE_SIZE to
    prevent premature throttling on low-volume runs.
    """
    window_start = datetime.now(timezone.utc) - timedelta(hours=_BACKPRESSURE_WINDOW_HOURS)

    row = db.execute(
        text("""
            SELECT
                COUNT(*) FILTER (WHERE send_status = 'sent') AS total_sent,
                COUNT(*) FILTER (
                    WHERE send_status = 'sent'
                      AND (replied_at IS NOT NULL OR clicked_at IS NOT NULL)
                ) AS engaged
            FROM message_outcomes
            WHERE sent_at >= :window_start
              AND message_type = 'sms'
        """),
        {"window_start": window_start},
    ).mappings().first()

    total_sent = int(row["total_sent"] or 0) if row else 0
    engaged = int(row["engaged"] or 0) if row else 0

    if total_sent < _MIN_SAMPLE_SIZE:
        return 0.0

    return max(0.0, min(1.0, 1.0 - (engaged / total_sent)))


def process_new_outbound_targets(event_payload: dict, db: Session) -> str:
    """
    Route a newly validated contact to the appropriate outbound channel.

    event_payload shape:
        {
            "contact_id":   int,
            "property_id":  int,
            "carrier_info": {"type": "mobile" | "landline" | "voip" | "unknown"},
        }

    Returns:
        "staged"       — mobile number staged for paced SMS dispatch
        "direct_mail"  — non-mobile, flagged for direct mail
        "skipped"      — contact not found or already routed (idempotent)
    """
    contact_id: int = event_payload["contact_id"]
    property_id: int = event_payload["property_id"]
    carrier_type: str = event_payload["carrier_info"]["type"]
    now = datetime.now(timezone.utc)

    row = db.execute(
        text("SELECT id, outbound_queued_at FROM enriched_contacts WHERE id = :cid"),
        {"cid": contact_id},
    ).mappings().first()

    if row is None:
        logger.warning("process_new_outbound_targets: contact_id=%d not found", contact_id)
        return "skipped"

    if row["outbound_queued_at"] is not None:
        return "skipped"

    if carrier_type == "mobile":
        db.execute(
            text("""
                UPDATE enriched_contacts
                SET outbound_queued_at = :now
                WHERE id = :cid AND outbound_queued_at IS NULL
            """),
            {"cid": contact_id, "now": now},
        )
        logger.info(
            "outbound_optimizer: staged mobile contact_id=%d property_id=%d",
            contact_id, property_id,
        )
        return "staged"

    # Landline / VoIP / unknown → direct mail
    flag_direct_mail_eligible(property_id, db)
    db.execute(
        text("""
            UPDATE enriched_contacts
            SET outbound_queued_at = :now
            WHERE id = :cid AND outbound_queued_at IS NULL
        """),
        {"cid": contact_id, "now": now},
    )
    logger.info(
        "outbound_optimizer: direct_mail contact_id=%d property_id=%d carrier=%s",
        contact_id, property_id, carrier_type,
    )
    return "direct_mail"
