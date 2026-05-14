"""Per-send audit log — writes one SmsSendLog row per sms_compliance.send_sms call."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def log_send(
    *,
    db: Session,
    phone: Optional[str],
    subscriber_id: Optional[int],
    task_type: Optional[str],
    message_type: str,
    outcome: str,
    suppress_reason: Optional[str] = None,
    vendor_message_id: Optional[str] = None,
    vendor: str = "telnyx",
    campaign: Optional[str] = None,
    variant_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    body_preview: Optional[str] = None,
) -> None:
    """
    Write one SmsSendLog row. Best-effort — never raises.

    outcome: sent | suppressed | dry_run | failed
    suppress_reason: opt_out | no_opt_in | quiet_hours | error (nullable)
    """
    try:
        from src.core.models import SmsSendLog
        row = SmsSendLog(
            phone=phone or None,
            subscriber_id=subscriber_id,
            task_type=task_type,
            message_type=message_type,
            outcome=outcome,
            suppress_reason=suppress_reason,
            vendor_message_id=vendor_message_id,
            vendor=vendor,
            campaign=campaign,
            variant_id=variant_id,
            decision_id=decision_id,
            body_preview=body_preview,
            created_at=datetime.now(timezone.utc),
        )
        db.add(row)
        db.flush()
    except Exception as exc:
        logger.warning("sms_send_log write failed: %s", exc)
