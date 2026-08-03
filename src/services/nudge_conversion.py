"""
Nudge Conversion — D7: last-touch attribution stamp for `message_outcomes`.

`record_nudge_conversion()` finds the most recent qualifying Lifecycle send for a
subscriber (sent, not already converted, within 48h of the given purchase)
and stamps it as the touch that drove the conversion. This is separate from
`attribution_service.record_conversion_attribution()` (revenue-signal scoring
across all conversion types) — this is specifically the last-touch nudge
credit consumed by abandonment Wave 2's self-skip check.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def record_nudge_conversion(
    subscriber_id: int,
    conversion_type: str = "unlock",
    revenue: Optional[float] = None,
    *,
    occurred_at: Optional[datetime] = None,
    db: Session,
) -> Optional[int]:
    """Stamp the last-touch `message_outcomes` row for this subscriber, if one qualifies.

    Returns the stamped row's id, or None if no qualifying nudge exists
    (nothing sent, or the only sends are already converted or older than 48h).
    """
    now = occurred_at or datetime.now(timezone.utc)
    cutoff_48h = now - timedelta(hours=48)

    row = db.execute(sa_text("""
        SELECT id, sent_at FROM message_outcomes
        WHERE subscriber_id = :sub_id
          AND send_status = 'sent'
          AND sent_at IS NOT NULL
          AND sent_at >= :cutoff_48h
          AND sent_at <= :now
          AND (conversion_type IS NULL OR conversion_type = 'none')
        ORDER BY sent_at DESC
        LIMIT 1
    """), {"sub_id": subscriber_id, "cutoff_48h": cutoff_48h, "now": now}).mappings().first()

    if not row:
        return None

    sent_at = row["sent_at"]
    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=timezone.utc)
    elapsed_hours = (now - sent_at).total_seconds() / 3600

    db.execute(sa_text("""
        UPDATE message_outcomes
        SET conversion_type = :conversion_type,
            conversion_within_4h = :within_4h,
            conversion_within_24h = :within_24h,
            conversion_within_48h = :within_48h,
            revenue_attributed = :revenue
        WHERE id = :id
    """), {
        "conversion_type": conversion_type,
        "within_4h": elapsed_hours <= 4,
        "within_24h": elapsed_hours <= 24,
        "within_48h": elapsed_hours <= 48,
        "revenue": revenue,
        "id": row["id"],
    })

    logger.info(
        "nudge_conversion: stamped message_outcome=%s sub=%s type=%s elapsed_h=%.1f",
        row["id"], subscriber_id, conversion_type, elapsed_hours,
    )
    return row["id"]
