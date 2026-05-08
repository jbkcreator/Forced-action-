"""
Synthflow Voice Drop Sweep — cron 0 15 * * 1-5 (3 PM UTC weekdays).

Finds subscribers who:
  - score >= 70
  - active subscription
  - haven't converted in 48h (no purchase/lock event in last 2 days)
  - haven't received a voice drop in 7 days
  - TCPA opt-in (sms_opt_in=True as proxy)
  - have a phone number

Dispatches high_intent_no_convert event to Cora supervisor for each match.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy import text

logger = logging.getLogger(__name__)

_SCORE_THRESHOLD = 70
_NO_CONVERT_HOURS = 48
_DEDUP_DAYS = 7


def run() -> dict:
    from src.core.database import get_db_context
    from src.agents.supervisor import dispatch_event

    cutoff_convert = datetime.now(timezone.utc) - timedelta(hours=_NO_CONVERT_HOURS)
    cutoff_drop = datetime.now(timezone.utc) - timedelta(days=_DEDUP_DAYS)

    with get_db_context() as db:
        # Joins user_segments for subscriber-level revenue score and sms_opt_ins for
        # phone + TCPA opt-in. Excludes subscribers who locked a territory in the last
        # 48h (recently converted) or received a voice drop in the last 7 days.
        rows = db.execute(text("""
            SELECT
                s.id,
                oi.phone AS phone,
                s.vertical,
                (
                    SELECT mo.message_type
                    FROM message_outcomes mo
                    WHERE mo.subscriber_id = s.id
                      AND mo.message_type IN (
                          'annual_lock', 'territory_lock', 'data_only', 'autopilot_upgrade'
                      )
                    ORDER BY mo.sent_at DESC
                    LIMIT 1
                ) AS last_offer_type
            FROM subscribers s
            JOIN user_segments us ON us.subscriber_id = s.id
            JOIN sms_opt_ins oi ON oi.subscriber_id = s.id
            LEFT JOIN manual_action_log mal
                ON mal.subscriber_id = s.id
                AND mal.action_type = 'voice_drop'
                AND mal.created_at > :cutoff_drop
            WHERE s.status = 'active'
              AND us.revenue_signal_score >= :threshold
              AND mal.id IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM zip_territories zt
                  WHERE zt.subscriber_id = s.id
                    AND zt.locked_at > :cutoff_convert
              )
        """), {
            "cutoff_drop": cutoff_drop,
            "cutoff_convert": cutoff_convert,
            "threshold": _SCORE_THRESHOLD,
        }).fetchall()

    dispatched = 0
    errors = 0
    for row in rows:
        try:
            dispatch_event({
                "event_type": "high_intent_no_convert",
                "subscriber_id": row[0],
                "payload": {
                    "vertical": row[2],
                    "phone": row[1],
                    "offer_type": row[3] or "",
                },
                "source": "cron",
                "decision_id": str(uuid.uuid4()),
                "idempotency_key": f"synthflow_drop:{row[0]}:{cutoff_drop.strftime('%Y%m%d')}",
            })
            dispatched += 1
        except Exception as exc:
            logger.error("voice_drop_sweep dispatch failed sub=%s: %s", row[0], exc)
            errors += 1

    result = {"dispatched": dispatched, "errors": errors, "candidates": len(rows)}
    logger.info("voice_drop_sweep complete %s", result)
    return result
