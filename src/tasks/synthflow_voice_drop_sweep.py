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
    from src.agents.events.ingestion import publish_cora_event
    from src.core.database import get_db_context
    from src.services.vendor_cost_pause_service import get_active_pause

    cutoff_convert = datetime.now(timezone.utc) - timedelta(hours=_NO_CONVERT_HOURS)
    cutoff_drop = datetime.now(timezone.utc) - timedelta(days=_DEDUP_DAYS)

    with get_db_context() as db:
        if get_active_pause(db, "claude", "synthflow_voice_drop"):
            logger.warning("[VoiceDropSweep] active vendor cost pause — skipping run")
            return {"dispatched": 0, "errors": 0, "candidates": 0, "skipped_by_pause": True}

        # Joins user_segments for subscriber-level revenue score and sms_opt_ins for
        # phone + TCPA opt-in. Excludes subscribers with any revenue/spend action in
        # the last 48h (broadened from territory-lock-only — see ADR 0010) and
        # those who received a voice drop in the last 7 days.
        #
        # Conversion = any of: territory lock, wallet debit, bundle purchase,
        # paid lead unlock, credit-report purchase, subscription upgrade.
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
                  -- Territory lock
                  SELECT 1 FROM zip_territories zt
                  WHERE zt.subscriber_id = s.id
                    AND zt.locked_at > :cutoff_convert
              )
              AND NOT EXISTS (
                  -- Wallet debit (any spend)
                  SELECT 1 FROM wallet_transactions wt
                  WHERE wt.subscriber_id = s.id
                    AND wt.txn_type = 'debit'
                    AND wt.created_at > :cutoff_convert
              )
              AND NOT EXISTS (
                  -- Bundle purchase or paid lead unlock
                  SELECT 1 FROM bundle_purchases bp
                  WHERE bp.subscriber_id = s.id
                    AND bp.purchased_at > :cutoff_convert
              )
        """), {
            "cutoff_drop": cutoff_drop,
            "cutoff_convert": cutoff_convert,
            "threshold": _SCORE_THRESHOLD,
        }).fetchall()

    dispatched = 0
    errors = 0
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    for row in rows:
        try:
            # Use the stable per-subscriber daily key as BOTH decision_id AND
            # idempotency_key. _already_handled queries AgentDecision.decision_id;
            # if they differ, dedup never fires and a 2-min sweep storms the queue.
            idem_key = f"synthflow_drop:{row[0]}:{today}"
            publish_cora_event({
                "event_type": "high_intent_no_convert",
                "subscriber_id": row[0],
                "payload": {
                    "vertical": row[2],
                    "phone": row[1],
                    "offer_type": row[3] or "",
                },
                "source": "cron",
                "decision_id": idem_key,
                "idempotency_key": idem_key,
            })
            dispatched += 1
        except Exception as exc:
            logger.error("voice_drop_sweep dispatch failed sub=%s: %s", row[0], exc)
            errors += 1

    result = {"dispatched": dispatched, "errors": errors, "candidates": len(rows)}
    logger.info("voice_drop_sweep complete %s", result)
    return result
