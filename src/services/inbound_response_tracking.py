"""Inbound response-time tracking (Block 11 / B11-04).

t0 is stamped at scoring time (webhook_log.created_at, per B11-01 decision).
t1 is backfilled from agent_decisions.completed_at once the shared
new_lead_voice_call graph run finishes, joined on decision_id == call_id.
Report-only optimization — no closed loop. See
.scratch/block-11-inbound-velocity/SPEC.md (ticket 04).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def record_inbound_response(
    db: Session,
    subscriber_id: Optional[int],
    decision_id: Optional[str],
    t0: datetime,
    score: int,
    matched_signals: List[str],
) -> None:
    """Write one inbound_response row at scoring time. outcome starts 'pending'."""
    db.execute(
        text(
            "INSERT INTO inbound_response "
            "(subscriber_id, decision_id, t0, score, matched_signals, outcome) "
            "VALUES (:subscriber_id, :decision_id, :t0, :score, CAST(:matched_signals AS JSONB), 'pending')"
        ),
        {
            "subscriber_id": subscriber_id,
            "decision_id": decision_id,
            "t0": t0,
            "score": score,
            "matched_signals": json.dumps(matched_signals),
        },
    )


# terminal_status/failure_reason (from new_lead_voice_call's finalize summary)
# -> inbound_response.outcome. Order matters: check specific failure reasons
# before falling back to the generic terminal_status mapping.
def _outcome_from_decision(terminal_status: Optional[str], failure_reason: Optional[str]) -> str:
    if failure_reason == "compliance:voice_consent_required":
        return "consent_blocked"
    if failure_reason == "compliance:dnc_check_required":
        return "dnc_blocked"
    if terminal_status == "completed":
        return "called"
    return "failed"


def sync_inbound_response_outcomes(db: Session, limit: int = 200) -> int:
    """
    Backfill t1/outcome for pending rows whose new_lead_voice_call graph run
    has finished. Zero new call-graph code — reads agent_decisions, the audit
    row Block 2's graph already writes unmodified on every path.

    Returns the number of rows reconciled.
    """
    # terminal_status (not completed_at) marks a finished run: log_decision only
    # backfills completed_at on its UPDATE path, and single-log graphs like
    # new_lead_voice_call INSERT a terminal row with completed_at still NULL.
    # COALESCE(completed_at, started_at) gives the best available finish time.
    rows = db.execute(
        text(
            "SELECT ir.id, COALESCE(ad.completed_at, ad.started_at) AS t1, "
            "       ad.terminal_status, ad.summary->>'failure_reason' AS failure_reason "
            "FROM inbound_response ir "
            "JOIN agent_decisions ad ON ad.decision_id = ir.decision_id "
            "WHERE ir.outcome = 'pending' AND ad.terminal_status IS NOT NULL "
            "LIMIT :limit"
        ),
        {"limit": limit},
    ).fetchall()

    for ir_id, completed_at, terminal_status, failure_reason in rows:
        outcome = _outcome_from_decision(terminal_status, failure_reason)
        db.execute(
            text("UPDATE inbound_response SET t1 = :t1, outcome = :outcome WHERE id = :id"),
            {"t1": completed_at, "outcome": outcome, "id": ir_id},
        )

    db.commit()
    logger.info("sync_inbound_response_outcomes: reconciled %d row(s)", len(rows))
    return len(rows)


def get_inbound_velocity_stats(db: Session) -> Dict[str, Any]:
    """
    Aggregate stats for the admin endpoint + daily report: counts by outcome,
    p50/p95 time-to-callback (t1-t0, seconds), and hot-callback rate
    (called / total). Computed in SQL, not Python-side over fetched rows.
    """
    row = db.execute(
        text(
            "SELECT "
            "  COUNT(*) AS total, "
            "  COUNT(*) FILTER (WHERE outcome = 'called') AS called, "
            "  COUNT(*) FILTER (WHERE outcome = 'consent_blocked') AS consent_blocked, "
            "  COUNT(*) FILTER (WHERE outcome = 'dnc_blocked') AS dnc_blocked, "
            "  COUNT(*) FILTER (WHERE outcome = 'failed') AS failed, "
            "  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t1 - t0))) "
            "    FILTER (WHERE t1 IS NOT NULL) AS p50_seconds, "
            "  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t1 - t0))) "
            "    FILTER (WHERE t1 IS NOT NULL) AS p95_seconds "
            "FROM inbound_response"
        )
    ).first()

    if row is None or row[0] == 0:
        return {
            "total": 0, "called": 0, "consent_blocked": 0, "dnc_blocked": 0, "failed": 0,
            "p50_seconds": None, "p95_seconds": None, "hot_callback_rate": 0.0,
        }

    total, called, consent_blocked, dnc_blocked, failed, p50_seconds, p95_seconds = row
    return {
        "total": total,
        "called": called,
        "consent_blocked": consent_blocked,
        "dnc_blocked": dnc_blocked,
        "failed": failed,
        "p50_seconds": p50_seconds,
        "p95_seconds": p95_seconds,
        "hot_callback_rate": round(called / total, 4) if total else 0.0,
    }
