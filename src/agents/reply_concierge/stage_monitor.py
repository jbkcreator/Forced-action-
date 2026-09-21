"""src/agents/reply_concierge/stage_monitor.py

WP-T2-6 -- the three periodic sweeps: stall detection, proactive status
touch, document chase. Cron-called by scripts/run_stage_monitor_worker.py
(Task 12), mirroring WP-T2-5's abandonment_agent.py fire_due_touches()
shape: read due rows, act, log, never crash the whole sweep on one row's
failure.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.fa_max_stage_monitoring import (
    DOC_CHASE_ESCALATE_BUSINESS_DAYS,
    DOC_CHASE_FOLLOWUP_BUSINESS_DAYS,
    STALL_THRESHOLD_BUSINESS_DAYS,
    STATUS_TOUCH_INTERVAL_BUSINESS_DAYS,
    TERMINAL_BACKFLIP_STAGES,
)
from src.services.fa_max_business_days import business_days_since

logger = logging.getLogger(__name__)

_VENTURE_KEY = "fa_max_lending"
_AGENT_NAME = "stage_monitor"

_STALLED_CANDIDATES_SQL = text("""
    SELECT opportunity_id::text, person_id::text, backflip_stage, last_stage_change_at
    FROM fa_max_file_state
    -- 'funded'/'declined' mirrors config.fa_max_stage_monitoring.TERMINAL_BACKFLIP_STAGES;
    -- keep this literal in sync if that constant ever changes.
    WHERE backflip_stage NOT IN ('funded', 'declined')
      AND stall_flagged_at IS NULL
    ORDER BY last_stage_change_at
    LIMIT 200
""")

_MARK_STALLED_SQL = text("""
    UPDATE fa_max_file_state
    SET stall_flagged_at = NOW()
    WHERE opportunity_id = :opportunity_id ::uuid
""")


def sweep_stalled_files(session: Session) -> int:
    rows = session.execute(_STALLED_CANDIDATES_SQL).fetchall()
    flagged = 0
    for row in rows:
        data = dict(row._mapping)
        elapsed = business_days_since(data["last_stage_change_at"])
        if elapsed < STALL_THRESHOLD_BUSINESS_DAYS:
            continue
        try:
            _post_stall_exception(session, data, elapsed)
            session.execute(_MARK_STALLED_SQL, {"opportunity_id": data["opportunity_id"]})
            session.commit()
            flagged += 1
        except Exception:
            session.rollback()
            logger.exception(
                "stage_monitor: failed to flag stall for opportunity_id=%s",
                data["opportunity_id"],
            )
    if flagged:
        logger.info("stage_monitor: flagged %d stalled file(s)", flagged)
    return flagged


def _post_stall_exception(session: Session, data: dict[str, Any], elapsed_days: float) -> None:
    # Raw insert, not enqueue() -- FA Max opportunities carry no Hunter-style
    # OPP-YYYY-##### thread_id, which enqueue()'s CoraRelayHandoff hard-
    # requires. Same pattern as router.py's _route_to_exceptions. 'noop' is
    # a registered dispatcher; this row is informational only.
    import json

    session.execute(
        text("""
            INSERT INTO relay_approval_queue
                (idempotency_key, venture_key, lane, channel, recipient,
                 payload, status, agent_name, autonomy_tier_at_send, person_id)
            VALUES
                (:idem, :vk, :lane, :channel, :recipient,
                 CAST(:payload AS JSONB), :status, :agent, :autonomy, :pid)
            ON CONFLICT (idempotency_key) DO NOTHING
        """),
        {
            "idem": f"fa_max_stall:{data['opportunity_id']}:{int(elapsed_days)}",
            "vk": _VENTURE_KEY,
            "lane": "EXCEPTIONS",
            "channel": "noop",
            "recipient": "n/a",
            "payload": json.dumps({
                "type": "file_stalled",
                "opportunity_id": data["opportunity_id"],
                "backflip_stage": data["backflip_stage"],
                "business_days_stalled": round(elapsed_days, 1),
            }),
            "status": "pending",
            "agent": _AGENT_NAME,
            "autonomy": "A",
            "pid": data["person_id"],
        },
    )


_STATUS_TOUCH_CANDIDATES_SQL = text("""
    SELECT fs.opportunity_id::text, fs.person_id::text, fs.backflip_stage,
           fs.last_borrower_touch_at, fs.contact_email
    FROM fa_max_file_state fs
    -- 'funded'/'declined' mirrors config.fa_max_stage_monitoring.TERMINAL_BACKFLIP_STAGES;
    -- keep this literal in sync if that constant ever changes.
    WHERE fs.backflip_stage NOT IN ('funded', 'declined')
    LIMIT 200
""")

_STATUS_TOUCH_BODY = (
    "Quick update on your file — it's currently {stage_label}. "
    "We'll reach out as soon as there's something new. In the meantime, "
    "reply here anytime with questions."
)

_STAGE_LABELS = {
    "submitted": "with Backflip for initial review",
    "under_review": "under review",
    "conditional_approval": "conditionally approved",
    "docs_requested": "waiting on a couple of documents",
    "cleared_to_close": "cleared to close",
}


def sweep_status_touches(session: Session) -> int:
    from src.services import fa_max_file_state

    rows = session.execute(_STATUS_TOUCH_CANDIDATES_SQL).fetchall()
    sent = 0
    for row in rows:
        data = dict(row._mapping)
        if data["last_borrower_touch_at"] is not None:
            elapsed = business_days_since(data["last_borrower_touch_at"])
            if elapsed < STATUS_TOUCH_INTERVAL_BUSINESS_DAYS:
                continue
        if not data.get("contact_email"):
            logger.warning(
                "stage_monitor: opportunity_id=%s has no resolvable contact email "
                "(fa_max_file_state.contact_email is NULL) — skipping status touch",
                data["opportunity_id"],
            )
            continue
        try:
            body = _STATUS_TOUCH_BODY.format(
                stage_label=_STAGE_LABELS.get(data["backflip_stage"], "moving forward")
            )
            was_sent = fa_max_file_state.send_governed_email(
                session, opportunity_id=data["opportunity_id"], person_id=data["person_id"],
                contact_email=data["contact_email"], subject="Update on your application",
                body=body, lane="RELATIONSHIPS", agent_name=_AGENT_NAME,
                idempotency_key=f"fa_max_status_touch:{data['opportunity_id']}:{_today_key()}",
            )
            if was_sent:
                fa_max_file_state.touch_borrower(session, opportunity_id=data["opportunity_id"])
                sent += 1
        except Exception:
            logger.exception(
                "stage_monitor: failed to send status touch for opportunity_id=%s",
                data["opportunity_id"],
            )
    if sent:
        logger.info("stage_monitor: sent %d status touch(es)", sent)
    return sent


def _today_key() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).date().isoformat()
