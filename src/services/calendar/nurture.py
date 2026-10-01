"""
WP-GL-5: Lending nurture routing for gate-fail contacts.

OPEN (Q6 from task-analysis): the real nurture destination (GHL pipeline
stage, Slack channel, Instantly sequence, etc.) is unknown. Until that is
confirmed, this module:
  1. Writes the failed gate to fa_max_nurture_queue (durable, auditable).
  2. Posts a one-line summary to EXCEPTIONS so nothing is silently lost.

When Q6 is answered, replace _route_to_destination() below with the
real routing call and flip the status to 'routed'.

IMPORTANT: this module must NOT use src.services.non_buyer_nurture — that
service sends a "please buy our SaaS" email and is unrelated to lending.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text as sa_text

logger = logging.getLogger(__name__)


def enqueue_nurture(
    session,
    *,
    gate_id: str,
    tracked_link_id: Optional[int] = None,
    person_id: Optional[int] = None,
    failed_field: Optional[str] = None,
    fail_reason: Optional[str] = None,
    list_key: Optional[str] = None,
) -> int:
    """Write a nurture queue row and fire an EXCEPTIONS alert.

    Returns the new queue row id. Never raises — a nurture failure must not
    roll back the gate store or caller-bonus record.
    """
    try:
        row = session.execute(
            sa_text(
                """
                INSERT INTO fa_max_nurture_queue
                    (gate_id, tracked_link_id, person_id,
                     failed_field, fail_reason, list_key, status)
                VALUES
                    (:gate_id, :tracked_link_id, :person_id,
                     :failed_field, :fail_reason, :list_key, 'pending_routing')
                RETURNING id
                """
            ),
            {
                "gate_id": gate_id,
                "tracked_link_id": tracked_link_id,
                "person_id": person_id,
                "failed_field": failed_field,
                "fail_reason": fail_reason,
                "list_key": list_key,
            },
        ).mappings().first()
        session.commit()
        queue_id = row["id"] if row else None
    except Exception:
        logger.exception(
            "nurture.enqueue: DB write failed gate_id=%s", gate_id
        )
        session.rollback()
        return -1

    _alert_exceptions(
        gate_id=gate_id,
        failed_field=failed_field,
        fail_reason=fail_reason,
        list_key=list_key,
        queue_id=queue_id,
    )
    return queue_id or -1


def _alert_exceptions(
    *,
    gate_id: str,
    failed_field: Optional[str],
    fail_reason: Optional[str],
    list_key: Optional[str],
    queue_id: Optional[int],
) -> None:
    """Post a notice to EXCEPTIONS. Never raises.

    OPEN (Q6): replace this with the real destination routing once the
    nurture destination is confirmed.
    """
    from config.calendar import CALENDAR_VENTURE_KEY

    try:
        from src.services.relay import exceptions_alert_queue

        message = (
            f"*Gate fail → nurture queue* (Q6 destination TBD)\n"
            f"Gate: `{gate_id}` | Queue row: `{queue_id}`\n"
            f"Failed field: `{failed_field}` | Reason: `{fail_reason}`\n"
            f"List key: `{list_key or 'unknown'}`\n"
            f"_Action required: route this contact once Q6 nurture destination is confirmed._"
        )
        exceptions_alert_queue.enqueue_and_attempt(
            venture_key=CALENDAR_VENTURE_KEY,
            rule="gate_fail_nurture",
            message=message,
        )
    except Exception:
        logger.exception(
            "nurture.alert: EXCEPTIONS post failed gate_id=%s queue_id=%s",
            gate_id, queue_id,
        )
