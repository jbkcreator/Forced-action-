"""
WP-GL-5: Lending nurture routing for gate-fail contacts.

Resolved (confirmed against the real account 2026-10-05): the nurture
destination is the Nurture stage of the Next Deal Lending "Booked Calls"
GHL pipeline (see src/services/calendar/ghl_pipeline.py). A gate fail:
  1. Writes to fa_max_nurture_queue (durable, auditable) — always, regardless
     of whether the GHL push below succeeds.
  2. Pushes the contact to the GHL Nurture stage.
  3. On push failure (or if GHL isn't configured), pages EXCEPTIONS so the
     contact is never silently lost — the queue row stays 'pending_routing'
     for manual follow-up.

The gate table stores enum codes only, never phone/email — contact details
are resolved via person_id -> fa_max_persons when present. A gate with no
person_id (not yet linked to a canonical person) cannot be pushed to GHL;
it still gets queued and alerted so it's visible, not dropped.

IMPORTANT: this module must NOT use src.services.non_buyer_nurture — that
service sends a "please buy our SaaS" email and is unrelated to lending.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

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
    """Write a nurture queue row, push to the GHL Nurture stage, and alert
    EXCEPTIONS if that push didn't happen.

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
            "nurture.enqueue: DB write failed gate_id=%s — still pushing to GHL", gate_id
        )
        session.rollback()
        queue_id = None

    pushed = _route_to_ghl_nurture(
        session,
        person_id=person_id,
        gate_id=gate_id,
        failed_field=failed_field,
        fail_reason=fail_reason,
    )

    if pushed and queue_id:
        try:
            session.execute(
                sa_text("UPDATE fa_max_nurture_queue SET status = 'routed', routed_at = NOW() WHERE id = :id"),
                {"id": queue_id},
            )
            session.commit()
        except Exception:
            logger.exception("nurture.enqueue: failed marking queue_id=%s routed", queue_id)
            session.rollback()
    else:
        _alert_exceptions(
            gate_id=gate_id,
            failed_field=failed_field,
            fail_reason=fail_reason,
            list_key=list_key,
            queue_id=queue_id,
        )

    return queue_id or -1


def _resolve_contact(session, person_id: Optional[int]) -> Optional[dict]:
    """phone/email/full_name for person_id, or None if unresolvable.

    Follows a merged person to its surviving record, same pattern WP-GL-10
    uses — a person merged after their gate was stored must still resolve.
    """
    if person_id is None:
        return None
    row = session.execute(
        sa_text(
            """
            SELECT person_id, phone, email, full_name, merged_into_id
            FROM fa_max_persons WHERE person_id = :pid
            """
        ),
        {"pid": person_id},
    ).mappings().first()
    if row is None:
        return None
    if row["merged_into_id"] is not None:
        return _resolve_contact(session, row["merged_into_id"])
    if not row["phone"] and not row["email"]:
        return None
    return dict(row)


def _route_to_ghl_nurture(
    session,
    *,
    person_id: Optional[Any],
    gate_id: str,
    failed_field: Optional[str],
    fail_reason: Optional[str],
) -> bool:
    """Push the contact to the GHL Nurture stage. Never raises."""
    try:
        contact = _resolve_contact(session, person_id)
        if contact is None:
            logger.warning(
                "nurture.route: gate_id=%s person_id=%s has no resolvable "
                "phone/email — cannot push to GHL, queued for manual follow-up",
                gate_id, person_id,
            )
            return False

        from src.services.calendar.ghl_pipeline import push_gate_fail_to_nurture_stage

        first_name = None
        if contact.get("full_name"):
            parts = contact["full_name"].strip().split(None, 1)
            first_name = parts[0] if parts else None

        return push_gate_fail_to_nurture_stage(
            phone=contact.get("phone"),
            email=contact.get("email"),
            first_name=first_name,
            opportunity_name=f"Gate fail — {failed_field or fail_reason or 'unqualified'} ({gate_id})",
        )
    except Exception:
        logger.exception("nurture.route: GHL push failed gate_id=%s", gate_id)
        return False


def _alert_exceptions(
    *,
    gate_id: str,
    failed_field: Optional[str],
    fail_reason: Optional[str],
    list_key: Optional[str],
    queue_id: Optional[int],
) -> None:
    """Post a notice to EXCEPTIONS. Never raises.

    Fires when the GHL push didn't happen — no resolvable contact, GHL not
    configured, or the push itself failed — so a gate-fail contact is never
    silently lost even when it can't reach the pipeline automatically.
    """
    from config.calendar import CALENDAR_VENTURE_KEY

    try:
        from src.services.relay import exceptions_alert_queue

        message = (
            f"*Gate fail → nurture queue* (GHL push did not happen — see log)\n"
            f"Gate: `{gate_id}` | Queue row: `{queue_id}`\n"
            f"Failed field: `{failed_field}` | Reason: `{fail_reason}`\n"
            f"List key: `{list_key or 'unknown'}`\n"
            f"_Action required: route this contact manually in GHL._"
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
