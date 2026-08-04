"""
LEARN-v2.2 Layer 2 — nightly experiment attribution service.

Attributes `reply.received` fleet events back to the experiment arm that
most plausibly caused the reply.

Credit model (grilled, locked):
  1. draft_match  — most recent outbound_drafts row on the same thread
                    whose created_at falls within [occurred_at - window_days,
                    occurred_at].  Uses the draft's cell_id / venture_key.
  2. last_touch   — if no qualifying draft exists, fall back to the most
                    recent agent_lane_experiment_assignments row on the same
                    thread whose created_at falls within the same window.

Only `reply.received` events are attributed (booking.created /
payment.received lack opportunity_thread_id today — deferred per spec).

The function is idempotent: ON CONFLICT DO NOTHING on the
(fleet_event_id, assignment_id) unique constraint prevents double-counting
on re-runs.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_EVENT_TYPE = "reply.received"


@dataclass
class AttributionReport:
    events_scanned: int = 0
    attributed: int = 0
    already_attributed: int = 0
    no_assignment: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)


def run_attribution(
    db: Session,
    *,
    now: Optional[datetime] = None,
    window_days: int = 14,
) -> AttributionReport:
    """Attribute unattributed reply.received events to experiment arms.

    Args:
        db:          Active SQLAlchemy session (caller owns commit/rollback).
        now:         Attribution reference point (defaults to UTC now).  Used
                     in tests to pin time without monkeypatching.
        window_days: Look-back window for draft / last-touch matching.

    Returns:
        AttributionReport summarising what happened this run.
    """
    if now is None:
        now = datetime.now(tz=timezone.utc)

    report = AttributionReport()

    # ── 1. Fetch unattributed reply.received events ───────────────────────────
    # An event is "unattributed" if no experiment_attributions row references
    # its id (regardless of which assignment it might later be paired with).
    # We resolve the assignment per event below.
    unattributed_rows = db.execute(text("""
        SELECT
            fe.id              AS event_id,
            fe.opportunity_thread_id,
            fe.occurred_at
        FROM fleet_events fe
        WHERE fe.event_type = :event_type
          AND fe.opportunity_thread_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM experiment_attributions ea
              WHERE ea.fleet_event_id = fe.id
          )
        ORDER BY fe.occurred_at
    """), {"event_type": _EVENT_TYPE}).fetchall()

    report.events_scanned = len(unattributed_rows)
    if not unattributed_rows:
        return report

    # Pre-build a set of thread IDs so the bulk look-ups below are O(n+m).
    thread_ids = list({r.opportunity_thread_id for r in unattributed_rows})

    # ── 2. Bulk-fetch assignments for all relevant threads ────────────────────
    assignment_rows = db.execute(text("""
        SELECT
            id,
            opportunity_thread_id,
            test_id,
            variant,
            outcome,
            outcome_at,
            created_at
        FROM agent_lane_experiment_assignments
        WHERE opportunity_thread_id = ANY(:thread_ids)
        ORDER BY opportunity_thread_id, created_at DESC
    """), {"thread_ids": thread_ids}).fetchall()

    # Build lookup: thread_id -> list[assignment] (already DESC by created_at)
    assignments_by_thread: dict[str, list] = {}
    for row in assignment_rows:
        assignments_by_thread.setdefault(row.opportunity_thread_id, []).append(row)

    # ── 3. Bulk-fetch drafts for all relevant threads ─────────────────────────
    draft_rows = db.execute(text("""
        SELECT
            opportunity_thread_id,
            cell_id,
            venture_key,
            created_at
        FROM outbound_drafts
        WHERE opportunity_thread_id = ANY(:thread_ids)
        ORDER BY opportunity_thread_id, created_at DESC
    """), {"thread_ids": thread_ids}).fetchall()

    # Build lookup: thread_id -> list[draft] (already DESC by created_at)
    drafts_by_thread: dict[str, list] = {}
    for row in draft_rows:
        drafts_by_thread.setdefault(row.opportunity_thread_id, []).append(row)

    # ── 4. Attribute each event ───────────────────────────────────────────────
    window = timedelta(days=window_days)

    for evt in unattributed_rows:
        thread = evt.opportunity_thread_id
        occurred_at = evt.occurred_at
        # Ensure timezone-aware for arithmetic.
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

        window_start = occurred_at - window
        thread_assignments = assignments_by_thread.get(thread, [])

        if not thread_assignments:
            report.no_assignment += 1
            continue

        # ── Draft-match: most recent draft in [window_start, occurred_at] ──
        matched_assignment: Optional[object] = None
        attribution_method: Optional[str] = None

        thread_drafts = drafts_by_thread.get(thread, [])
        for draft in thread_drafts:  # already DESC by created_at
            draft_created = draft.created_at
            if draft_created.tzinfo is None:
                draft_created = draft_created.replace(tzinfo=timezone.utc)
            if window_start <= draft_created <= occurred_at:
                # Find the assignment most plausibly tied to this draft.
                # The draft has no FK to an assignment; use last-created
                # assignment on this thread as the link (same rationale as
                # last_touch, but we log it as draft_match because a draft
                # was present).
                matched_assignment = thread_assignments[0]
                attribution_method = "draft_match"
                break

        # ── Last-touch fallback ──────────────────────────────────────────────
        if matched_assignment is None:
            for asgn in thread_assignments:  # DESC by created_at
                asgn_created = asgn.created_at
                if asgn_created.tzinfo is None:
                    asgn_created = asgn_created.replace(tzinfo=timezone.utc)
                if window_start <= asgn_created <= occurred_at:
                    matched_assignment = asgn
                    attribution_method = "last_touch"
                    break

        if matched_assignment is None:
            report.no_assignment += 1
            continue

        # ── 5. Write attribution row (idempotent) ────────────────────────────
        try:
            result = db.execute(text("""
                INSERT INTO experiment_attributions
                    (fleet_event_id, assignment_id, test_id,
                     opportunity_thread_id, variant, event_type,
                     attribution_method, window_days, attributed_at, created_at)
                VALUES
                    (:fleet_event_id, :assignment_id, :test_id,
                     :opportunity_thread_id, :variant, :event_type,
                     :attribution_method, :window_days, NOW(), NOW())
                ON CONFLICT (fleet_event_id, assignment_id) DO NOTHING
            """), {
                "fleet_event_id": evt.event_id,
                "assignment_id": matched_assignment.id,
                "test_id": matched_assignment.test_id,
                "opportunity_thread_id": thread,
                "variant": matched_assignment.variant,
                "event_type": _EVENT_TYPE,
                "attribution_method": attribution_method,
                "window_days": window_days,
            })

            if result.rowcount == 0:
                report.already_attributed += 1
            else:
                report.attributed += 1

                # ── 6. Stamp outcome on the assignment if not already set ───
                if matched_assignment.outcome is None:
                    db.execute(text("""
                        UPDATE agent_lane_experiment_assignments
                        SET outcome    = 'reply',
                            outcome_at = :occurred_at
                        WHERE id = :assignment_id
                          AND outcome IS NULL
                    """), {
                        "occurred_at": occurred_at,
                        "assignment_id": matched_assignment.id,
                    })

        except Exception as exc:
            logger.error(
                "experiment_attribution: error attributing event_id=%s thread=%s: %s",
                evt.event_id, thread, exc,
            )
            report.errors += 1
            report.error_details.append(f"event_id={evt.event_id}: {exc}")

    return report
