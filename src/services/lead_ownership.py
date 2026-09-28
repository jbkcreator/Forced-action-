"""Lead ownership: one owning campaign per person.

claim_ownership() is called when a lead is handed off (the PropertyRadar
handoff calls it inside its create-lead savepoint). The highest-priority
campaign in config/lead_ownership.py owns the person; ownership only moves
upward:

  - no owner yet            -> the claim becomes the active owner
  - claim outranks owner    -> the old assignment is closed as `preempted`
  - owner outranks claim    -> the claim is recorded as `blocked`

An active FA Max engine enrollment (fa_max_campaign_enrollments, WP-T3-4) is a
competing owner. It is only ever read: if the claim outranks it, the claim is
blocked rather than cancelling the engine's enrollment. The engine table may
not exist yet (its PR is unmerged); it is then treated as having no rows.

owning_campaign() is the read-side check any send path for these leads must
call before a touch.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lead_ownership import FA_MAX_ENGINE_CAMPAIGNS, priority_rank

logger = logging.getLogger(__name__)

ACTIVE = "active"
PREEMPTED = "preempted"
BLOCKED = "blocked"

_ENGINE_ACTIVE_STATUSES = ("active", "paused")


@dataclass(frozen=True)
class OwnershipResult:
    status: str                     # status of the claim's own row: active / blocked
    owning_campaign: str            # who owns the person after this claim
    preempted_campaign: Optional[str] = None


_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext(:lock_key))"

_EXISTING_CLAIM_SQL = """
    SELECT status FROM lead_campaign_assignments
    WHERE person_id = CAST(:person_id AS uuid) AND campaign = :campaign
      AND COALESCE(radar_id, '') = COALESCE(:radar_id, '')
"""

_ACTIVE_ASSIGNMENT_SQL = """
    SELECT id, campaign FROM lead_campaign_assignments
    WHERE person_id = CAST(:person_id AS uuid) AND status = 'active'
"""

_ENGINE_TABLE_SQL = "SELECT to_regclass('fa_max_campaign_enrollments') IS NOT NULL"

_ENGINE_OWNER_SQL = """
    SELECT campaign_key FROM fa_max_campaign_enrollments
    WHERE person_id = CAST(:person_id AS uuid) AND status = ANY(:statuses)
    LIMIT 1
"""

_INSERT_SQL = """
    INSERT INTO lead_campaign_assignments
        (person_id, opportunity_id, campaign, source, radar_id, status, displaced_by, ended_at)
    VALUES (CAST(:person_id AS uuid), CAST(:opportunity_id AS uuid), :campaign, :source,
            :radar_id, :status, :displaced_by,
            CASE WHEN :status = 'blocked' THEN now() END)
"""

_PREEMPT_SQL = """
    UPDATE lead_campaign_assignments
    SET status = 'preempted', displaced_by = :by, ended_at = now()
    WHERE id = :id
"""


def _engine_owner(session: Session, person_id: str) -> Optional[str]:
    if not session.execute(text(_ENGINE_TABLE_SQL)).scalar():
        return None
    return session.execute(
        text(_ENGINE_OWNER_SQL),
        {"person_id": person_id, "statuses": list(_ENGINE_ACTIVE_STATUSES)},
    ).scalar()


def _best(*campaigns: Optional[str]) -> Optional[str]:
    present = [c for c in campaigns if c]
    return min(present, key=priority_rank) if present else None


def owning_campaign(session: Session, person_id: str) -> Optional[str]:
    """The campaign allowed to run a sequence for this person right now, or None."""
    ours = session.execute(text(_ACTIVE_ASSIGNMENT_SQL), {"person_id": person_id}).mappings().first()
    return _best(ours["campaign"] if ours else None, _engine_owner(session, person_id))


def claim_ownership(
    session: Session,
    *,
    person_id: str,
    campaign: str,
    source: str,
    opportunity_id: Optional[str] = None,
    radar_id: Optional[str] = None,
) -> OwnershipResult:
    """Record a campaign's claim on a person and resolve who owns them. Idempotent."""
    if campaign in FA_MAX_ENGINE_CAMPAIGNS:
        raise ValueError(f"{campaign!r} is owned through fa_max_campaign_enrollments, not claimed here")
    rank = priority_rank(campaign)
    # Serialize claims per person: the partial unique index alone would turn a
    # concurrent second claim into an IntegrityError instead of a preempt/block.
    session.execute(text(_LOCK_SQL), {"lock_key": f"lead_ownership:{person_id}"})

    params = {"person_id": person_id, "campaign": campaign, "radar_id": radar_id}
    ours = session.execute(text(_ACTIVE_ASSIGNMENT_SQL), params).mappings().first()
    engine = _engine_owner(session, person_id)

    existing = session.execute(text(_EXISTING_CLAIM_SQL), params).scalar()
    if existing is not None:
        return OwnershipResult(existing, _best(ours["campaign"] if ours else None, engine) or campaign)

    preempted: Optional[str] = None
    if ours and engine and priority_rank(engine) < priority_rank(ours["campaign"]):
        session.execute(text(_PREEMPT_SQL), {"id": ours["id"], "by": engine})
        logger.info("Lead ownership: %s preempted by engine campaign %s for person %s",
                    ours["campaign"], engine, person_id)
        preempted, ours = ours["campaign"], None

    current = _best(ours["campaign"] if ours else None, engine)
    if current is None or rank < priority_rank(current):
        if current is not None and current == engine:
            status, displaced_by, owner = BLOCKED, engine, engine
        else:
            if ours:
                session.execute(text(_PREEMPT_SQL), {"id": ours["id"], "by": campaign})
                preempted = ours["campaign"]
            status, displaced_by, owner = ACTIVE, None, campaign
    else:
        status, displaced_by, owner = BLOCKED, current, current

    session.execute(text(_INSERT_SQL), {
        **params, "opportunity_id": opportunity_id, "source": source,
        "status": status, "displaced_by": displaced_by,
    })
    logger.info("Lead ownership: %s claim for person %s -> %s (owner %s)", campaign, person_id, status, owner)
    return OwnershipResult(status, owner, preempted)
