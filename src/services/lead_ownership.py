"""Lead ownership: one owning campaign per person.

claim_ownership() is called when a lead is handed off (the PropertyRadar
handoff calls it inside its create-lead savepoint). The highest-priority
campaign in config/lead_ownership.py owns the person; ownership only moves
upward:

  - no owner yet            -> the claim becomes the active owner
  - claim outranks owner    -> the old assignment is closed as `preempted`
  - owner outranks claim    -> the claim is recorded as `blocked`

An active or paused FA Max engine enrollment (fa_max_campaign_enrollments,
WP-T3-4) is a competing owner — paused still counts, since resuming it would
collide with a second sequence. It is only ever read: if the claim outranks
it, the claim is blocked rather than cancelling the engine's enrollment. The
engine table may not exist yet (its PR is unmerged); it then has no owners.

owning_campaign() is the read-side check any send path for these leads must
call before a touch.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lead_ownership import FA_MAX_ENGINE_CAMPAIGNS, priority_rank

logger = logging.getLogger(__name__)


class AssignmentStatus(str, Enum):
    ACTIVE = "active"
    PREEMPTED = "preempted"
    BLOCKED = "blocked"


_ENGINE_OWNING_STATUSES = ("active", "paused")


@dataclass(frozen=True)
class OwnershipResult:
    status: AssignmentStatus         # status of the claim's own row
    owning_campaign: str             # who owns the person after this claim
    preempted_campaign: Optional[str] = None


@dataclass(frozen=True)
class _Decision:
    status: AssignmentStatus
    owner: str
    displaced_by: Optional[str]      # set on a blocked claim: who blocked it
    preempt_ours_by: Optional[str]   # set when our active assignment must close: who took it


def _best(*campaigns: Optional[str]) -> Optional[str]:
    present = [c for c in campaigns if c]
    return min(present, key=priority_rank) if present else None


def decide(claim: str, ours: Optional[str], engine: Optional[str]) -> _Decision:
    """Pure ownership rule. `ours` = our active assignment's campaign; `engine` = engine owner."""
    if ours and engine and priority_rank(engine) < priority_rank(ours):
        # A higher engine enrollment has taken this person: close ours; the claim is
        # blocked either way (even outranking the engine, its row is not ours to cancel).
        return _Decision(AssignmentStatus.BLOCKED, engine, engine, engine)
    current = _best(ours, engine)
    if current is None:
        return _Decision(AssignmentStatus.ACTIVE, claim, None, None)
    if priority_rank(claim) < priority_rank(current):
        if current == engine:
            return _Decision(AssignmentStatus.BLOCKED, engine, engine, None)
        return _Decision(AssignmentStatus.ACTIVE, claim, None, claim)
    return _Decision(AssignmentStatus.BLOCKED, current, current, None)


_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext(:lock_key))"

_EXISTING_CLAIM_SQL = """
    SELECT status FROM lead_campaign_assignments
    WHERE person_id = CAST(:person_id AS uuid) AND campaign = :campaign
      AND COALESCE(radar_id, '') = COALESCE(:radar_id, '')
"""

_ACTIVE_ASSIGNMENT_SQL = """
    SELECT id, campaign FROM lead_campaign_assignments
    WHERE person_id = CAST(:person_id AS uuid) AND status = :active
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
            :radar_id, :status, :displaced_by, CASE WHEN :status = :blocked THEN now() END)
"""

_PREEMPT_SQL = """
    UPDATE lead_campaign_assignments
    SET status = :preempted, displaced_by = :by, ended_at = now()
    WHERE id = :id
"""

def _engine_owner(session: Session, person_id: str) -> Optional[str]:
    if "lead_ownership_engine_table" not in session.info:  # probe once per session
        session.info["lead_ownership_engine_table"] = bool(session.execute(text(_ENGINE_TABLE_SQL)).scalar())
    if not session.info["lead_ownership_engine_table"]:
        return None
    return session.execute(
        text(_ENGINE_OWNER_SQL),
        {"person_id": person_id, "statuses": list(_ENGINE_OWNING_STATUSES)},
    ).scalar()


def _active_assignment(session: Session, person_id: str) -> Optional[dict]:
    row = session.execute(
        text(_ACTIVE_ASSIGNMENT_SQL), {"person_id": person_id, "active": AssignmentStatus.ACTIVE.value}
    ).mappings().first()
    return dict(row) if row else None


def owning_campaign(session: Session, person_id: str) -> Optional[str]:
    """The campaign allowed to run a sequence for this person right now, or None."""
    ours = _active_assignment(session, person_id)
    return _best(ours and ours["campaign"], _engine_owner(session, person_id))


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
    priority_rank(campaign)  # rejects unknown campaigns before touching the DB
    # Serialize claims per person: the partial unique index alone would turn a
    # concurrent second claim into an IntegrityError instead of a preempt/block.
    session.execute(text(_LOCK_SQL), {"lock_key": f"lead_ownership:{person_id}"})

    ours = _active_assignment(session, person_id)
    ours_campaign = ours and ours["campaign"]
    engine = _engine_owner(session, person_id)

    claim_params = {"person_id": person_id, "campaign": campaign, "radar_id": radar_id}
    existing = session.execute(text(_EXISTING_CLAIM_SQL), claim_params).scalar()
    if existing is not None:
        return OwnershipResult(AssignmentStatus(existing), _best(ours_campaign, engine) or campaign)

    d = decide(campaign, ours_campaign, engine)
    preempted = None
    if d.preempt_ours_by and ours:
        session.execute(text(_PREEMPT_SQL), {
            "id": ours["id"], "by": d.preempt_ours_by, "preempted": AssignmentStatus.PREEMPTED.value,
        })
        preempted = ours_campaign
        logger.info("Lead ownership: %s preempted by %s for person %s", ours_campaign, d.preempt_ours_by, person_id)

    session.execute(text(_INSERT_SQL), {
        **claim_params, "opportunity_id": opportunity_id, "source": source,
        "status": d.status.value, "displaced_by": d.displaced_by,
        "blocked": AssignmentStatus.BLOCKED.value,
    })
    logger.info("Lead ownership: %s claim for person %s -> %s (owner %s)",
                campaign, person_id, d.status.value, d.owner)
    return OwnershipResult(d.status, d.owner, preempted)
