"""R-1 Overflow Router: main routing logic.

``route_file(lead_id, db)`` is the single production entry point.  It:
1. Locks the file's routing state row (or creates it).
2. Loads active lenders ordered by priority.
3. Evaluates rules 1–6 generically (no lender name in code).
4. Writes the routing decision to ``file_routing`` + ``file_routing_events``.
5. Posts to Slack (best-effort; failure does not roll back the route).

Caller paths:
  - ``src.lending.enrichment.service`` (on lead creation, after enrichment)
  - ``src.tasks.lending_routing_sweep`` (cron for unrouted files)

RULE LOGIC (generic — no lender name in code)
---------------------------------------------
The primary lender is whichever row has ``is_primary=TRUE`` and
``agreement_status='signed'``.  Its ``construction_min_amount`` is the
threshold for rule 1 vs rule 3.  Only ``agreement_status='signed'`` lenders
participate in routing; the rest are invisible.

Rule 1: ground-up AND amount >= primary.construction_min_amount
         AND primary.box passes AND primary not at cap → primary
Rule 2: flip AND primary.box passes AND primary not at cap → primary
Rule 3: ground-up AND amount < primary.construction_min_amount (or no primary)
         → overflow waterfall, lenders with takes_ground_up=TRUE
Rule 4: flip AND (primary box fail OR at cap)
         → overflow waterfall, flip lenders
Rule 5: DSCR or refi → overflow waterfall, DSCR lenders
Rule 6: no lender matches → hold queue ('no_match')

BRIDGE and other products not in the spec's rules fall through to rule 6
(never silently dropped — Q5 default).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import MAX_WATERFALL_DEPTH, ROUTABLE_AGREEMENT_STATUSES
from src.lending.contracts import LoanType
from src.lending.routing.notify import post_route_event, post_hold_event
from src.lending.routing.rules import (
    FileProfile,
    LenderRow,
    lender_passes_box,
    load_active_lenders,
    primary_lender,
)
from src.lending.routing.seat import seat_at_cap

logger = logging.getLogger(__name__)

# Loan-type groupings used by rules 1–5.  These are the values LendingFlow leads actually carry
# (LoanType in contracts.py, stored by the T-11 parser).  The spec's rule 5 says "DSCR or refi",
# but LendingFlow has no refi product: a refi file arrives with no loan type and goes to rule 6.
_GROUND_UP = LoanType.GROUND_UP_CONSTRUCTION.value
_FLIP = LoanType.FIX_AND_FLIP.value
_DSCR_TYPES = frozenset({LoanType.DSCR_RENTAL.value})


# ── Internal helpers ─────────────────────────────────────────────────────────


def _signed(lenders: list[LenderRow]) -> list[LenderRow]:
    """Filter to only signed, active lenders."""
    return [lr for lr in lenders if lr.agreement_status in ROUTABLE_AGREEMENT_STATUSES and lr.active]


def _waterfall(
    lenders: list[LenderRow],
    loan_type: str,
    profile: FileProfile,
    lenders_tried: list[int],
    *,
    ground_up_only: bool = False,
    dscr_only: bool = False,
) -> Optional[LenderRow]:
    """Return the first signed, active lender in priority order that:
    - accepts the loan_type in at least one product box
    - passes the box constraints for the file
    - has not already been tried (lenders_tried)
    - is not the primary (primary has its own rules in the caller)
    - satisfies takes_ground_up when ground_up_only
    """
    seen = set(lenders_tried)
    for lr in lenders:
        if lr.is_primary:
            continue
        if lr.id in seen:
            continue
        if ground_up_only and not lr.takes_ground_up:
            continue
        if dscr_only and not any(_DSCR_TYPES & box.loan_types for box in lr.boxes):
            continue
        if not lender_passes_box(lr, profile):
            continue
        return lr
    return None


def _choose(
    profile: FileProfile,
    signed: list[LenderRow],
    tried: list[int],
    db: Session,
) -> Optional[LenderRow]:
    """Rules 1–5, first match wins; None means rule 6 (hold queue).

    The single place the rules live: ``route_file`` writes the result, ``preview_next`` only reads it.
    """
    primary = primary_lender(signed)
    loan_type = profile.loan_type
    amount = profile.loan_amount

    if loan_type == _GROUND_UP and primary is not None:
        construction_min = primary.construction_min_amount or 0
        if amount >= construction_min and primary.id not in tried:
            if lender_passes_box(primary, profile) and not seat_at_cap(primary, db):
                return primary   # Rule 1

    if loan_type == _FLIP and primary is not None and primary.id not in tried:
        if lender_passes_box(primary, profile) and not seat_at_cap(primary, db):
            return primary   # Rule 2

    if loan_type == _GROUND_UP:
        construction_min = (primary.construction_min_amount or 0) if primary else 0
        if amount < construction_min or primary is None:
            chosen = _waterfall(signed, loan_type, profile, tried, ground_up_only=True)  # Rule 3
            if chosen is not None:
                return chosen

    if loan_type == _FLIP:
        chosen = _waterfall(signed, loan_type, profile, tried)  # Rule 4
        if chosen is not None:
            return chosen

    if loan_type in _DSCR_TYPES:
        return _waterfall(signed, loan_type, profile, tried, dscr_only=True)  # Rule 5

    return None


def preview_next(lead_id: int, db: Session) -> Optional[dict]:
    """Who the file would go to next, without writing anything.

    For a routed file the current lender is already in ``lenders_tried``, so the answer is the lender a
    decline would send it to.  For an unrouted file it is the lender it would be routed to first.  None
    when no lender qualifies (the file would go to the hold queue).
    """
    lead = _load_lead(lead_id, db)
    if lead.get("suppressed"):
        return None
    row = db.execute(
        text("SELECT lenders_tried FROM lending.file_routing WHERE lead_id = :lid"), {"lid": lead_id},
    ).mappings().one_or_none()
    tried = list(row["lenders_tried"] or []) if row else []
    signed = _signed(load_active_lenders(db))
    chosen = _choose(_build_profile(lead), signed, tried, db) if signed else None
    if chosen is None:
        return None
    return {"lender_id": chosen.id, "name": chosen.name, "position": len(tried) + 1}


def _build_profile(lead: dict) -> FileProfile:
    return FileProfile(
        loan_type=(lead.get("loan_type") or "").upper(),
        loan_amount=int(lead.get("loan_amount") or 0),
        state=lead.get("property_state") or "",
        credit_band_min_fico=lead.get("credit_band_min_fico"),
        completed_projects=None,   # not captured by LendingFlow in v1
    )


def _load_lead(lead_id: int, db: Session) -> dict:
    row = db.execute(
        text(
            "SELECT id, first_name, last_name, loan_type, loan_amount,"
            "       property_state, credit_band_min_fico, suppressed"
            "  FROM lending.lendingflow_leads"
            " WHERE id = :id"
        ),
        {"id": lead_id},
    ).mappings().one_or_none()
    if row is None:
        raise ValueError(f"lendingflow_leads.id={lead_id} not found")
    return dict(row)


def _upsert_file_routing(lead_id: int, db: Session) -> dict:
    """Get or create the file_routing row, locked for update."""
    db.execute(
        text(
            "INSERT INTO lending.file_routing (lead_id, status)"
            " VALUES (:lid, 'unrouted')"
            " ON CONFLICT (lead_id) DO NOTHING"
        ),
        {"lid": lead_id},
    )
    row = db.execute(
        text(
            "SELECT id, status, current_lender_id, current_lender_name,"
            "       route_position, lenders_tried, hold_reason"
            "  FROM lending.file_routing"
            " WHERE lead_id = :lid"
            " FOR UPDATE"
        ),
        {"lid": lead_id},
    ).mappings().one()
    return dict(row)


def _set_routed(
    routing: dict,
    lender: LenderRow,
    position: int,
    db: Session,
) -> int:
    """Write the 'routed' state and append an event. Returns the event id."""
    lenders_tried = list(routing["lenders_tried"] or []) + [lender.id]
    now = datetime.now(timezone.utc)
    db.execute(
        text(
            "UPDATE lending.file_routing"
            "   SET status = 'routed',"
            "       current_lender_id = :lid,"
            "       current_lender_name = :name,"
            "       route_position = :pos,"
            "       lenders_tried = :tried,"
            "       hold_reason = NULL,"
            "       updated_at = :now"
            " WHERE id = :pk"
        ),
        {
            "lid": lender.id,
            "name": lender.name,
            "pos": position,
            "tried": lenders_tried,
            "now": now,
            "pk": routing["id"],
        },
    )
    event_id = db.execute(
        text(
            "INSERT INTO lending.file_routing_events"
            "  (lead_id, lender_id, lender_name, action, actor)"
            " VALUES (:lead_id, :lid, :name, 'routed', 'system')"
            " RETURNING id"
        ),
        {"lead_id": routing["lead_id"], "lid": lender.id, "name": lender.name},
    ).scalar_one()
    return event_id


def _set_held(
    routing: dict,
    reason: str,
    db: Session,
) -> int:
    """Write the 'held' state and append an event. Returns the event id."""
    now = datetime.now(timezone.utc)
    db.execute(
        text(
            "UPDATE lending.file_routing"
            "   SET status = :status,"
            "       current_lender_id = NULL,"
            "       current_lender_name = NULL,"
            "       route_position = NULL,"
            "       hold_reason = :reason,"
            "       updated_at = :now"
            " WHERE lead_id = :lead"
        ),
        {
            "status": "exhausted" if reason == "exhausted" else "held",
            "reason": reason,
            "now": now,
            "lead": routing.get("lead_id"),
        },
    )
    event_id = db.execute(
        text(
            "INSERT INTO lending.file_routing_events"
            "  (lead_id, lender_id, lender_name, action, actor)"
            " VALUES (:lead_id, NULL, NULL, 'held', 'system')"
            " RETURNING id"
        ),
        {"lead_id": routing.get("lead_id")},
    ).scalar_one()
    return event_id


# ── Public entry point ────────────────────────────────────────────────────────


def route_file(lead_id: int, db: Session) -> None:
    """Evaluate routing rules 1–6 for a file and commit the result.

    Idempotent: a file already routed to a lender is not re-routed.
    A file already held/exhausted is not changed.

    This function does not raise on a Slack failure — the route is committed
    and the event is retried by a sweep on slack_posted_at IS NULL.
    """
    lead = _load_lead(lead_id, db)
    routing = _upsert_file_routing(lead_id, db)

    if routing["status"] in ("routed", "held", "exhausted"):
        logger.debug(
            "route_file lead_id=%d already %s — skipping", lead_id, routing["status"]
        )
        return

    # Patch the lead_id into routing dict (needed by _set_routed/_set_held)
    routing = {**routing, "lead_id": lead_id}

    if lead.get("suppressed"):
        # Opted-out borrowers are never routed; the hold queue keeps the file visible (Q8).
        logger.warning("route_file lead_id=%d: lead is suppressed — holding (suppressed)", lead_id)
        event_id = _set_held(routing, "suppressed", db)
        db.commit()
        post_hold_event(lead, event_id, reason="suppressed", db=db)
        return

    profile = _build_profile(lead)
    all_lenders = load_active_lenders(db)
    signed = _signed(all_lenders)

    if not signed:
        logger.warning("route_file lead_id=%d: no signed lenders — holding (no_match)", lead_id)
        event_id = _set_held(routing, "no_match", db)
        db.commit()
        post_hold_event(lead, event_id, reason="no_match", db=db)
        return

    tried: list[int] = list(routing["lenders_tried"] or [])
    position = len(tried) + 1

    chosen = _choose(profile, signed, tried, db)

    # Rule 6: no lender matched
    if chosen is None:
        hold_reason = "exhausted" if tried else "no_match"
        logger.info(
            "route_file lead_id=%d holding reason=%s tried=%s",
            lead_id, hold_reason, tried,
        )
        event_id = _set_held(routing, hold_reason, db)
        db.commit()
        post_hold_event(lead, event_id, reason=hold_reason, db=db)
        return

    logger.info(
        "route_file lead_id=%d -> lender_id=%d name=%r position=%d",
        lead_id, chosen.id, chosen.name, position,
    )
    event_id = _set_routed(routing, chosen, position, db)
    db.commit()
    post_route_event(lead, chosen, position, event_id, db=db)
