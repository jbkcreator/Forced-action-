"""R-1 Overflow Router: main routing logic.

``route_file(lead_id, db)`` is the single production entry point.  It:
1. Locks the file's routing state row (or creates it).
2. Loads active lenders ordered by priority.
3. Evaluates rules 1–6 generically (no lender name in code).
4. Writes the routing decision to ``file_routing`` + ``file_routing_events``.
5. Posts to Slack (best-effort; failure does not roll back the route).

Caller paths:
  - ``src.lending.enrichment.service`` (a LendingFlow lead, after enrichment): ``route_file``
  - ``src.lending.partner_pipeline`` (a partner file, after it is scored): ``route_loan_file``
  - ``src.tasks.lending_routing_sweep`` (cron for unrouted files of either kind)
  - ``src.lending.routing.decline`` (re-route after a decline)

A file is a LendingFlow lead or a partner file (see ``file_key``). After every committed decision on a
partner file the router moves that file's status and tells the partner (``partner_link``).

RULE LOGIC (generic — no lender name in code)
---------------------------------------------
The primary lender is whichever row has ``is_primary=TRUE`` and
``agreement_status='signed'``.  Its ``construction_min_amount`` is the
threshold for rule 1 vs rule 3.  Only ``agreement_status='signed'`` lenders
participate in routing; the rest are invisible.

Rule 1: ground-up AND amount >= primary.construction_min_amount
         AND primary.box passes AND primary not at cap → primary
         (otherwise it cascades to rule 3 at once, never to the hold queue: Chunk 1 Addendum 1.2.1)
Rule 2: flip AND primary.box passes AND primary not at cap → primary
Rule 3: ground-up not taken by rule 1 (below the minimum, no primary, primary capped / failing its box,
         leverage or states, or already tried) → overflow waterfall, lenders with takes_ground_up=TRUE
Rule 4: flip AND (primary box fail OR at cap)
         → overflow waterfall, flip lenders
Rule 5: DSCR or refi → overflow waterfall, DSCR lenders
Rule 6: no lender matches → hold queue ('no_match'; 'out_of_footprint' when no signed lender's box takes the
        file's state at all, Chunk 1 Addendum 1.1.2)

BRIDGE and other products not in the spec's rules fall through to rule 6
(never silently dropped — Q5 default).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import MAX_WATERFALL_DEPTH, ROUTABLE_AGREEMENT_STATUSES
from src.lending.contracts import LoanType
from config.lending_occupancy import HOMESTEAD_HOLD_REASON, OWNER_OCCUPIED
from config.lending_partners import CREDIT_BANDS, PRODUCTS
from src.lending.routing import partner_link
from src.lending.routing.file_key import FileKey, LEAD, lead_key, loan_file_key
from src.lending.routing.notify import post_route_event, post_hold_event
from src.lending.routing.rules import (
    FileProfile,
    LenderRow,
    lender_passes_box,
    load_active_lenders,
    outside_footprint,
    BypassReason,
    primary_bypass_reason,
    primary_lender,
)
from src.lending.routing.seat import seat_at_cap

logger = logging.getLogger(__name__)

# Loan-type groupings used by rules 1–5.  These are the values LendingFlow leads actually carry
# (LoanType in contracts.py, stored by the T-11 parser).  The spec's rule 5 says "DSCR or refi",
# but LendingFlow has no refi product: a refi file arrives with no loan type and goes to rule 6.
_GROUND_UP = LoanType.GROUND_UP_CONSTRUCTION.value
OUT_OF_FOOTPRINT = "out_of_footprint"
# Holds a newly signed lender or a widened box can clear: the /lenders re-route sends these back.
NO_LENDER_HOLD_REASONS = ("no_match", OUT_OF_FOOTPRINT)
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


@dataclass(frozen=True)
class Choice:
    """The lender a file goes to, and why the primary was bypassed when rule 1 could not take it (1.2.1)."""

    lender: LenderRow
    bypass: Optional[BypassReason] = None


def _choose(
    profile: FileProfile,
    signed: list[LenderRow],
    tried: list[int],
    db: Session,
) -> Optional[Choice]:
    """Rules 1–5, first match wins; None means rule 6 (hold queue).

    The single place the rules live: ``route_file`` writes the result, ``preview_next`` only reads it.
    """
    primary = primary_lender(signed)
    loan_type = profile.loan_type
    amount = profile.loan_amount
    bypass: Optional[BypassReason] = None

    if loan_type == _GROUND_UP and primary is not None:
        construction_min = primary.construction_min_amount or 0
        if amount >= construction_min and primary.id not in tried:
            at_cap = seat_at_cap(primary, db)
            if lender_passes_box(primary, profile) and not at_cap:
                return Choice(primary)   # Rule 1
            bypass = primary_bypass_reason(primary, profile, at_cap)

    if loan_type == _FLIP and primary is not None and primary.id not in tried:
        if lender_passes_box(primary, profile) and not seat_at_cap(primary, db):
            return Choice(primary)   # Rule 2

    if loan_type == _GROUND_UP:
        # Rule 1 did not take it: below the primary's minimum, no primary, the primary capped or failing its box
        # (1.2.1: cascade, never hold), or the primary already declined it.
        chosen = _waterfall(signed, loan_type, profile, tried, ground_up_only=True)  # Rule 3
        if chosen is not None:
            return Choice(chosen, bypass)

    if loan_type == _FLIP:
        chosen = _waterfall(signed, loan_type, profile, tried)  # Rule 4
        if chosen is not None:
            return Choice(chosen)

    if loan_type in _DSCR_TYPES:
        chosen = _waterfall(signed, loan_type, profile, tried, dscr_only=True)  # Rule 5
        return Choice(chosen) if chosen is not None else None

    return None


def preview_next(file_id: int, db: Session, kind: str = LEAD) -> Optional[dict]:
    """Who the file would go to next, without writing anything.

    For a routed file the current lender is already in ``lenders_tried``, so the answer is the lender a
    decline would send it to.  For an unrouted file it is the lender it would be routed to first.  None
    when no lender qualifies (the file would go to the hold queue).
    """
    key = FileKey(kind, file_id)
    file = _load_file(key, db)
    if file.get("suppressed"):
        return None
    row = db.execute(
        text(f"SELECT lenders_tried FROM lending.file_routing WHERE {key.column} = :id"), {"id": key.id},
    ).mappings().one_or_none()
    tried = list(row["lenders_tried"] or []) if row else []
    signed = _signed(load_active_lenders(db))
    choice = _choose(_build_profile(file), signed, tried, db) if signed else None
    if choice is None:
        return None
    return {"lender_id": choice.lender.id, "name": choice.lender.name, "position": len(tried) + 1}


def _build_profile(file: dict) -> FileProfile:
    return FileProfile(
        loan_type=(file.get("loan_type") or "").upper(),
        loan_amount=int(file.get("loan_amount") or 0),
        state=file.get("property_state") or "",
        credit_band_min_fico=file.get("credit_band_min_fico"),
        completed_projects=file.get("completed_projects"),  # a partner supplies it; LendingFlow does not
        purchase_price=file.get("purchase_price"),  # partner figures; a LendingFlow lead's wait for the call
        rehab_budget=file.get("rehab_budget"),
        arv=file.get("arv"),
    )


def _load_file(key: FileKey, db: Session) -> dict:
    """The facts the router needs, in one shape for both kinds of file (plus ``id`` and ``label``)."""
    if key.kind == LEAD:
        row = db.execute(
            text(
                "SELECT id, first_name, last_name, loan_type, loan_amount,"
                "       property_state, credit_band_min_fico, suppressed"
                "  FROM lending.lendingflow_leads"
                " WHERE id = :id"
            ),
            {"id": key.id},
        ).mappings().one_or_none()
        if row is None:
            raise ValueError(f"lendingflow_leads.id={key.id} not found")
        return {**dict(row), "completed_projects": None, "label": key.label}

    row = db.execute(
        text(
            "SELECT id, borrower_first_name AS first_name, borrower_last_name AS last_name, product,"
            "       loan_amount, property_state, credit_band, completed_projects, purchase_price, rehab_budget, arv,"
            "       occupancy"
            "  FROM lending.loan_files"
            " WHERE id = :id"
        ),
        {"id": key.id},
    ).mappings().one_or_none()
    if row is None:
        raise ValueError(f"loan_files.id={key.id} not found")
    file = dict(row)
    loan_type = PRODUCTS.get(file.pop("product"))  # "other" has no lender-box loan type, so no rule matches it
    band = file.pop("credit_band")
    return {
        **file,
        "loan_type": loan_type.value if loan_type is not None else "",
        "credit_band_min_fico": CREDIT_BANDS.get(band) if band else None,
        "suppressed": False,  # routing is not contact with the borrower, and a partner file is never contacted
        "label": key.label,
    }


def _upsert_file_routing(key: FileKey, db: Session) -> dict:
    """Get or create the file_routing row, locked for update."""
    conflict = "(lead_id)" if key.kind == LEAD else "(loan_file_id) WHERE loan_file_id IS NOT NULL"
    db.execute(
        text(
            f"INSERT INTO lending.file_routing ({key.column}, status)"
            f" VALUES (:id, 'unrouted')"
            f" ON CONFLICT {conflict} DO NOTHING"
        ),
        {"id": key.id},
    )
    row = db.execute(
        text(
            "SELECT id, status, current_lender_id, current_lender_name,"
            "       route_position, lenders_tried, hold_reason"
            "  FROM lending.file_routing"
            f" WHERE {key.column} = :id"
            " FOR UPDATE"
        ),
        {"id": key.id},
    ).mappings().one()
    return dict(row)


def _set_routed(
    key: FileKey,
    routing: dict,
    lender: LenderRow,
    position: int,
    db: Session,
    metadata: Optional[dict] = None,
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
    return db.execute(
        text(
            f"INSERT INTO lending.file_routing_events"
            f"  ({key.column}, lender_id, lender_name, action, actor, metadata)"
            f" VALUES (:id, :lid, :name, 'routed', 'system', CAST(:meta AS jsonb))"
            f" RETURNING id"
        ),
        {"id": key.id, "lid": lender.id, "name": lender.name, "meta": json.dumps(metadata) if metadata else None},
    ).scalar_one()


def _set_held(key: FileKey, reason: str, db: Session) -> int:
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
            f" WHERE {key.column} = :id"
        ),
        {
            "status": "exhausted" if reason == "exhausted" else "held",
            "reason": reason,
            "now": now,
            "id": key.id,
        },
    )
    return db.execute(
        text(
            f"INSERT INTO lending.file_routing_events"
            f"  ({key.column}, lender_id, lender_name, action, actor)"
            f" VALUES (:id, NULL, NULL, 'held', 'system')"
            f" RETURNING id"
        ),
        {"id": key.id},
    ).scalar_one()


# ── Public entry points ───────────────────────────────────────────────────────


def route_file(lead_id: int, db: Session) -> None:
    """Route a LendingFlow lead: rules 1–6, then commit the result (see ``_route``)."""
    _route(lead_key(lead_id), db)


def route_loan_file(loan_file_id: int, db: Session) -> None:
    """Route a partner file (lending.loan_files) the same way, then sync its status and tell the partner."""
    _route(loan_file_key(loan_file_id), db)


def reopen_file(key: FileKey, db: Session) -> bool:
    """Put a held or exhausted file back in the queue so the router tries it again.

    A held file is otherwise final: ``_route`` skips it and the sweep only picks up ``unrouted`` rows.  Reopen
    it after the reason it was held has changed (a lender was signed, a box was edited, a lender was added).
    ``lenders_tried`` is kept, so a lender that already declined the file is not offered it again.
    Returns False when the file is not held or exhausted, so there is nothing to reopen.
    """
    reopened = db.execute(
        text(
            "UPDATE lending.file_routing SET status = 'unrouted', hold_reason = NULL, updated_at = NOW()"
            f" WHERE {key.column} = :id AND status IN ('held', 'exhausted') RETURNING id"
        ),
        {"id": key.id},
    ).scalar()
    db.commit()
    if reopened is not None:
        logger.info("reopened %s %d for routing", key.kind, key.id)
    return reopened is not None


def reopen_held(db: Session, reason: str, limit: int) -> int:
    """Reopen up to ``limit`` held files that were held for ``reason`` (for example ``no_match``).

    The sweep then routes them in batches, oldest first, so a large backlog is not pushed through at once.
    Returns how many files were reopened.
    """
    ids = db.execute(
        text(
            "SELECT id FROM lending.file_routing WHERE status IN ('held', 'exhausted') AND hold_reason = :reason"
            " ORDER BY updated_at LIMIT :limit FOR UPDATE SKIP LOCKED"
        ),
        {"reason": reason, "limit": limit},
    ).scalars().all()
    if ids:
        db.execute(
            text("UPDATE lending.file_routing SET status = 'unrouted', hold_reason = NULL, updated_at = NOW()"
                 " WHERE id = ANY(:ids)"),
            {"ids": list(ids)},
        )
    db.commit()
    logger.info("reopened %d held files (reason=%s)", len(ids), reason)
    return len(ids)


def _hold(key: FileKey, file: dict, reason: str, db: Session) -> None:
    event_id = _set_held(key, reason, db)
    db.commit()
    post_hold_event(file, event_id, reason=reason, db=db)
    partner_link.mirror_status(key, partner_link.STATUS_FOR_HOLD, reason)


def _route(key: FileKey, db: Session) -> None:
    """Evaluate routing rules 1–6 for a file and commit the result.

    Idempotent: a file already routed to a lender is not re-routed.
    A file already held/exhausted is not changed.

    This function does not raise on a Slack failure — the route is committed
    and the event is retried by a sweep on slack_posted_at IS NULL.
    """
    file = _load_file(key, db)
    routing = _upsert_file_routing(key, db)

    if routing["status"] in ("routed", "held", "exhausted"):
        logger.debug("route %s %d already %s - skipping", key.kind, key.id, routing["status"])
        db.commit()  # release the row lock taken above; the status sync below uses its own session
        if key.kind != LEAD:
            # Routed earlier, but the file never took the status (the sync after the commit failed): heal it.
            partner_link.resync_if_never_synced(key, routing["status"], routing["hold_reason"], db)
        return

    if file.get("suppressed"):
        # Opted-out borrowers are never routed; the hold queue keeps the file visible (Q8).
        logger.warning("route %s %d: borrower is suppressed — holding (suppressed)", key.kind, key.id)
        _hold(key, file, "suppressed", db)
        return

    if file.get("occupancy") == OWNER_OCCUPIED:
        # Business-purpose loans only (1.1.2): the hold post tells Josh; nothing ever goes to the borrower.
        logger.info("route %s %d: owner-occupied — holding (%s)", key.kind, key.id, HOMESTEAD_HOLD_REASON)
        _hold(key, file, HOMESTEAD_HOLD_REASON, db)
        return

    profile = _build_profile(file)
    signed = _signed(load_active_lenders(db))

    if not signed:
        logger.warning("route %s %d: no signed lenders — holding (no_match)", key.kind, key.id)
        _hold(key, file, "no_match", db)
        return

    tried: list[int] = list(routing["lenders_tried"] or [])
    position = len(tried) + 1

    choice = _choose(profile, signed, tried, db)

    # Rule 6: no lender matched
    if choice is None:
        hold_reason = ("exhausted" if tried
                       else OUT_OF_FOOTPRINT if outside_footprint(signed, profile.state) else "no_match")
        logger.info("route %s %d holding reason=%s tried=%s", key.kind, key.id, hold_reason, tried)
        _hold(key, file, hold_reason, db)
        return

    chosen = choice.lender
    logger.info(
        "route %s %d -> lender_id=%d name=%r position=%d", key.kind, key.id, chosen.id, chosen.name, position,
    )
    overflow = {"overflow": True, "primary_bypass_reason": choice.bypass.value} if choice.bypass else None
    event_id = _set_routed(key, routing, chosen, position, db, overflow)
    db.commit()
    post_route_event(file, chosen, position, event_id, db=db)
    partner_link.mirror_status(key, "routed")


# Public name for the loader, used by the decline handler to build its Slack post.
load_file = _load_file

