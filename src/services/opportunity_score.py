"""
Opportunity scoring service — REVINT-I1.

Computes and persists OpportunityScore rows using cold-start Bayesian priors
when no actuals exist, and rolling-median actuals once ≥10 samples are available.

NBRA score = expected_retained_gross_profit_cents / josh_minutes_required.
Automated actions carry josh_minutes_required=0 and nbra_score=None — they
bypass the NBRA queue and are dispatched directly to Relay.

NOTE: RevenueType.REFERRAL_FEE (lender_intro) financial projections are
DISABLED until RESPA clearance is confirmed. Any scoring call with
revenue_type=RevenueType.REFERRAL_FEE will raise ValueError until cleared.
"""

from __future__ import annotations

import logging
import statistics
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import OpportunityScore, OpportunityScoreHistory, RevenueType

logger = logging.getLogger(__name__)

# ── Cold-start priors ────────────────────────────────────────────────────────
# All values configurable here; never hardcoded deep in logic.

COLD_START_PRIORS: dict[str, dict] = {
    "whale": {
        "p_reply": 0.15,
        "p_close": 0.05,
        "time_to_cash_days": 14,
    },
    "auction_winner": {
        "p_reply": 0.20,
        "p_close": 0.07,
        "time_to_cash_days": 10,
    },
    "lapsed_subscriber": {
        "p_reply": 0.25,
        "p_close": 0.10,
        "time_to_cash_days": 7,
    },
    "default": {
        "p_reply": 0.05,
        "p_close": 0.02,
        "time_to_cash_days": 21,
    },
}

# Minimum actuals required before we trust rolling median over cold-start priors.
ACTUALS_MIN_SAMPLES = 10

# Default josh-minutes per action_type when no rolling actuals yet.
# Standing-order / automated actions get 0.0 — they never enter the NBRA denominator.
JOSH_MINUTES_DEFAULTS: dict[str, float] = {
    "email_outreach": 5.0,
    "sms_outreach": 3.0,
    "call_outreach": 15.0,
    "proposal_send": 20.0,
    "follow_up": 4.0,
    "lender_intro": 10.0,
    "standing_order": 0.0,   # automated — bypasses NBRA
    "relay_auto": 0.0,        # automated — bypasses NBRA
}


# ── ID minting ───────────────────────────────────────────────────────────────

def mint_opportunity_thread_id(year: int, sequence: int) -> str:
    """Return OPP-YYYY-##### format identifier."""
    return f"OPP-{year}-{sequence:05d}"


# ── NBRA computation ─────────────────────────────────────────────────────────

def compute_nbra_score(
    expected_rgp_cents: int,
    josh_minutes: float,
) -> Optional[float]:
    """
    Returns expected_rgp_cents / josh_minutes, or None when josh_minutes == 0.
    None signals an automated action — never enters the NBRA queue denominator.
    """
    if josh_minutes == 0:
        return None
    return expected_rgp_cents / josh_minutes


# ── Josh-minutes estimation ──────────────────────────────────────────────────

def _estimate_josh_minutes(
    db: Session,
    source_action_type: Optional[str],
    is_automated: bool,
) -> float:
    """
    Returns 0.0 for automated actions.
    For manual actions: uses rolling median of actuals when ≥ ACTUALS_MIN_SAMPLES
    exist; falls back to JOSH_MINUTES_DEFAULTS.
    """
    if is_automated:
        return 0.0

    if source_action_type is None:
        return JOSH_MINUTES_DEFAULTS.get("email_outreach", 5.0)

    # Try rolling median from actuals (josh_minutes_required logged on completed scores)
    rows = db.execute(
        text(
            """
            SELECT josh_minutes_required
            FROM opportunity_scores
            WHERE source_action_type = :action_type
              AND is_automated = FALSE
              AND josh_minutes_required > 0
            ORDER BY created_at DESC
            LIMIT 50
            """
        ),
        {"action_type": source_action_type},
    ).fetchall()

    if len(rows) >= ACTUALS_MIN_SAMPLES:
        values = [float(r[0]) for r in rows]
        return statistics.median(values)

    return JOSH_MINUTES_DEFAULTS.get(source_action_type, 5.0)


# ── Core scoring functions ────────────────────────────────────────────────────

def get_or_create_score(
    db: Session,
    buyer_entity_id: int,
    opportunity_thread_id: str,
    segment: str,
    revenue_type: RevenueType,
    expected_revenue_cents: int,
    expected_retained_gross_profit_cents: int,
    expected_mrr_cents: Optional[int] = None,
    billing_interval: Optional[str] = None,
    source_action_type: Optional[str] = None,
    is_automated: bool = False,
) -> OpportunityScore:
    """
    Returns an existing OpportunityScore for the given thread, or creates one.
    Uses cold-start priors when no actuals exist for the segment.

    Raises ValueError for RevenueType.REFERRAL_FEE — calculation disabled
    until RESPA clearance.
    """
    # NOTE: REFERRAL_FEE projections disabled until RESPA legal clearance.
    if revenue_type == RevenueType.REFERRAL_FEE:
        raise ValueError(
            "RevenueType.REFERRAL_FEE scoring is disabled pending RESPA clearance. "
            "Do not compute financial projections for lender_intro actions."
        )

    existing = db.execute(
        text(
            "SELECT id FROM opportunity_scores WHERE opportunity_thread_id = :tid LIMIT 1"
        ),
        {"tid": opportunity_thread_id},
    ).fetchone()

    if existing:
        return db.get(OpportunityScore, existing[0])  # type: ignore[return-value]

    priors = COLD_START_PRIORS.get(segment, COLD_START_PRIORS["default"])
    josh_minutes = _estimate_josh_minutes(db, source_action_type, is_automated)
    nbra = compute_nbra_score(expected_retained_gross_profit_cents, josh_minutes)

    score = OpportunityScore(
        opportunity_thread_id=opportunity_thread_id,
        buyer_entity_id=buyer_entity_id,
        segment=segment,
        revenue_type=revenue_type.value,
        billing_interval=billing_interval,
        expected_revenue_cents=expected_revenue_cents,
        expected_mrr_cents=expected_mrr_cents,
        expected_retained_gross_profit_cents=expected_retained_gross_profit_cents,
        p_reply=priors["p_reply"],
        p_close=priors["p_close"],
        time_to_cash_days=priors["time_to_cash_days"],
        josh_minutes_required=josh_minutes,
        nbra_score=nbra,
        source_action_type=source_action_type,
        is_automated=is_automated,
    )
    db.add(score)
    db.flush()
    logger.info(
        "opportunity_score: created id=%s thread=%s segment=%s nbra=%s",
        score.id, opportunity_thread_id, segment, nbra,
    )
    return score


def recalculate_score(
    db: Session,
    opportunity_score_id: int,
    reason: Optional[str] = None,
) -> OpportunityScore:
    """
    Refreshes p_reply, p_close, time_to_cash_days, nbra_score on an existing
    OpportunityScore using current actuals/priors, then appends a history record.
    """
    score = db.get(OpportunityScore, opportunity_score_id)
    if score is None:
        raise ValueError(f"OpportunityScore id={opportunity_score_id} not found")

    priors = COLD_START_PRIORS.get(score.segment, COLD_START_PRIORS["default"])
    josh_minutes = _estimate_josh_minutes(db, score.source_action_type, score.is_automated)
    nbra = compute_nbra_score(score.expected_retained_gross_profit_cents, josh_minutes)

    score.p_reply = priors["p_reply"]
    score.p_close = priors["p_close"]
    score.time_to_cash_days = priors["time_to_cash_days"]
    score.josh_minutes_required = josh_minutes
    score.nbra_score = nbra
    score.updated_at = datetime.now(timezone.utc)

    history = OpportunityScoreHistory(
        opportunity_score_id=score.id,
        opportunity_thread_id=score.opportunity_thread_id,
        snapshot_at=datetime.now(timezone.utc),
        p_reply=float(score.p_reply),
        p_close=float(score.p_close),
        time_to_cash_days=score.time_to_cash_days,
        nbra_score=float(nbra) if nbra is not None else 0.0,
        reason=reason,
    )
    db.add(history)
    db.flush()
    logger.info(
        "opportunity_score: recalculated id=%s thread=%s nbra=%s reason=%r",
        score.id, score.opportunity_thread_id, nbra, reason,
    )
    return score
