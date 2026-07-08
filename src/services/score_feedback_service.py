"""
M5 — CDS Score Feedback Service (spec §4.4).

Writes score_feedback rows linking a prospect's predicted tier to the
realized outcome of a homeowner call. Provides the tier inversion check
used by the acceptance test.

closer_call_id is optional: homeowner outbound calling is not built yet.
Pass it when available so the row can be joined back to a call record later.

No event is emitted — feedback is batch-consumed by scoring_fit.py.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_VALID_OUTCOMES = frozenset({"contacted", "converted", "funded", "dead"})
_POSITIVE_OUTCOMES = frozenset({"contacted", "converted", "funded"})

# Contactability floor per grade — mirrors grade_thresholds seeds from fa091.
# 'Ultra' is used, not 'Ultra Platinum' (matches config/grading.py GRADE_ORDER).
_GRADE_PREDICTED_RATES: dict[str, Optional[float]] = {
    "Ultra":     0.40,
    "Platinum":  0.25,
    "Gold":      0.12,
    "Silver":    0.05,
    "Bronze":    None,
    "sub_grade": None,
}

# Maps closer_calls.call_outcome → score_feedback.realized_outcome.
# For future use when homeowner calling is wired to this service.
CALL_OUTCOME_MAP: dict[str, str] = {
    "committed":                  "converted",
    "callback_scheduled":         "contacted",
    "undecided":                  "contacted",
    "declined":                   "dead",
    "no_meaningful_conversation": "dead",
}


def _fetch_predicted_tier(session: Session, prospect_id: str) -> tuple[str, Optional[float]]:
    """Return (predicted_tier, predicted_rate) for a prospect.

    Prefers the latest Truth Engine verdict. Falls back to distress_scores
    joined via prospects.property_id, normalising 'Ultra Platinum' → 'Ultra'.
    Always returns a best-effort value — never raises.
    """
    row = session.execute(sa_text("""
        SELECT grade FROM verdicts
        WHERE prospect_id = CAST(:pid AS uuid)
        ORDER BY created_at DESC
        LIMIT 1
    """), {"pid": prospect_id}).fetchone()

    if row:
        grade = row.grade
        return grade, _GRADE_PREDICTED_RATES.get(grade)

    logger.warning(
        "[score_feedback] no verdict for prospect_id=%s — falling back to distress_scores",
        prospect_id,
    )
    ds_row = session.execute(sa_text("""
        SELECT ds.lead_tier
        FROM distress_scores ds
        JOIN prospects p ON p.property_id = ds.property_id
        WHERE p.prospect_id = CAST(:pid AS uuid)
        ORDER BY ds.score_date DESC
        LIMIT 1
    """), {"pid": prospect_id}).fetchone()

    if ds_row:
        raw = ds_row.lead_tier or "sub_grade"
        grade = "Ultra" if raw == "Ultra Platinum" else raw
        return grade, _GRADE_PREDICTED_RATES.get(grade)

    logger.warning(
        "[score_feedback] no distress_score for prospect_id=%s — defaulting to sub_grade",
        prospect_id,
    )
    return "sub_grade", None


def outcome_from_call_result(call_outcome: str) -> Optional[str]:
    """Map a closer_calls.call_outcome value to a realized_outcome enum value."""
    return CALL_OUTCOME_MAP.get(call_outcome)


# B0-02 — Outcome Sanity Filter. A death caused by the buyer's inability to act
# (no credit / no capital) must not teach the model that the lead is bad.
# 'no_meaningful_conversation' is deliberately excluded — it is ambiguous
# (buyer disengaged vs. lead genuinely bad) and stays a real 'dead' signal.
BUYER_CAPACITY_REASONS = frozenset({"low_fico", "no_capital"})


def normalize_reason(reason: Optional[str]) -> Optional[str]:
    """Canonicalise a raw reason for the shield check: trim + lowercase.

    So 'No_Capital', ' low_fico ', 'NO_CAPITAL' all match the canonical set —
    a case/whitespace variant must never silently fail to shield. Returns None
    for a blank/None reason.
    """
    if not reason:
        return None
    normalized = reason.strip().lower()
    return normalized or None


def classify_realized_outcome(
    outcome: str, reason: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Decide what to persist for (outcome, reason).

    Returns (realized_outcome, buyer_could_not_act_reason).

    When a Buyer-Capacity Failure reason accompanies a non-positive outcome, the
    realized outcome is nulled (the shield) and the normalised reason preserved
    for autopsy. Otherwise the outcome is written unchanged and no reason is
    stored. The reason is normalised (trim + lowercase) before matching so a
    case/whitespace variant of a canonical reason still shields.
    """
    normalized = normalize_reason(reason)
    if normalized in BUYER_CAPACITY_REASONS and outcome not in _POSITIVE_OUTCOMES:
        return None, normalized
    return outcome, None


def post_outcome(
    session: Session,
    prospect_id: str,
    outcome: str,
    reason: Optional[str] = None,
    closer_call_id: Optional[int] = None,
) -> Optional[dict]:
    """Write or update the score_feedback row for a prospect.

    Idempotent per prospect_id — re-posting with a new outcome updates in place.
    Returns None if the prospect does not exist (caller should return 404).

    B0-02 Outcome Sanity Filter: a Buyer-Capacity Failure reason (low_fico /
    no_capital) on a non-positive outcome shields the lead — realized_outcome is
    left NULL (so it never reaches a rate/training consumer) and the raw reason
    is preserved in buyer_could_not_act_reason. Re-posting flips this in either
    direction (the reason column is set by plain assignment, not COALESCE).
    """
    if outcome not in _VALID_OUTCOMES:
        raise ValueError(f"outcome must be one of {sorted(_VALID_OUTCOMES)}")

    prospect_exists = session.execute(
        sa_text("SELECT 1 FROM prospects WHERE prospect_id = CAST(:pid AS uuid)"),
        {"pid": prospect_id},
    ).fetchone()
    if not prospect_exists:
        return None

    predicted_tier, predicted_rate = _fetch_predicted_tier(session, prospect_id)

    realized_outcome, reason_stored = classify_realized_outcome(outcome, reason)

    # A reason was supplied but did not trigger the shield — make the gap visible
    # rather than silently indistinguishable from "no reason given" (the feature
    # exists to prevent quiet training-set corruption). Not an error: unknown
    # reasons deliberately fall through to a normal write.
    if reason and reason_stored is None:
        logger.warning(
            "[score_feedback] reason=%r did not match a buyer-capacity shield "
            "(outcome=%s) — writing outcome unshielded",
            reason, outcome,
        )

    delta: Optional[float] = None
    if realized_outcome is not None and predicted_rate is not None:
        realized_rate = 1.0 if realized_outcome in _POSITIVE_OUTCOMES else 0.0
        delta = round(realized_rate - predicted_rate, 4)

    row = session.execute(sa_text("""
        INSERT INTO score_feedback
            (prospect_id, closer_call_id, predicted_tier, predicted_rate,
             realized_outcome, buyer_could_not_act_reason, delta, scored_at, resolved_at)
        VALUES
            (CAST(:pid AS uuid), :ccid, :ptier, :prate,
             :outcome, :reason, :delta, NOW(), NOW())
        ON CONFLICT (prospect_id) DO UPDATE SET
            closer_call_id             = COALESCE(EXCLUDED.closer_call_id, score_feedback.closer_call_id),
            realized_outcome           = EXCLUDED.realized_outcome,
            buyer_could_not_act_reason = EXCLUDED.buyer_could_not_act_reason,
            delta                      = EXCLUDED.delta,
            resolved_at                = EXCLUDED.resolved_at
        RETURNING
            score_id, prospect_id, closer_call_id, predicted_tier,
            predicted_rate, realized_outcome, buyer_could_not_act_reason,
            delta, scored_at, resolved_at
    """), {
        "pid":     prospect_id,
        "ccid":    closer_call_id,
        "ptier":   predicted_tier,
        "prate":   float(predicted_rate) if predicted_rate is not None else None,
        "outcome": realized_outcome,
        "reason":  reason_stored,
        "delta":   float(delta) if delta is not None else None,
    }).mappings().first()

    logger.info(
        "[score_feedback] posted outcome prospect_id=%s predicted=%s outcome=%s "
        "buyer_could_not_act_reason=%s delta=%s",
        prospect_id, predicted_tier, realized_outcome, reason_stored, delta,
    )
    return dict(row) if row is not None else None


def get_feedback_rows(
    session: Session,
    *,
    predicted_tier: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    """Return resolved score_feedback rows, optionally filtered by tier."""
    where = "WHERE realized_outcome IS NOT NULL"
    params: dict = {"limit": limit}
    if predicted_tier:
        where += " AND predicted_tier = :ptier"
        params["ptier"] = predicted_tier

    rows = session.execute(sa_text(f"""
        SELECT score_id, prospect_id, closer_call_id, predicted_tier,
               predicted_rate, realized_outcome, delta, scored_at, resolved_at
        FROM score_feedback
        {where}
        ORDER BY resolved_at DESC
        LIMIT :limit
    """), params).mappings().all()

    return [dict(r) for r in rows]


def check_tier_inversion(session: Session) -> dict:
    """Acceptance gate — confirms Ultra outranks Bronze in realized outcomes.

    Returns avg realized rate per tier. inversion_fixed=True when
    Ultra avg rate >= Bronze avg rate.
    Requires at least 5 resolved rows per tier; returns status='insufficient_data'
    if the data is too sparse.
    """
    rows = session.execute(sa_text("""
        SELECT
            predicted_tier,
            COUNT(*)                                                             AS total,
            SUM(CASE WHEN realized_outcome IN ('contacted','converted','funded')
                     THEN 1 ELSE 0 END)                                          AS positive,
            AVG(CASE WHEN realized_outcome IN ('contacted','converted','funded')
                     THEN 1.0 ELSE 0.0 END)                                      AS avg_rate
        FROM score_feedback
        WHERE realized_outcome IS NOT NULL
        GROUP BY predicted_tier
        ORDER BY avg_rate DESC
    """)).mappings().all()

    if not rows:
        return {"status": "insufficient_data", "inversion_fixed": False, "min_required": 5}

    tier_rates = {
        r["predicted_tier"]: {
            "total":            int(r["total"]),
            "positive":         int(r["positive"]),
            "avg_realized_rate": float(r["avg_rate"]) if r["avg_rate"] is not None else 0.0,
        }
        for r in rows
    }

    ultra_count  = tier_rates.get("Ultra",  {}).get("total", 0)
    bronze_count = tier_rates.get("Bronze", {}).get("total", 0)

    if ultra_count < 5 or bronze_count < 5:
        return {
            "status":        "insufficient_data",
            "inversion_fixed": False,
            "min_required":  5,
            "ultra_count":   ultra_count,
            "bronze_count":  bronze_count,
        }

    ultra_rate  = tier_rates.get("Ultra",  {}).get("avg_realized_rate", 0.0)
    bronze_rate = tier_rates.get("Bronze", {}).get("avg_realized_rate", 0.0)

    return {
        "status":          "correct" if ultra_rate >= bronze_rate else "inverted",
        "inversion_fixed": ultra_rate >= bronze_rate,
        "ultra_avg_rate":  ultra_rate,
        "bronze_avg_rate": bronze_rate,
        "tier_rates":      tier_rates,
    }
