"""
M6 — Lead Quality Truth Engine.

Reconciles claimed quality (CDS score) against realized reachability
(contactability) and produces an explainable, routed verdict per prospect
(spec §3.1, §3.1a, §4.5, §12.1). It does NOT compute the CDS score (reads the
latest distress_scores row) and does NOT mutate prospects.

Grade thresholds live in the grade_thresholds config table (tunable without a
deploy). CDS is compared on the 0–100 scale; contactability on the 0–1 scale.

Contactability resolution is 3-tier: per-prospect rate (>=5 attempts) → cohort
rate → contactability_state. Pre-launch there are no attempts, so the state gate
is the effective path; the numeric floor activates automatically once rate data
exists.
"""
import uuid
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.grading import (
    GOLD_PLUS_GRADES,
    GRADE_CHANNEL_ROUTING,
    GRADE_ORDER,
    compute_cohort_key,
    grade_rank,
    lower_grade,
    primary_channel,
)
from src.core.models import Verdict
from src.services.event_bus import emit_event
from src.services.prospect_service import get_prospect
from src.utils.logger import get_logger

logger = get_logger(__name__)

SOURCE_COMPONENT = "truth_engine"


def _load_thresholds(session: Session) -> dict[str, dict]:
    """Return active grade thresholds keyed by grade."""
    rows = session.execute(sa_text("""
        SELECT grade, cds_min, cds_max, contactability_min, requires_mobile_consent
        FROM grade_thresholds
        WHERE is_active = true
    """)).mappings().all()
    return {r["grade"]: dict(r) for r in rows}


def _cds_band_grade(score: float, thresholds: dict[str, dict]) -> str:
    """Highest grade whose cds_min lower bound the score meets.

    Bands are matched by lower bound only (mirroring config/scoring.py
    LEAD_TIER_THRESHOLDS), walking grades high → low. This keeps the bands
    contiguous so a fractional score between two integer band edges (e.g. 84.5,
    which is neither <=84 nor >=85) cannot fall through a gap into sub_grade.
    cds_max is retained on grade_thresholds for documentation/validation but is
    intentionally not used for matching. sub_grade (cds_min IS NULL) is the
    bottom catch-all.
    """
    for grade in reversed(GRADE_ORDER):
        t = thresholds.get(grade)
        if t is None:
            continue
        lo = t["cds_min"]
        if lo is None or score >= lo:
            return grade
    return "sub_grade"


def _contactability_grade_for_rate(rate: float, thresholds: dict[str, dict]) -> str:
    """Highest grade whose contactability_min the rate satisfies (NULL floor = always met)."""
    for grade in reversed(GRADE_ORDER):
        t = thresholds.get(grade)
        if t is None:
            continue
        floor = t["contactability_min"]
        if floor is None or rate >= float(floor):
            return grade
    return "sub_grade"


def assign_grade(
    score: float,
    rate: Optional[float],
    contactable: bool,
    has_mobile_consent: bool,
    thresholds: dict[str, dict],
) -> dict:
    """Pure grading: combine CDS band and realized contactability into a final grade.

    Returns {grade, contactability_flag, cds_band_grade, contactability_grade}.
    Contactability can only pull the grade DOWN, never promote above the CDS band.
    """
    cds_band = _cds_band_grade(score, thresholds)

    if rate is not None:
        contact_grade = _contactability_grade_for_rate(rate, thresholds)
    else:
        contact_grade = cds_band if contactable else "sub_grade"

    final = lower_grade(cds_band, contact_grade)

    # Per-grade mobile+consent gate (Ultra): demote one grade if unmet.
    t_final = thresholds.get(final)
    if t_final and t_final.get("requires_mobile_consent") and not has_mobile_consent:
        idx = grade_rank(final)
        final = GRADE_ORDER[idx - 1] if idx > 0 else final

    # Flag only the contactability gap (a low rate pulling a Gold+ lead down),
    # not the consent cap above. Requires a numeric rate to be meaningful.
    flag = (
        rate is not None
        and cds_band in GOLD_PLUS_GRADES
        and grade_rank(contact_grade) < grade_rank(cds_band)
    )
    return {
        "grade": final,
        "contactability_flag": flag,
        "cds_band_grade": cds_band,
        "contactability_grade": contact_grade,
    }


def _has_mobile_consent(prospect: dict) -> bool:
    """A validated mobile plus per-channel consent for sms or call.

    channel_consent is a flat boolean map per channel, e.g. {"sms": true, "call": true}
    (confirmed with M2/Dev A). We match an explicit `is True` so anything else —
    false, missing, or an unexpected non-boolean shape — fails closed (no consent).
    """
    ec = prospect.get("enriched_contact") or {}
    if not ec.get("mobile"):
        return False
    consent = prospect.get("channel_consent") or {}
    if not isinstance(consent, dict):
        return False
    return consent.get("sms") is True or consent.get("call") is True


def _latest_cds(session: Session, property_id: int) -> Optional[dict]:
    row = session.execute(sa_text("""
        SELECT final_cds_score, lead_tier, county_id, vertical_scores, factor_scores
        FROM distress_scores
        WHERE property_id = :pid
        ORDER BY score_date DESC
        LIMIT 1
    """), {"pid": property_id}).mappings().first()
    return dict(row) if row else None


def _cohort_rate(session: Session, cohort_key: str) -> Optional[float]:
    row = session.execute(sa_text("""
        SELECT contactability_rate, contact_attempts
        FROM cohort_rates
        WHERE cohort_key = :k
    """), {"k": cohort_key}).mappings().first()
    if row and row["contact_attempts"] and row["contactability_rate"] is not None:
        return float(row["contactability_rate"])
    return None


def _write_verdict(
    session: Session,
    *,
    prospect_id: str,
    grade: str,
    routed_channel: str,
    contactability_flag: bool,
    contributing_factors: dict,
    actor: str,
) -> str:
    verdict = Verdict(
        prospect_id=prospect_id,
        grade=grade,
        routed_channel=routed_channel,
        contactability_flag=contactability_flag,
        contributing_factors=contributing_factors,
    )
    session.add(verdict)
    session.flush()
    emit_event(
        session,
        event_type="truth.verdict",
        actor=actor,
        source_component=SOURCE_COMPONENT,
        prospect_id=uuid.UUID(prospect_id),
        payload={
            "verdict_id": str(verdict.verdict_id),
            "grade": grade,
            "routed_channel": routed_channel,
            "contactability_flag": contactability_flag,
        },
    )
    return str(verdict.verdict_id)


def grade_prospect(session: Session, prospect_id: str, *, actor: str = SOURCE_COMPONENT) -> Optional[dict]:
    """Grade one enriched prospect and write an explainable verdict + truth.verdict event.

    Returns the verdict dict, or {"held": True, ...} when no CDS score exists yet
    (the lead is left untouched and should be re-tried), or None if the prospect
    is unknown. The caller owns the transaction commit.
    """
    prospect = get_prospect(session, prospect_id)
    if prospect is None:
        logger.warning("[TruthEngine] prospect not found prospect_id=%s", prospect_id)
        return None

    cds = _latest_cds(session, prospect["property_id"])
    if cds is None or cds["final_cds_score"] is None:
        return {"held": True, "reason": "no_cds_score", "prospect_id": prospect_id}

    score = float(cds["final_cds_score"])
    lead_tier = cds["lead_tier"]
    county_id = cds["county_id"]
    ec = prospect.get("enriched_contact") or {}
    source = ec.get("source")

    rate = prospect.get("contactability_rate")
    if rate is not None:
        rate = float(rate)
        contactability_source = "per_prospect"
        cohort_key = compute_cohort_key(lead_tier, county_id, source)
    else:
        cohort_key = compute_cohort_key(lead_tier, county_id, source)
        cohort = _cohort_rate(session, cohort_key)
        if cohort is not None:
            rate = cohort
            contactability_source = "cohort"
        else:
            contactability_source = "state_only"

    contactable = prospect.get("contactability_state") == "contactable"
    has_consent = _has_mobile_consent(prospect)
    thresholds = _load_thresholds(session)

    result = assign_grade(score, rate, contactable, has_consent, thresholds)
    grade = result["grade"]
    routed_channel = primary_channel(grade)

    contributing_factors = {
        "cds_score": score,
        "cds_scale": "0-100",
        "cds_lead_tier": lead_tier,
        "cds_band_grade": result["cds_band_grade"],
        "contactability_grade": result["contactability_grade"],
        "contactability_state": prospect.get("contactability_state"),
        "contactability_rate": rate,
        "contactability_source": contactability_source,
        "cohort_key": cohort_key,
        "enriched_contact": {"source": source, "confidence": ec.get("confidence")},
        "routed_channels": GRADE_CHANNEL_ROUTING.get(grade, []),
        "vertical_scores": cds.get("vertical_scores"),
        "factor_scores": cds.get("factor_scores"),
    }

    verdict_id = _write_verdict(
        session,
        prospect_id=prospect_id,
        grade=grade,
        routed_channel=routed_channel,
        contactability_flag=result["contactability_flag"],
        contributing_factors=contributing_factors,
        actor=actor,
    )
    logger.info(
        "[TruthEngine] graded prospect_id=%s grade=%s channel=%s flag=%s source=%s",
        prospect_id, grade, routed_channel, result["contactability_flag"], contactability_source,
    )
    return {
        "verdict_id": verdict_id,
        "grade": grade,
        "routed_channel": routed_channel,
        "contactability_flag": result["contactability_flag"],
        "contributing_factors": contributing_factors,
    }


def record_sub_grade(session: Session, prospect_id: str, *, reason: str, actor: str = SOURCE_COMPONENT) -> Optional[dict]:
    """Write a sub_grade / recycle_suppress verdict for an unreachable lead.

    Used for enrichment.failed (exhausted) prospects, which are suppressed
    regardless of CDS score (spec §5/§130). No CDS lookup required.
    """
    prospect = get_prospect(session, prospect_id)
    if prospect is None:
        logger.warning("[TruthEngine] prospect not found prospect_id=%s", prospect_id)
        return None

    routed_channel = primary_channel("sub_grade")
    contributing_factors = {
        "reason": reason,
        "contactability_state": prospect.get("contactability_state"),
        "routed_channels": ["recycle_suppress"],
    }
    verdict_id = _write_verdict(
        session,
        prospect_id=prospect_id,
        grade="sub_grade",
        routed_channel=routed_channel,
        contactability_flag=False,
        contributing_factors=contributing_factors,
        actor=actor,
    )
    logger.info("[TruthEngine] sub_grade prospect_id=%s reason=%s", prospect_id, reason)
    return {"verdict_id": verdict_id, "grade": "sub_grade", "routed_channel": routed_channel}
