"""Sprint 4.6 — Underwriting Reason-Code Feedback Service.

Receives broker decline reason codes, writes per-property audit records,
translates reason codes into CDS signal nudges (via A3 scoring_weight_overrides),
and immediately rescores the affected property with the updated weights.

Architecture:
  underwriting_feedback table  → per-property audit log (who declined, why)
  scoring_weight_overrides (A3) → global signal weight deltas (read by cds_engine)
  MultiVerticalScorer           → already consumes _weight_overrides from A3
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.scoring import UNDERWRITING_REASON_SIGNAL_NUDGES
from src.services.heuristic_loader import invalidate_cache

logger = logging.getLogger(__name__)

_DELTA_BOUNDS = (-15.0, 15.0)


class UnderwritingReasonCode(StrEnum):
    LTV_TOO_HIGH          = "ltv_too_high"
    STRUCTURAL_DAMAGE     = "structural_damage"
    COMMERCIAL_ZONING     = "commercial_zoning"
    TITLE_DEFECT          = "title_defect"
    FLOOD_ZONE            = "flood_zone"
    ENVIRONMENTAL_HAZARD  = "environmental_hazard"
    DEFERRED_MAINTENANCE  = "deferred_maintenance"
    UNPERMITTED_ADDITIONS = "unpermitted_additions"
    TENANT_OCCUPIED       = "tenant_occupied"
    MARKET_SATURATION     = "market_saturation"


VALID_REASON_CODES: frozenset[str] = frozenset(c.value for c in UnderwritingReasonCode)


def record_feedback(
    *,
    property_id: int,
    reason_code: str,
    submitted_by: str,
    db: Session,
    reason_detail: Optional[str] = None,
    lender_id: Optional[str] = None,
    loan_amount: Optional[float] = None,
) -> None:
    """Insert one underwriting_feedback row."""
    db.execute(
        sa_text("""
            INSERT INTO underwriting_feedback
                (property_id, reason_code, reason_detail, lender_id,
                 loan_amount, submitted_by, submitted_at)
            VALUES
                (:pid, :code, :detail, :lender, :amount, :by, :at)
        """),
        {
            "pid":    property_id,
            "code":   reason_code,
            "detail": reason_detail,
            "lender": lender_id,
            "amount": loan_amount,
            "by":     submitted_by,
            "at":     datetime.now(timezone.utc),
        },
    )


def apply_signal_nudges(reason_code: str, db: Session) -> list[dict]:
    """Upsert CDS signal nudges for a reason code into scoring_weight_overrides.

    Each nudge compounds with any existing delta (from seed/tuner/prior feedback),
    clamped to [-15, +15]. Returns a list of the applied nudge records for logging.
    """
    nudges = UNDERWRITING_REASON_SIGNAL_NUDGES.get(reason_code, [])
    if not nudges:
        logger.warning("[underwriting] no signal nudges configured for reason_code=%s", reason_code)
        return []

    applied: list[dict] = []
    for vertical, signal_type, delta in nudges:
        existing_row = db.execute(
            sa_text("""
                SELECT delta FROM scoring_weight_overrides
                WHERE vertical = :v AND signal_type = :s
            """),
            {"v": vertical, "s": signal_type},
        ).first()

        current = float(existing_row.delta) if existing_row else 0.0
        new_delta = max(_DELTA_BOUNDS[0], min(_DELTA_BOUNDS[1], current + delta))

        db.execute(
            sa_text("""
                INSERT INTO scoring_weight_overrides
                    (vertical, signal_type, delta, source, reason, enabled,
                     loss_sample_count, win_sample_count)
                VALUES
                    (:v, :s, :d, 'underwriting_feedback',
                     :reason, TRUE, 0, 0)
                ON CONFLICT (vertical, signal_type) DO UPDATE
                    SET delta   = EXCLUDED.delta,
                        source  = EXCLUDED.source,
                        reason  = EXCLUDED.reason,
                        enabled = TRUE,
                        updated_at = NOW()
            """),
            {
                "v":      vertical,
                "s":      signal_type,
                "d":      new_delta,
                "reason": f"Underwriting decline: {reason_code}",
            },
        )
        applied.append(
            {"vertical": vertical, "signal_type": signal_type, "delta": new_delta}
        )

    invalidate_cache()
    logger.info(
        "[underwriting] applied %d signal nudges for reason_code=%s",
        len(applied), reason_code,
    )
    return applied


def rescore_property(property_id: int, parcel_id: str, db: Session) -> dict:
    """Re-run CDS scoring for one property with freshly loaded weight overrides.

    Instantiates a new MultiVerticalScorer so it picks up the just-invalidated
    override cache, loads the property bundle via the engine's raw SQL path,
    scores all 6 verticals, persists the result, and returns the score dict.
    """
    from src.services.cds_engine import MultiVerticalScorer

    scorer = MultiVerticalScorer(db)

    prop_rows = scorer._fetch_properties_by_ids([property_id])
    if not prop_rows:
        raise ValueError(f"Property {parcel_id!r} (id={property_id}) not found for rescore")

    signal_map = scorer._fetch_signals_for_batch([property_id])
    bundle = scorer._build_property_bundle(prop_rows[0], signal_map)

    score_data = scorer.score_property(bundle)
    scorer.save_score_to_database(score_data)
    db.commit()

    logger.info(
        "[underwriting] rescored property %s → cds=%.2f tier=%s",
        parcel_id, score_data["final_cds_score"], score_data["lead_tier"],
    )
    return score_data
