"""
Proof Moment service — Item 24.

Returns 1 fully-revealed lead + 2 blurred leads for new signup proof moment.
Goal: show real value within 30 seconds of account creation.
No auth required for the endpoint — leads are scored/qualified properties.
"""
import logging
from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from src.core.models import DistressScore, EnrichedContact, Property

logger = logging.getLogger(__name__)


def get_proof_leads(vertical: str, county_id: str, db: Session) -> dict:
    """
    Return proof moment payload:
      - 'revealed': 1 fully enriched lead (address, score, contact if available)
      - 'blurred':  2 blurred leads (partial address, score visible, contact hidden)

    Uses the CDS vertical score for ranking. Falls back to final_cds_score if
    the vertical column is not present in the JSONB.
    """
    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except (KeyError, TypeError):
        score_col = DistressScore.final_cds_score

    top = db.execute(
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(
            Property.county_id == county_id,
            DistressScore.qualified == True,  # noqa: E712
        )
        .order_by(desc(score_col))
        .limit(3)
    ).all()

    if not top:
        return {"revealed": None, "blurred": [], "county_id": county_id, "vertical": vertical}

    result: dict = {"revealed": None, "blurred": [], "county_id": county_id, "vertical": vertical}

    for i, (prop, score) in enumerate(top):
        v_score = score.vertical_scores.get(vertical) if score.vertical_scores else None
        # distress_types is declared JSONB dict in models.py but production rows
        # sometimes carry a list (legacy scoring output). Tolerate both so the
        # proof moment endpoint doesn't crash on real data.
        dt = score.distress_types
        if isinstance(dt, dict):
            distress = list(dt.keys())
        elif isinstance(dt, list):
            distress = list(dt)
        else:
            distress = []

        lead: dict = {
            "property_id": prop.id,
            "address": prop.address,
            "city": prop.city,
            "state": prop.state,
            "zip": prop.zip,
            "score": float(score.final_cds_score or 0),
            "vertical_score": float(v_score) if v_score is not None else None,
            "lead_tier": score.lead_tier,
            "distress_types": distress,
            "urgency_level": score.urgency_level,
        }

        if i == 0:
            # Full reveal — fetch enriched contact if available
            enriched = db.execute(
                select(EnrichedContact).where(
                    EnrichedContact.property_id == prop.id,
                    EnrichedContact.match_success == True,  # noqa: E712
                ).limit(1)
            ).scalar_one_or_none()

            lead["contact"] = (
                {
                    "mobile_phone": enriched.mobile_phone,
                    "email": enriched.email,
                    "mailing_address": enriched.mailing_address,
                }
                if enriched
                else None
            )
            result["revealed"] = lead
        else:
            lead["address"] = _blur_address(prop.address)
            lead["contact"] = None
            result["blurred"].append(lead)

    return result


def _blur_address(address: Optional[str]) -> str:
    """Partially obscure a street address: '1234 *** **' to hide street name."""
    if not address:
        return "*** ***"
    parts = address.split()
    if not parts:
        return "*** ***"
    # Keep house number, mask street name and suffix
    masked = [parts[0]] + ["*" * max(3, len(p)) for p in parts[1:]]
    return " ".join(masked)
