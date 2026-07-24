"""
Hero deal service — T-B12-01.

Powers the unauthenticated landing-page hero: a visitor types their ZIP and
sees ONE real, blurred, scored deal. No IP-geo (rejected in T-B12-R1) — the
ZIP always comes from the visitor's own input.

Fallback ladder (per T-B12-01 resolution):
  1. Exact ZIP match — top-scored qualified lead in that ZIP.
  2. Nearest fallback — top-scored qualified lead statewide, honestly
     labeled with its county (e.g. "Nearest to you: Hillsborough").
  3. Empty — no qualified leads anywhere; caller renders waitlist capture.

No PII: address is blurred, phone/owner/email are never selected.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.scoring import is_hot_score
from src.api.deps import visible_tier_fields
from src.core.models import DistressScore, Property
from src.services.proof_moment import _blur_address
from src.utils.county_config import get_county

logger = logging.getLogger(__name__)


def _serialize_deal(prop: Property, score: DistressScore, vertical: str) -> dict:
    v_score = score.vertical_scores.get(vertical) if score.vertical_scores else None
    dt = score.distress_types
    if isinstance(dt, dict):
        distress = list(dt.keys())
    elif isinstance(dt, list):
        distress = list(dt)
    else:
        distress = []

    lead_tier, _urgency = visible_tier_fields(prop, score)

    return {
        "property_id": prop.id,
        "address_masked": _blur_address(prop.address),
        "city": prop.city,
        "state": prop.state,
        "zip": prop.zip,
        "county_id": prop.county_id,
        "score": float(score.final_cds_score or 0),
        "vertical_score": float(v_score) if v_score is not None else None,
        "lead_tier": lead_tier,
        "is_hot": is_hot_score(score.final_cds_score),
        "distress_types": distress,
        "unlocked": False,
    }


def _query_top_lead(
    db: Session, vertical: str, *, zip_code: Optional[str] = None
) -> Optional[tuple]:
    """Top-scored qualified, non-guess lead for `vertical`, optionally scoped to a ZIP.

    Returns (Property, DistressScore) ORM instances so `_serialize_deal` can read
    their relationships unchanged; the *lookup* itself is raw SQL per project rule
    (CLAUDE.md "Database and SQLAlchemy" — text() for retrieval, ORM only for
    add/delete). We select just the two primary keys via text(), then load the
    full instances with session.get(), which is a cheap identity-map fetch.
    """
    zip_clause = " AND p.zip = :zip_code" if zip_code else ""
    params: dict = {"vertical": vertical}
    if zip_code:
        params["zip_code"] = zip_code

    row = db.execute(
        sa_text(
            f"""
            SELECT p.id AS property_id, ds.id AS score_id
              FROM properties p
              JOIN distress_scores ds ON ds.property_id = p.id
             WHERE ds.qualified = true
               AND ds.is_guess_lead = false
               AND (ds.vertical_scores ->> :vertical)::float > 0
               {zip_clause}
             ORDER BY (ds.vertical_scores ->> :vertical)::float DESC
             LIMIT 1
            """
        ),
        params,
    ).first()
    if not row:
        return None

    prop = db.get(Property, row.property_id)
    score = db.get(DistressScore, row.score_id)
    if not prop or not score:
        return None
    return prop, score


def get_hero_deal(zip_code: str, vertical: str, db: Session) -> dict:
    """
    Returns the hero payload for a visitor-entered ZIP.

    Shape:
        {"status": "zip_match" | "nearest" | "empty",
         "zip_code": str, "vertical": str,
         "deal": {...} | None,
         "nearest_label": str | None,
         "message": str | None}
    """
    row = _query_top_lead(db, vertical, zip_code=zip_code)
    if row:
        prop, score = row
        return {
            "status": "zip_match",
            "zip_code": zip_code,
            "vertical": vertical,
            "deal": _serialize_deal(prop, score, vertical),
            "nearest_label": None,
            "message": None,
        }

    row = _query_top_lead(db, vertical)
    if row:
        prop, score = row
        try:
            county_name = get_county(prop.county_id).get("display_name") or prop.county_id
        except Exception:
            logger.warning("[HeroDeal] county lookup failed for %s", prop.county_id, exc_info=True)
            county_name = prop.county_id
        return {
            "status": "nearest",
            "zip_code": zip_code,
            "vertical": vertical,
            "deal": _serialize_deal(prop, score, vertical),
            "nearest_label": f"Nearest to you: {county_name}",
            "message": None,
        }

    return {
        "status": "empty",
        "zip_code": zip_code,
        "vertical": vertical,
        "deal": None,
        "nearest_label": None,
        "message": f"No live deals in {zip_code} yet — get notified when they arrive.",
    }
