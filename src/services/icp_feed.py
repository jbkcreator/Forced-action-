"""
ICP feed queries — one per Expansion ICP Channel.

These queries define the lead set each channel will receive on launch.
They are NOT wired to any API route yet (config-only stage).
The feed is returned as a SQLAlchemy Query/select so callers can paginate,
count, or filter further without loading all rows.

REI Investor feed (v1, single-county):
  Properties in the Source County that either:
    (a) have a high investment-vertical CDS score (wholesalers OR fix_flip
        score >= threshold in the latest DistressScore record), OR
    (b) have at least one active Bankruptcy legal proceeding.
  This is the differentiator from the contractor lead product — Bankruptcy
  filing data is not surfaced in any contractor-product feed.
"""
from sqlalchemy import exists, or_, select, text as sa_text
from sqlalchemy.orm import Session

from src.core.models import DistressScore, LegalProceeding, Property

# Minimum investment-vertical score to qualify a property for the REI feed.
# Matches the Silver tier floor (40) from config/scoring.py so the feed
# contains actionable leads, not marginal noise.
REI_INVESTMENT_SCORE_THRESHOLD = 40.0

# LegalProceeding record_type for bankruptcy (verified in models.py:552).
_BANKRUPTCY = "Bankruptcy"


def rei_feed_query(db: Session, county_id: str):
    """
    Return a SQLAlchemy select() of Property rows for the REI Investor feed.

    Includes properties where:
      - latest DistressScore has wholesalers or fix_flip vertical score
        >= REI_INVESTMENT_SCORE_THRESHOLD, OR
      - there is at least one active Bankruptcy LegalProceeding.

    county_id: scope to this county (single-county feed per grilling Q8).
    """
    # Subquery: latest DistressScore per property in county
    latest_score_sq = (
        select(DistressScore.property_id)
        .where(
            DistressScore.county_id == county_id,
            or_(
                # Postgres JSONB operator via cast text — compatible with raw SQL
                # vertical_scores->>'wholesalers' cast to float >= threshold
                sa_text(
                    f"(distress_scores.vertical_scores->>'wholesalers')::float "
                    f">= {REI_INVESTMENT_SCORE_THRESHOLD}"
                ),
                sa_text(
                    f"(distress_scores.vertical_scores->>'fix_flip')::float "
                    f">= {REI_INVESTMENT_SCORE_THRESHOLD}"
                ),
            ),
        )
        .correlate(DistressScore)
    )

    # Subquery: active Bankruptcy proceeding for a property
    bankruptcy_sq = (
        select(LegalProceeding.property_id)
        .where(
            LegalProceeding.record_type == _BANKRUPTCY,
            LegalProceeding.county_id == county_id,
        )
    )

    return (
        select(Property)
        .where(
            Property.county_id == county_id,
            or_(
                Property.id.in_(latest_score_sq),
                Property.id.in_(bankruptcy_sq),
            ),
        )
    )
