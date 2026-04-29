"""
Proof Moment service — Item 24.

Returns 1 fully-revealed lead + 2 blurred leads for new signup proof moment.
Goal: show real value within 30 seconds of account creation.
No auth required for the endpoint — leads are scored/qualified properties.
"""
import logging
from typing import Optional

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from src.core.models import DistressScore, EnrichedContact, Owner, Property, SentLead, Subscriber

logger = logging.getLogger(__name__)


def get_proof_leads(
    vertical: str,
    county_id: str,
    db: Session,
    feed_uuid: Optional[str] = None,
) -> dict:
    """
    Return proof moment payload:
      - 'revealed': 1 fully enriched lead (address, score, contact if available)
      - 'blurred':  2 blurred leads (partial address, score visible, contact hidden)

    Uses the CDS vertical score for ranking. Falls back to final_cds_score if
    the vertical column is not present in the JSONB.

    When feed_uuid is supplied, any blurred lead whose property has a SentLead
    row for the resolved subscriber (e.g. via the $4 unlock) is upgraded to a
    fully-enriched form with `unlocked: True`. Without feed_uuid the behavior
    is unchanged.
    """
    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except (KeyError, TypeError):
        score_col = DistressScore.final_cds_score

    from config.settings import get_settings
    from src.utils.lead_filters import has_contact_filter, phone_priority_order
    contact_clause = has_contact_filter(get_settings())

    where_clauses = [
        Property.county_id == county_id,
        DistressScore.qualified == True,  # noqa: E712
    ]
    if contact_clause is not None:
        where_clauses.append(contact_clause)

    top = db.execute(
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(*where_clauses))
        .order_by(*phone_priority_order(score_col))
        .limit(3)
    ).all()

    if not top:
        return {"revealed": None, "blurred": [], "county_id": county_id, "vertical": vertical}

    # Resolve which of these top properties this subscriber has paid to unlock
    unlocked_ids: set = set()
    if feed_uuid:
        try:
            subscriber = db.execute(
                select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
            ).scalar_one_or_none()
            if subscriber is not None:
                top_ids = [prop.id for prop, _ in top]
                rows = db.execute(
                    select(SentLead.property_id).where(
                        SentLead.subscriber_id == subscriber.id,
                        SentLead.property_id.in_(top_ids),
                    )
                ).scalars().all()
                unlocked_ids = set(rows)
        except Exception as exc:  # noqa: BLE001 — non-fatal degradation
            logger.warning("proof_moment unlock lookup failed: %s", exc)

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

        # Address + owner name are public-record — return unblurred for every
        # lead. Phone/email (skip-trace enrichment) stay behind the paywall.
        owner_row = db.execute(
            select(Owner).where(Owner.property_id == prop.id).limit(1)
        ).scalar_one_or_none()
        lead["owner_name"] = (
            owner_row.owner_name if owner_row and getattr(owner_row, "owner_name", None)
            else None
        )

        is_paid_unlock = prop.id in unlocked_ids
        # The first (top-scored) lead is the FREE preview; others are blurred
        # unless this subscriber has paid to unlock them.
        reveal_contact = (i == 0) or is_paid_unlock

        if reveal_contact:
            enriched = db.execute(
                select(EnrichedContact).where(
                    EnrichedContact.property_id == prop.id,
                    EnrichedContact.match_success == True,  # noqa: E712
                ).limit(1)
            ).scalar_one_or_none()

            if enriched:
                lead["contact"] = {
                    "mobile_phone": enriched.mobile_phone,
                    "email": enriched.email,
                    "mailing_address": enriched.mailing_address,
                }
            elif owner_row and (owner_row.phone_1 or owner_row.email_1):
                # Fall back to Owner skip-trace columns when no EnrichedContact
                # match exists, so paid unlocks always show some contact data.
                lead["contact"] = {
                    "mobile_phone": owner_row.phone_1 or owner_row.phone_2 or owner_row.phone_3,
                    "email": owner_row.email_1 or owner_row.email_2,
                    "mailing_address": owner_row.mailing_address,
                }
            else:
                lead["contact"] = None
        else:
            lead["contact"] = None

        lead["unlocked"] = bool(is_paid_unlock)

        if i == 0:
            result["revealed"] = lead
        else:
            # Paid-unlock leads keep their blurred-slot ordering; the
            # `unlocked` flag tells the frontend to render the full-contact
            # Purchased card instead of the lock icon.
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
