"""
Storm signal tagger — converts an NWS alert into storm_damage Incident rows
for downstream CDS rescoring.

storm_damage is a stacking-only signal in the CDS engine: tagging a property
boosts its existing distress score via the stacking bonus, but cannot make a
non-distressed property a lead on its own. We therefore only tag properties
that already cross a minimum distress threshold to keep rescore cost bounded
and pack quality high.

Called from nws_webhook.process_alert() after _activate_storm_packs(). Does
not commit — the caller owns the transaction.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, and_, func
from sqlalchemy.orm import Session

from src.core.models import Incident, Property, DistressScore

logger = logging.getLogger(__name__)

DEFAULT_MIN_SCORE = 40.0  # Silver tier floor


def tag_affected_properties(
    affected_zips: list[str],
    alert_id: str,
    effective_dt: Optional[datetime],
    db: Session,
    min_score: float = DEFAULT_MIN_SCORE,
) -> list[int]:
    """Insert storm_damage Incident rows for distressed properties in affected ZIPs.

    Idempotent: an existing storm_damage incident for the same property whose
    crime_types JSONB carries this alert_id will not be re-inserted.

    Args:
        affected_zips: ZIP codes from the NWS alert (e.g. ["33602", "33647"]).
        alert_id: NWS @id, stored in Incident.crime_types for dedup.
        effective_dt: alert effective time; falls back to now() if None.
        db: open Session — caller commits.
        min_score: latest final_cds_score floor (inclusive).

    Returns:
        List of property IDs newly tagged this call (pre-existing matches
        excluded).
    """
    if not affected_zips:
        logger.info("[storm_tagger] alert=%s: no affected_zips, skipping", alert_id[:40])
        return []

    incident_date = (effective_dt or datetime.now(timezone.utc)).date()

    latest_score_subq = (
        select(
            DistressScore.property_id.label("pid"),
            func.max(DistressScore.score_date).label("max_date"),
        )
        .group_by(DistressScore.property_id)
        .subquery()
    )

    candidates_q = (
        select(Property.id)
        .join(latest_score_subq, latest_score_subq.c.pid == Property.id)
        .join(
            DistressScore,
            and_(
                DistressScore.property_id == Property.id,
                DistressScore.score_date == latest_score_subq.c.max_date,
            ),
        )
        .where(
            Property.zip.in_(affected_zips),
            DistressScore.final_cds_score >= min_score,
        )
    )
    candidate_ids = {row[0] for row in db.execute(candidates_q).all()}

    if not candidate_ids:
        logger.info(
            "[storm_tagger] alert=%s: no properties >= %.1f in %d ZIPs",
            alert_id[:40], min_score, len(affected_zips),
        )
        return []

    existing_q = select(Incident.property_id).where(
        Incident.property_id.in_(candidate_ids),
        Incident.incident_type == "storm_damage",
        Incident.crime_types["alert_id"].astext == alert_id,
    )
    existing_ids = {row[0] for row in db.execute(existing_q).all()}

    new_ids = sorted(candidate_ids - existing_ids)
    if not new_ids:
        logger.info(
            "[storm_tagger] alert=%s: all %d candidates already tagged",
            alert_id[:40], len(candidate_ids),
        )
        return []

    metadata = {"alert_id": alert_id, "source": "nws"}
    db.add_all([
        Incident(
            property_id=pid,
            incident_type="storm_damage",
            incident_date=incident_date,
            crime_types=metadata,
        )
        for pid in new_ids
    ])
    db.flush()

    logger.info(
        "[storm_tagger] alert=%s: tagged %d properties (skipped %d already-tagged) across %d ZIPs",
        alert_id[:40], len(new_ids), len(existing_ids), len(affected_zips),
    )
    return new_ids
