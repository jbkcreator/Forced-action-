"""
Prospect lifecycle: creation, dedup, merge, and canonical read.

All prospect mutations are exclusive to this module — no other component
creates or merges prospect records.
"""
from typing import Optional

from sqlalchemy import text as sa_text

from src.core.models import EnrichedContact
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_or_create_prospect(session, property_id: int) -> str:
    """Return prospect_id (UUID str) for property_id, creating if absent."""
    row = session.execute(sa_text("""
        INSERT INTO prospects (property_id)
        VALUES (:pid)
        ON CONFLICT (property_id) DO NOTHING
        RETURNING prospect_id
    """), {"pid": property_id}).fetchone()

    if not row:
        row = session.execute(sa_text(
            "SELECT prospect_id FROM prospects WHERE property_id = :pid"
        ), {"pid": property_id}).fetchone()

    return str(row.prospect_id)


def best_ec(session, property_id: int) -> Optional[EnrichedContact]:
    """Latest successful enriched_contact row for a property."""
    return (
        session.query(EnrichedContact)
        .filter_by(property_id=property_id, match_success=True)
        .filter(EnrichedContact.superseded_at.is_(None))
        .order_by(EnrichedContact.enriched_at.desc())
        .first()
    )


def get_prospect(session, prospect_id: str) -> Optional[dict]:
    """
    Canonical read: prospect + property address + owner name + best enriched contact.
    Always resolves through merge chain — returns the surviving record.
    Returns None if prospect_id does not exist.
    """
    row = session.execute(sa_text("""
        WITH RECURSIVE resolved AS (
            SELECT prospect_id, property_id, contactability_state,
                   channel_consent, contact_attempts, successful_contacts,
                   contactability_rate, cohort_key, last_touch_at,
                   merged_into_id, created_at
            FROM prospects
            WHERE prospect_id = CAST(:pid AS uuid)
            UNION ALL
            SELECT p.prospect_id, p.property_id, p.contactability_state,
                   p.channel_consent, p.contact_attempts, p.successful_contacts,
                   p.contactability_rate, p.cohort_key, p.last_touch_at,
                   p.merged_into_id, p.created_at
            FROM prospects p
            INNER JOIN resolved r ON p.prospect_id = r.merged_into_id
        )
        SELECT
            r.prospect_id,
            r.property_id,
            r.contactability_state,
            r.channel_consent,
            r.contact_attempts,
            r.successful_contacts,
            r.contactability_rate,
            r.cohort_key,
            r.last_touch_at,
            r.created_at,
            prop.address          AS property_address,
            prop.city             AS property_city,
            prop.state            AS property_state,
            prop.zip              AS property_zip,
            prop.parcel_id,
            o.owner_name,
            o.phone_1             AS owner_phone,
            o.email_1             AS owner_email,
            ec.id                 AS enriched_contact_id,
            ec.mobile_phone       AS ec_mobile,
            ec.email              AS ec_email,
            ec.source             AS ec_source,
            ec.confidence         AS ec_confidence,
            ec.enriched_at        AS ec_enriched_at
        FROM resolved r
        JOIN properties prop ON prop.id = r.property_id
        LEFT JOIN owners o ON o.property_id = r.property_id
        LEFT JOIN enriched_contacts ec ON ec.property_id = r.property_id
            AND ec.match_success = TRUE
            AND ec.superseded_at IS NULL
        WHERE r.merged_into_id IS NULL
        ORDER BY ec.enriched_at DESC NULLS LAST
        LIMIT 1
    """), {"pid": prospect_id}).fetchone()

    if not row:
        return None

    return {
        "prospect_id":         str(row.prospect_id),
        "property_id":         row.property_id,
        "contactability_state": row.contactability_state,
        "channel_consent":     row.channel_consent,
        "contact_attempts":    row.contact_attempts,
        "successful_contacts": row.successful_contacts,
        "contactability_rate": float(row.contactability_rate) if row.contactability_rate else None,
        "cohort_key":          row.cohort_key,
        "last_touch_at":       row.last_touch_at,
        "created_at":          row.created_at,
        "property": {
            "address": row.property_address,
            "city":    row.property_city,
            "state":   row.property_state,
            "zip":     row.property_zip,
            "parcel_id": row.parcel_id,
        },
        "owner": {
            "name":  row.owner_name,
            "phone": row.owner_phone,
            "email": row.owner_email,
        },
        "enriched_contact": {
            "id":          row.enriched_contact_id,
            "mobile":      row.ec_mobile,
            "email":       row.ec_email,
            "source":      row.ec_source,
            "confidence":  float(row.ec_confidence) if row.ec_confidence else None,
            "enriched_at": row.ec_enriched_at,
        } if row.enriched_contact_id else None,
    }


def merge_prospects(
    session,
    surviving_id: str,
    merged_id: str,
    field_decisions: dict,
) -> None:
    """
    Mark merged_id as absorbed into surviving_id.
    Writes merge_events record; does NOT re-point existing events (deferred to M10).
    """
    session.execute(sa_text("""
        UPDATE prospects
        SET merged_into_id = CAST(:surviving AS uuid),
            updated_at = NOW()
        WHERE prospect_id = CAST(:merged AS uuid)
    """), {"surviving": surviving_id, "merged": merged_id})

    session.execute(sa_text("""
        INSERT INTO merge_events (surviving_id, merged_id, field_decisions)
        VALUES (
            CAST(:surviving AS uuid),
            CAST(:merged AS uuid),
            CAST(:decisions AS jsonb)
        )
    """), {
        "surviving": surviving_id,
        "merged":    merged_id,
        "decisions": __import__("json").dumps(field_decisions),
    })

    logger.info(
        "[Prospects] merged prospect_id=%s into surviving=%s",
        merged_id, surviving_id,
    )


def dedupe_after_cascade(session, property_ids: list[int]) -> None:
    """
    Post-cascade mobile dedup: if two active prospects share a validated mobile,
    merge the newer one into the older (stable IDs survive).

    Sprint 1 scope: mobile match only. APN+name, address, voter ID deferred.
    """
    if not property_ids:
        return

    rows = session.execute(sa_text("""
        SELECT
            p.prospect_id,
            p.property_id,
            p.created_at,
            ec.mobile_phone
        FROM prospects p
        JOIN enriched_contacts ec ON ec.property_id = p.property_id
        WHERE p.property_id = ANY(:pids)
          AND p.merged_into_id IS NULL
          AND ec.match_success = TRUE
          AND ec.superseded_at IS NULL
          AND ec.mobile_phone IS NOT NULL
          AND ec.mobile_phone != ''
        ORDER BY ec.enriched_at DESC
    """), {"pids": property_ids}).fetchall()

    # Build mobile → earliest prospect map
    mobile_to_prospect: dict[str, tuple] = {}  # mobile → (prospect_id, created_at)
    merges: list[tuple[str, str]] = []          # (surviving_id, merged_id)

    for row in rows:
        mobile = row.mobile_phone.strip()
        pid    = str(row.prospect_id)
        if mobile not in mobile_to_prospect:
            mobile_to_prospect[mobile] = (pid, row.created_at)
        else:
            surviving_pid, surviving_created = mobile_to_prospect[mobile]
            # Older created_at wins (stable IDs)
            if row.created_at < surviving_created:
                merges.append((pid, surviving_pid))
                mobile_to_prospect[mobile] = (pid, row.created_at)
            else:
                merges.append((surviving_pid, pid))

    for surviving_id, merged_id in merges:
        merge_prospects(
            session,
            surviving_id=surviving_id,
            merged_id=merged_id,
            field_decisions={"reason": "mobile_dedup", "mobile_match": True},
        )

    if merges:
        logger.info("[Prospects] dedupe_after_cascade merged %d duplicate(s)", len(merges))
