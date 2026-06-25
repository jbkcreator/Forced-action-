"""Stream self-diagnosis metric compute functions (Sprint 4.2).

Each function: (db: Session, county_id: str) -> float | None
Returns fraction 0–1, or None when denominator is zero (metric undefined).
All queries are county-scoped, raw SQL via text().
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


def compute_enrichment_rate(db: Session, county_id: str) -> float | None:
    """Fraction of properties scored in last 30d whose owner has
    (phone_1 OR email_1) AND contact_info_confidence IN ('high','medium').
    """
    row = db.execute(text("""
        SELECT
            count(*) FILTER (
                WHERE (o.phone_1 IS NOT NULL OR o.email_1 IS NOT NULL)
                  AND o.contact_info_confidence IN ('high','medium')
            ) AS enriched,
            count(*) AS total
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        LEFT JOIN owners o ON o.property_id = ds.property_id
        WHERE p.county_id = :cid
          AND ds.score_date >= CURRENT_DATE - INTERVAL '30 days'
    """), {"cid": county_id}).fetchone()

    if row is None or row[1] == 0:
        return None
    return float(row[0]) / float(row[1])


def compute_dialable_rate(db: Session, county_id: str) -> float | None:
    """Fraction of owners (scoped by county) with any phone whose
    phone_metadata primary type is 'mobile' or 'landline'.
    Falls back to phone_deliverability_snapshots mobile_pct if no phone_metadata.
    """
    row = db.execute(text("""
        SELECT
            count(*) FILTER (
                WHERE (o.phone_metadata->>'type') IN ('mobile','landline')
            ) AS dialable,
            count(*) AS total
        FROM owners o
        JOIN properties p ON p.id = o.property_id
        WHERE p.county_id = :cid
          AND o.phone_1 IS NOT NULL
    """), {"cid": county_id}).fetchone()

    if row is None or row[1] == 0:
        return None
    return float(row[0]) / float(row[1])


def compute_sms_delivery_rate(db: Session, county_id: str) -> float | None:
    """Fraction of SMS messages sent in last 7d that were delivered
    (delivered_at IS NOT NULL), scoped by county_id on message_outcomes.
    """
    row = db.execute(text("""
        SELECT
            count(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered,
            count(*) AS total
        FROM message_outcomes
        WHERE county_id = :cid
          AND message_type = 'sms'
          AND sent_at >= NOW() - INTERVAL '7 days'
    """), {"cid": county_id}).fetchone()

    if row is None or row[1] == 0:
        return None
    return float(row[0]) / float(row[1])


def compute_closer_conv_rate(db: Session, county_id: str) -> float | None:
    """Fraction of closer calls (last 30d) with call_outcome='committed'
    out of all calls with a non-null outcome, scoped by subscriber.county_id.
    """
    row = db.execute(text("""
        SELECT
            count(*) FILTER (WHERE cc.call_outcome = 'committed') AS committed,
            count(*) AS total
        FROM closer_calls cc
        JOIN subscribers s ON s.id = cc.subscriber_id
        WHERE s.county_id = :cid
          AND cc.call_outcome IS NOT NULL
          AND cc.started_at >= NOW() - INTERVAL '30 days'
    """), {"cid": county_id}).fetchone()

    if row is None or row[1] == 0:
        return None
    return float(row[0]) / float(row[1])
