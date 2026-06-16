"""
geo_interest — Subscriber county and ZIP interest resolver.

Provides both forward lookups (what ZIPs/counties does subscriber X care about?)
and reverse lookups (which subscribers care about ZIP/county Y?).

Interest signals ranked by confidence:
  high    — active/former ZIP territory lock or waitlist signup
  medium  — recent lead engagement (sent_leads in last 30 days)
  low     — wallet spend tagged to a ZIP (wallet_transactions.zip_code)

All queries use sa.text() per project conventions.
"""

import logging
from typing import TypedDict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ZipInterestMap(TypedDict):
    locked: list[str]
    waitlist: list[str]
    engagement: list[str]
    wallet: list[str]


def get_subscriber_zips_of_interest(subscriber_id: int, db: Session) -> ZipInterestMap:
    # A single query that aggregates all categories into JSON arrays
    query = text("""
        WITH 
        -- 1. Fetch locked/grace ZIPs
        z_locked AS (
            SELECT DISTINCT zip_code 
            FROM zip_territories 
            WHERE subscriber_id = :sid AND status IN ('locked', 'grace')
        ),
        -- 2. Fetch waitlist ZIPs
        z_waitlist AS (
            SELECT DISTINCT we.zip_code 
            FROM waitlist_entries we
            JOIN subscribers s ON s.email = we.email
            WHERE s.id = :sid AND we.status IN ('waiting', 'notified', 'converted')
        ),
        -- 3. Fetch engagement ZIPs
        z_engagement AS (
            SELECT DISTINCT p.zip AS zip_code
            FROM sent_leads sl
            JOIN properties p ON p.id = sl.property_id
            WHERE sl.subscriber_id = :sid 
              AND sl.sent_at >= NOW() - INTERVAL '30 days'
              AND p.zip IS NOT NULL
        ),
        -- 4. Fetch wallet ZIPs
        z_wallet AS (
            SELECT DISTINCT zip_code 
            FROM wallet_transactions 
            WHERE subscriber_id = :sid AND zip_code IS NOT NULL AND txn_type = 'debit'
        )
        
        -- Combine them all into a single row of arrays
        SELECT 
            COALESCE((SELECT jsonb_agg(zip_code) FROM z_locked), '[]'::jsonb) AS locked,
            COALESCE((SELECT jsonb_agg(zip_code) FROM z_waitlist), '[]'::jsonb) AS waitlist,
            COALESCE((SELECT jsonb_agg(zip_code) FROM z_engagement), '[]'::jsonb) AS engagement,
            COALESCE((SELECT jsonb_agg(zip_code) FROM z_wallet), '[]'::jsonb) AS wallet;
    """)

    # Execute and fetch the single result row
    row = db.execute(query, {"sid": subscriber_id}).mappings().first()

    if not row:
        return ZipInterestMap(locked=[], waitlist=[], engagement=[], wallet=[])

    # row elements will automatically be parsed into Python lists by SQLAlchemy/Psycopg
    return ZipInterestMap(
        locked=row["locked"],
        waitlist=row["waitlist"],
        engagement=row["engagement"],
        wallet=row["wallet"]
    )


def get_subscriber_counties_of_interest(subscriber_id: int, db: Session) -> list[str]:
    """Returns distinct counties the subscriber has shown interest in, across all sources."""
    rows = db.execute(text("""
        SELECT county_id FROM subscribers WHERE id = :sid
        UNION
        SELECT DISTINCT zt.county_id
          FROM zip_territories zt
         WHERE zt.subscriber_id = :sid
        UNION
        SELECT DISTINCT we.county_id
          FROM waitlist_entries we
          JOIN subscribers s ON s.email = we.email
         WHERE s.id = :sid
           AND we.status IN ('waiting', 'notified', 'converted')
    """), {"sid": subscriber_id}).scalars().all()
    return [r for r in rows if r]


def get_subscribers_interested_in_county(county_id: str, db: Session) -> list[int]:
    """
    Reverse lookup: subscriber IDs with any geo-interest in the given county.
    Used by County-Live reactivation to build candidate cohorts.
    Sources: subscribers.county_id, zip_territories, waitlist_entries.
    """
    rows = db.execute(text("""
        SELECT DISTINCT s.id
          FROM subscribers s
         WHERE s.county_id = :county
        UNION
        SELECT DISTINCT zt.subscriber_id
          FROM zip_territories zt
         WHERE zt.county_id = :county
           AND zt.subscriber_id IS NOT NULL
        UNION
        SELECT DISTINCT sub.id
          FROM waitlist_entries we
          JOIN subscribers sub ON sub.email = we.email
         WHERE we.county_id = :county
           AND we.status IN ('waiting', 'notified', 'converted')
    """), {"county": county_id}).scalars().all()
    return list(rows)


def get_subscribers_interested_in_zip(
    zip_code: str,
    vertical: str,
    county_id: str,
    db: Session,
) -> list[int]:
    """
    Reverse lookup: subscriber IDs with any geo-interest in the given ZIP+vertical.
    Used by Sold-Out reactivation to build candidate cohorts.
    Sources: zip_territories (current/former lock), waitlist_entries, sent_leads (90 days).
    """
    rows = db.execute(text("""
        SELECT DISTINCT zt.subscriber_id
          FROM zip_territories zt
         WHERE zt.zip_code    = :zip
           AND zt.vertical    = :vertical
           AND zt.county_id   = :county
           AND zt.subscriber_id IS NOT NULL
        UNION
        SELECT DISTINCT sub.id
          FROM waitlist_entries we
          JOIN subscribers sub ON sub.email = we.email
         WHERE we.zip_code  = :zip
           AND we.vertical  = :vertical
           AND we.county_id = :county
           AND we.status IN ('waiting', 'notified', 'converted')
        UNION
        SELECT DISTINCT sl.subscriber_id
          FROM sent_leads sl
          JOIN properties p ON p.id = sl.property_id
         WHERE p.zip       = :zip
           AND p.county_id = :county
           AND sl.sent_at  >= NOW() - INTERVAL '90 days'
    """), {"zip": zip_code, "vertical": vertical, "county": county_id}).scalars().all()
    return list(rows)
