"""
Clay enrichment integration for white-label contractor data (Stage 12 / fa056).

Fetches contractor/company data from Clay for a given county + vertical.
Results are cached in white_label_contractor_enrichments with a configurable TTL
(default 7 days, configurable via CLAY_ENRICHMENT_TTL_DAYS).

Clay API reference: https://api.clay.com/v1/
The platform uses Clay's "Table Runs" endpoint to fetch enriched company data.
Each request returns a list of contractor objects with fields:
  name, website, phone, email, headcount, revenue_range, locations, linkedin_url, etc.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from sqlalchemy import text as sa_text

from config.settings import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# Clay API client
# ---------------------------------------------------------------------------

def _clay_headers() -> dict:
    s = get_settings()
    if not s.clay_api_key:
        raise RuntimeError("CLAY_API_KEY not configured")
    return {
        "Authorization": f"Bearer {s.clay_api_key.get_secret_value()}",
        "Content-Type": "application/json",
    }


def enrich_contractors(county_id: str, vertical: str, limit: int = 50) -> list[dict]:
    """
    Call Clay API to enrich contractor/company data for a county + vertical.
    Returns a list of contractor dicts.
    Falls back to empty list on API error (so callers always get a usable result).
    """
    s = get_settings()
    base = s.clay_api_base

    # Build a query: search for businesses in county matching the vertical keyword
    vertical_keywords = {
        "roofing":       "roofing contractor",
        "restoration":   "water damage restoration",
        "wholesalers":   "real estate investor",
        "fix_flip":      "house flipper investor",
        "attorneys":     "real estate attorney",
        "public_adjusters": "public adjuster insurance",
    }
    keyword = vertical_keywords.get(vertical, vertical.replace("_", " "))

    # Map county_id to a geographic location string
    county_locations = {
        "hillsborough": "Hillsborough County FL",
        "pinellas":     "Pinellas County FL",
        "pasco":        "Pasco County FL",
        "polk":         "Polk County FL",
        "manatee":      "Manatee County FL",
    }
    location = county_locations.get(county_id, f"{county_id.title()} FL")

    try:
        response = requests.post(
            f"{base}/sources/enrichment",
            headers=_clay_headers(),
            json={
                "query": keyword,
                "location": location,
                "limit": limit,
                "fields": [
                    "name", "website", "phone", "email",
                    "headcount", "revenue_range", "locations",
                    "linkedin_url", "description",
                ],
            },
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        contractors = data.get("results", data.get("companies", []))
        logger.info(
            "[clay] enriched %d contractors for %s/%s",
            len(contractors), county_id, vertical,
        )
        return contractors
    except requests.RequestException as exc:
        logger.warning("[clay] enrichment API call failed for %s/%s: %s", county_id, vertical, exc)
        return []
    except Exception as exc:
        logger.error("[clay] unexpected error for %s/%s: %s", county_id, vertical, exc, exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Cache layer
# ---------------------------------------------------------------------------

def get_or_refresh_enrichment(
    client_id: int,
    county_id: str,
    vertical: str,
    db,
    force_refresh: bool = False,
) -> list[dict]:
    """
    Return cached Clay data if fresh (within TTL), otherwise call Clay and update cache.
    Always returns a list (empty on failure).
    """
    s = get_settings()
    ttl_days = s.clay_enrichment_ttl_days
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)

    existing = db.execute(
        sa_text("""
            SELECT id, data, enriched_at
              FROM white_label_contractor_enrichments
             WHERE client_id = :cid AND county_id = :county AND vertical = :vert
        """),
        {"cid": client_id, "county": county_id, "vert": vertical},
    ).fetchone()

    if (
        not force_refresh
        and existing
        and existing.enriched_at
        and existing.enriched_at > stale_cutoff
        and existing.data
    ):
        logger.debug("[clay] serving cached enrichment for client %d %s/%s", client_id, county_id, vertical)
        return existing.data or []

    # Fetch fresh data from Clay
    fresh_data = enrich_contractors(county_id, vertical)

    if existing:
        db.execute(
            sa_text("""
                UPDATE white_label_contractor_enrichments
                   SET data = :data, enriched_at = now()
                 WHERE id = :id
            """),
            {"data": fresh_data, "id": existing.id},
        )
    else:
        db.execute(
            sa_text("""
                INSERT INTO white_label_contractor_enrichments
                       (client_id, county_id, vertical, data, enriched_at, created_at)
                VALUES (:cid, :county, :vert, :data, now(), now())
                ON CONFLICT (client_id, county_id, vertical)
                DO UPDATE SET data = EXCLUDED.data, enriched_at = EXCLUDED.enriched_at
            """),
            {
                "cid": client_id,
                "county": county_id,
                "vert": vertical,
                "data": fresh_data,
            },
        )
    db.commit()
    return fresh_data
