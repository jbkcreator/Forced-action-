"""Stage F — WP-T2-8: seed Pasco County config rows (best-effort, Q1 GRILL-DECISIONS.md).

Inserts:
  counties       — pasco county row (is_active=True, same venture as hillsborough)
  county_sources — pasco permits source pointing to the Pasco Accela portal

Pasco is best-effort: the heartbeat SLA is deliberately lenient (stale-feed
alert to EXCEPTIONS, never blocks the builder engine). The playwright_code is
left NULL — a Pasco scraper has not been built yet; the row marks the county
as configured so the permit engine can pick it up once a scraper lands.

Idempotent — ON CONFLICT DO NOTHING throughout.

Usage:
    PYTHONPATH=. python migrations/apply_builder_pasco_county_config.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Pasco Accela portal (New West FL counties portal):
# https://aca-prod.accela.com/PASCO/Cap/CapHome.aspx?module=Building
_PASCO_PORTAL_URL = "https://aca-prod.accela.com/PASCO/Cap/CapHome.aspx?module=Building"

DDL = [
    # counties row
    """
    INSERT INTO counties (
        county_id, display_name, fips, nws_zone,
        parcel_id_format, bankruptcy_division,
        venture_key, is_active
    ) VALUES (
        'pasco', 'Pasco County', '12101', 'FLZ142',
        'folio', '8',
        'hillsborough_distress', TRUE
    )
    ON CONFLICT (county_id) DO NOTHING;
    """,

    # county_sources row — scrape_mode intentionally NULL until scraper built
    """
    INSERT INTO county_sources (
        county_id, signal_type, source_name, url, description,
        navigation_hint, output_format, date_range_available,
        frequency, is_active, special_flags, scrape_mode
    ) VALUES (
        'pasco', 'permits',
        'Pasco Accela Building',
        :portal_url,
        'Pasco County building permit portal via Accela. Scraper not yet built — best-effort (WP-T2-8 Stage F).',
        'Navigate to Building module, search by date range, download CSV.',
        'csv', TRUE, 'daily', FALSE,
        '{"builder_engine": true, "best_effort": true}',
        'playwright_then_ai'
    )
    ON CONFLICT DO NOTHING;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt), {"portal_url": _PASCO_PORTAL_URL})

    logger.info("apply_builder_pasco_county_config complete.")


if __name__ == "__main__":
    main()
