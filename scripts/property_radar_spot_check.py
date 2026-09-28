"""Read-only spot-check of PropertyRadar -> FA property links.

Samples linked staging records in loaded counties and prints them beside the
FA property they point to, so a reviewer can eyeball APN/address agreement.
Also reports link coverage per county. Writes nothing.

Usage:
    PYTHONPATH=. python scripts/property_radar_spot_check.py [--limit 20]
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import create_engine, text

from config.property_radar import COUNTY_FIPS_TO_SLUG
from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_COVERAGE_SQL = """
    SELECT county_fips, COUNT(*) AS total, COUNT(property_id) AS linked
    FROM property_radar_records
    WHERE county_fips = ANY(:fips)
    GROUP BY county_fips
    ORDER BY county_fips
"""

_SAMPLE_SQL = """
    SELECT r.radar_id, r.county_fips, r.apn, r.property_address, r.zip,
           r.match_method, r.match_confidence,
           p.id AS property_id, p.parcel_id, p.address AS fa_address, p.zip AS fa_zip, p.county_id
    FROM property_radar_records r
    JOIN properties p ON p.id = r.property_id
    WHERE r.county_fips = ANY(:fips)
    ORDER BY random()
    LIMIT :limit
"""


def main(limit: int) -> None:
    fips = list(COUNTY_FIPS_TO_SLUG)
    engine = create_engine(str(get_settings().database_url))
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        for row in conn.execute(text(_COVERAGE_SQL), {"fips": fips}).mappings():
            logger.info("%s (%s): %d/%d linked", COUNTY_FIPS_TO_SLUG[row["county_fips"]],
                        row["county_fips"], row["linked"], row["total"])
        samples = conn.execute(text(_SAMPLE_SQL), {"fips": fips, "limit": limit}).mappings().all()

    if not samples:
        logger.info("No linked records to sample.")
        return
    for s in samples:
        county_ok = s["county_id"] == COUNTY_FIPS_TO_SLUG[s["county_fips"]]
        logger.info(
            "%s  %s/%s  APN=%s | parcel=%s  PR=%s %s | FA=%s %s  county_ok=%s",
            s["radar_id"], s["match_method"], s["match_confidence"], s["apn"], s["parcel_id"],
            s["property_address"], s["zip"], s["fa_address"], s["fa_zip"], county_ok,
        )
    logger.info("Sampled %d linked records.", len(samples))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    main(parser.parse_args().limit)
