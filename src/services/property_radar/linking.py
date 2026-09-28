"""PropertyRadar → FA property linker.

Walks unlinked property_radar_records for loaded counties and sets property_id
via the existing parcel/address cascade (stages 1–2 only — no owner-name
fuzzy match to avoid false positives with LLCs that own many properties).

Guard: the matched FA property's county_id must equal the county slug for the
record's county_fips. A cross-county parcel collision is discarded.

Run after upsert_records(), either inline or as a separate scheduled pass.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.property_radar import COUNTY_FIPS_TO_SLUG

logger = logging.getLogger(__name__)

# ── minimal concrete loader ──────────────────────────────────────────────────
# BaseLoader is abstract (load_from_dataframe). We need a thin subclass to
# access find_property_cascade without duplicating the matching waterfall.

def _make_loader(session: Session, county_id: str):
    from src.loaders.base import BaseLoader
    import pandas as pd

    class _RadarLinker(BaseLoader):
        def load_from_dataframe(self, df: "pd.DataFrame", skip_duplicates: bool = True):  # type: ignore[override]
            raise NotImplementedError

    return _RadarLinker(session, county_id=county_id)


# ── SQL ─────────────────────────────────────────────────────────────────────

_FETCH_UNLINKED_SQL = """
    SELECT id, state_fips, county_fips, apn, property_address, city, zip
    FROM property_radar_records
    WHERE property_id IS NULL
      AND county_fips = ANY(:fips_list)
    ORDER BY id
    LIMIT :batch_size
    OFFSET :offset
"""

_SET_LINK_SQL = """
    UPDATE property_radar_records
    SET property_id      = :property_id,
        match_method     = :match_method,
        match_confidence = :match_confidence
    WHERE id = :id
"""


# ── public API ───────────────────────────────────────────────────────────────

def link_unlinked(session: Session, *, batch_size: int = 500) -> dict[str, int]:
    """Link unlinked staging records to FA properties for loaded counties.

    Processes in pages of `batch_size`. Returns a summary dict with keys
    linked, skipped_county_mismatch, no_match.
    """
    loaded_fips = list(COUNTY_FIPS_TO_SLUG.keys())
    if not loaded_fips:
        return {"linked": 0, "skipped_county_mismatch": 0, "no_match": 0}

    linked = no_match = mismatch = 0
    offset = 0

    while True:
        rows = session.execute(
            text(_FETCH_UNLINKED_SQL),
            {"fips_list": loaded_fips, "batch_size": batch_size, "offset": offset},
        ).mappings().all()

        if not rows:
            break

        for row in rows:
            result = _try_link(session, dict(row))
            if result == "linked":
                linked += 1
            elif result == "mismatch":
                mismatch += 1
            else:
                no_match += 1

        offset += batch_size
        if len(rows) < batch_size:
            break

    session.flush()
    logger.info(
        "PropertyRadar link_unlinked: linked=%d no_match=%d county_mismatch=%d",
        linked, no_match, mismatch,
    )
    return {"linked": linked, "skipped_county_mismatch": mismatch, "no_match": no_match}


def _try_link(session: Session, row: dict[str, Any]) -> str:
    """Attempt one record. Returns 'linked', 'mismatch', or 'no_match'."""
    county_fips: str = row["county_fips"]
    expected_slug = COUNTY_FIPS_TO_SLUG.get(county_fips)
    if not expected_slug:
        return "no_match"

    loader = _make_loader(session, county_id=expected_slug)

    prop, method, confidence = loader.find_property_cascade(
        parcel_id=row.get("apn"),
        address=row.get("property_address"),
        zip_code=row.get("zip"),
        city=row.get("city"),
        # No owner_name — avoids LLC cross-property false positives (Q2)
    )

    if prop is None:
        return "no_match"

    # County guard: discard cross-county parcel collision (Q3)
    if (prop.county_id or "").lower() != expected_slug.lower():
        logger.debug(
            "County mismatch for record id=%s: expected %s got %s",
            row["id"], expected_slug, prop.county_id,
        )
        return "mismatch"

    session.execute(
        text(_SET_LINK_SQL),
        {
            "id": row["id"],
            "property_id": prop.id,
            "match_method": method,
            "match_confidence": confidence,
        },
    )
    return "linked"
