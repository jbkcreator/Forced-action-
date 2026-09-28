"""PropertyRadar -> FA property linker.

Sets property_id on unlinked property_radar_records in loaded counties, via
BaseLoader.find_property_cascade stages 1-2 only (parcel -> address). Owner
fuzzy matching is deliberately excluded: an LLC owning many properties would
link to the wrong one.

The cascade is already county-scoped; the county check after a match is a
guard against that ever changing, since a cross-county link would attach a
lead to the wrong property.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.matching import for_county
from config.property_radar import COUNTY_FIPS_TO_SLUG
from src.loaders.base import BaseLoader

logger = logging.getLogger(__name__)

LINKED = "linked"
NO_MATCH = "no_match"
COUNTY_MISMATCH = "skipped_county_mismatch"


class _MatchOnlyLoader(BaseLoader):
    """BaseLoader used only for its matching cascade; never loads data."""

    def load_from_dataframe(self, df: pd.DataFrame, skip_duplicates: bool = True):
        raise NotImplementedError


_FETCH_UNLINKED_SQL = """
    SELECT id, county_fips, apn, property_address, city, zip
    FROM property_radar_records
    WHERE property_id IS NULL
      AND county_fips = ANY(:fips_list)
      AND id > :last_id
    ORDER BY id
    LIMIT :batch_size
"""

_SET_LINK_SQL = """
    UPDATE property_radar_records r
    SET property_id = x.property_id, match_method = x.match_method, match_confidence = x.match_confidence
    FROM jsonb_to_recordset(CAST(:rows AS jsonb))
         AS x(id bigint, property_id bigint, match_method text, match_confidence integer)
    WHERE r.id = x.id
"""


def link_unlinked(session: Session, *, batch_size: int = 500) -> dict[str, int]:
    """Link unlinked records in loaded counties. Returns counts per outcome."""
    counts = {LINKED: 0, NO_MATCH: 0, COUNTY_MISMATCH: 0}
    fips_to_slug = dict(COUNTY_FIPS_TO_SLUG)
    if not fips_to_slug:
        return counts

    try:
        _link_pages(session, fips_to_slug, batch_size, counts)
    except SQLAlchemyError:
        logger.exception("PropertyRadar link_unlinked failed after %s", counts)
        raise
    logger.info(
        "PropertyRadar link_unlinked: linked=%d no_match=%d county_mismatch=%d",
        counts[LINKED], counts[NO_MATCH], counts[COUNTY_MISMATCH],
    )
    return counts


def _link_pages(session: Session, fips_to_slug: dict[str, str], batch_size: int, counts: dict[str, int]) -> None:
    loaders = {slug: _MatchOnlyLoader(session, county_id=slug) for slug in set(fips_to_slug.values())}
    last_id = 0

    while True:
        rows = session.execute(
            text(_FETCH_UNLINKED_SQL),
            {"fips_list": list(fips_to_slug), "last_id": last_id, "batch_size": batch_size},
        ).mappings().all()
        if not rows:
            break
        last_id = rows[-1]["id"]

        links: list[dict[str, Any]] = []
        for row in rows:
            slug = fips_to_slug[row["county_fips"]]
            outcome, link = _match(loaders[slug], slug, row)
            counts[outcome] += 1
            if link:
                links.append(link)
        if links:
            session.execute(text(_SET_LINK_SQL), {"rows": json.dumps(links)})

        if len(rows) < batch_size:
            break

    session.flush()


def _match(loader: BaseLoader, slug: str, row: Any) -> tuple[str, dict[str, Any] | None]:
    # Address hits below auto_match are "pending_review" everywhere else in FA;
    # a staging link has no review step, so only accept auto-match quality.
    prop, method, confidence = loader.find_property_cascade(
        parcel_id=row["apn"],
        address=row["property_address"],
        zip_code=row["zip"],
        city=row["city"],
        addr_threshold=round(for_county(slug).auto_match * 100),
    )
    if prop is None:
        return NO_MATCH, None
    if (prop.county_id or "").lower() != slug.lower():
        logger.warning("County mismatch for record id=%s: expected %s got %s", row["id"], slug, prop.county_id)
        return COUNTY_MISMATCH, None
    return LINKED, {"id": row["id"], "property_id": prop.id, "match_method": method,
                    "match_confidence": round(confidence) if confidence is not None else None}
