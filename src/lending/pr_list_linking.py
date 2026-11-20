"""Link PropertyRadar calling-list records (``lending.pr_list_records``) to Forced Action properties.

A county is linkable when its ``counties`` row has a FIPS code and Forced Action holds parcels for it, read from the
database each run (not a hard-coded list), so a county is picked up the night its parcels are first loaded. Matching
is the same as the maturity pipeline's linker (src/services/property_radar/linking.py): parcel, then address at the
county's auto-match threshold, never owner name (an investor owning many properties would link to the wrong one).

A record in a county without parcels is left unlinked (``linked_at`` NULL) and tried again once that county loads.
A record that was tried is stamped ``linked_at`` with its outcome, matched or not, so it is never matched twice.
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from typing import Any

from sqlalchemy import text

from config.matching import for_county
from config.property_radar_fips import COUNTY_ALIASES, FIPS_BY_STATE, STATE_FIPS

logger = logging.getLogger(__name__)

LINKED, NO_MATCH, COUNTY_MISMATCH = "linked", "no_match", "county_mismatch"

_LINKABLE_COUNTIES_SQL = text("""
    SELECT c.county_id, c.fips FROM counties c
     WHERE nullif(btrim(c.fips), '') IS NOT NULL
       AND EXISTS (SELECT 1 FROM properties p WHERE p.county_id = c.county_id)
""")

_FETCH_SQL = text("""
    SELECT id, state, upper(btrim(county)) AS county, apn, property_address, city, zip
      FROM lending.pr_list_records
     WHERE linked_at IS NULL AND (state || ':' || upper(btrim(county))) = ANY(:keys) AND id > :last_id
     ORDER BY id LIMIT :batch_size
""")

_SET_SQL = text("""
    UPDATE lending.pr_list_records r
       SET property_id = x.property_id, match_method = x.match_method, match_confidence = x.match_confidence,
           linked_at = now()
      FROM jsonb_to_recordset(CAST(:rows AS jsonb))
           AS x(id bigint, property_id integer, match_method text, match_confidence numeric)
     WHERE r.id = x.id
""")


def full_fips(raw: str) -> str:
    """A county FIPS as five digits. A three-digit code is a Florida county (every county Forced Action loaded
    before Georgia was added is in Florida)."""
    digits = raw.strip()
    return digits if len(digits) == 5 else f"{STATE_FIPS['FL']}{digits.zfill(3)}"


def county_names_by_fips() -> dict[str, list[tuple[str, str]]]:
    """FIPS -> every (state, county name) PropertyRadar may use for it, aliases included."""
    names: dict[str, list[tuple[str, str]]] = {}
    for state, counties in FIPS_BY_STATE.items():
        for name, fips in counties.items():
            names.setdefault(fips, []).append((state, name.upper()))
        for alias, canonical in COUNTY_ALIASES.get(state, {}).items():
            fips = counties.get(canonical)
            if fips:
                names.setdefault(fips, []).append((state, alias.upper()))
    return names


def linkable_counties(db) -> dict[str, str]:
    """FIPS -> Forced Action county slug, for every county with a FIPS code and parcels."""
    return {full_fips(row.fips): row.county_id for row in db.execute(_LINKABLE_COUNTIES_SQL)}


def link_unlinked_records(db, *, batch_size: int = 500, loader_for=None) -> dict[str, Any]:
    """Link every not-yet-tried record in a linkable county. Returns the outcome counts and the linkable FIPS codes
    (the number reported to Josh). ``loader_for(slug)`` is for tests."""
    fips_to_slug = linkable_counties(db)
    names = county_names_by_fips()
    key_to_slug = {f"{state}:{name}": slug for fips, slug in fips_to_slug.items() for state, name in names.get(fips, [])}
    counts: Counter = Counter()
    if key_to_slug:
        loaders: dict[str, Any] = {}
        make_loader = loader_for or _match_only_loader(db)
        last_id = 0
        while True:
            rows = db.execute(_FETCH_SQL, {"keys": sorted(key_to_slug), "last_id": last_id,
                                           "batch_size": batch_size}).mappings().all()
            if not rows:
                break
            last_id = rows[-1]["id"]
            links = []
            for row in rows:
                slug = key_to_slug[f"{row['state']}:{row['county']}"]
                loader = loaders.setdefault(slug, make_loader(slug))
                outcome, link = match_record(loader, slug, row)
                counts[outcome] += 1
                links.append(link)
            db.execute(_SET_SQL, {"rows": json.dumps(links)})
            if len(rows) < batch_size:
                break
    summary = {"linked": counts[LINKED], "no_match": counts[NO_MATCH], "county_mismatch": counts[COUNTY_MISMATCH],
               "linkable_county_fips": sorted(fips_to_slug)}
    logger.info("[pr-list-link] %s", json.dumps(summary))
    return summary


def _match_only_loader(db):
    from src.services.property_radar.linking import _MatchOnlyLoader

    return lambda slug: _MatchOnlyLoader(db, county_id=slug)


def match_record(loader, slug: str, row) -> tuple[str, dict[str, Any]]:
    """Parcel, then address at the county's auto-match threshold (below it FA would ask for a review, and a link
    made at night has no review step). Every outcome is returned as a row to stamp."""
    prop, method, confidence = loader.find_property_cascade(
        parcel_id=row["apn"], address=row["property_address"], zip_code=row["zip"], city=row["city"],
        addr_threshold=round(for_county(slug).auto_match * 100))
    tried = {"id": row["id"], "property_id": None, "match_method": NO_MATCH, "match_confidence": None}
    if prop is None:
        return NO_MATCH, tried
    if (prop.county_id or "").lower() != slug.lower():
        logger.warning("[pr-list-link] record %s matched a property in %s, not %s; left unlinked", row["id"],
                       prop.county_id, slug)
        return COUNTY_MISMATCH, {**tried, "match_method": COUNTY_MISMATCH}
    score = None if confidence is None else round(float(confidence) / 100 if confidence > 1 else float(confidence), 3)
    return LINKED, {**tried, "property_id": prop.id, "match_method": method, "match_confidence": score}
