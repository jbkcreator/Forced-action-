"""CFPB rural/underserved lookup by ZIP: ZIP -> county FIPS (Census crosswalk) -> CFPB county list."""
from __future__ import annotations

import csv
from functools import lru_cache
from typing import Optional

from config.lending_instant_quote import CFPB_RURAL_COUNTIES_FILE, ZIP_COUNTY_FILE


@lru_cache(maxsize=1)
def _zip_counties() -> dict[str, str]:
    with ZIP_COUNTY_FILE.open(newline="", encoding="utf-8") as f:
        return {row["zip"]: row["county_fips"] for row in csv.DictReader(f)}


@lru_cache(maxsize=1)
def _rural_counties() -> frozenset[str]:
    with CFPB_RURAL_COUNTIES_FILE.open(newline="", encoding="utf-8-sig") as f:
        return frozenset(row["FIPS Code"] for row in csv.DictReader(f))


def is_rural(zip_code: str) -> Optional[bool]:
    """True/False for a known ZIP; None when the ZIP is not in the crosswalk."""
    county = _zip_counties().get(zip_code)
    if county is None:
        return None
    return county in _rural_counties()
