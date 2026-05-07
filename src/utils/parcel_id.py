"""
County-aware parcel ID normalization.

Hillsborough uses 'folio' format  — stored as-is from HCPA bulk CSV (10-digit
numeric string or compact alpha-numeric).  The canonical form strips leading/
trailing whitespace and uppercases any alpha characters.

Pinellas uses 'strap' format  — Section-Township-Range-SubDivision-Block-Lot
encoded as SS-TT-RR-SSSSS-LLL-LLLL (six hyphen-separated groups).  Compact
18-digit strings are accepted and expanded; already-hyphenated strings are
validated and returned in canonical form.

Public API
----------
    normalize_parcel_id(raw, county_id) -> str
        Main entry point used by all loaders.  Dispatches to the
        county-specific normalizer based on parcel_id_format from county config.
"""

import re
from typing import Optional

from src.utils.county_config import get_county_config

# ---------------------------------------------------------------------------
# Folio (Hillsborough)
# ---------------------------------------------------------------------------

_FOLIO_STRIP_RE = re.compile(r'[^\w\-]')  # keep word chars and hyphens


def _normalize_folio(raw: str) -> str:
    """
    Normalize a Hillsborough-style folio parcel ID.

    Strips surrounding whitespace and non-word characters, uppercases
    alpha characters, collapses repeated hyphens/spaces.
    """
    cleaned = _FOLIO_STRIP_RE.sub('', raw.strip()).upper()
    # Collapse any run of hyphens to a single one
    cleaned = re.sub(r'-{2,}', '-', cleaned)
    return cleaned.strip('-')


# ---------------------------------------------------------------------------
# STRAP (Pinellas)
# STRAP canonical form: SS-TT-RR-SSSSS-LLL-LLLL (2-2-2-5-3-4 = 18 digits)
# ---------------------------------------------------------------------------

_STRAP_HYPHENATED_RE = re.compile(
    r'^(\d{1,2})-(\d{1,2})-(\d{1,2})-(\d{1,5})-(\d{1,3})-(\d{1,4})$'
)


def _normalize_strap(raw: str) -> str:
    """
    Normalize a Pinellas-style STRAP parcel ID to canonical SS-TT-RR-SSSSS-LLL-LLLL.

    Accepts:
    - Already-hyphenated: '08-31-15-00000-001-0100'  →  '08-31-15-00000-001-0100'
    - Compact 18-digit:  '083115000000010100'         →  '08-31-15-00000-001-0100'
    - Partial-padding:   '8-31-15-0-1-100'            →  '08-31-15-00000-001-0100'

    Raises ValueError for strings that cannot be interpreted as a valid STRAP.
    """
    raw = raw.strip()

    # Already hyphenated?
    m = _STRAP_HYPHENATED_RE.match(raw)
    if m:
        sec, twn, rng, sub, blk, lot = m.groups()
        return f"{int(sec):02d}-{int(twn):02d}-{int(rng):02d}-{int(sub):05d}-{int(blk):03d}-{int(lot):04d}"

    # Compact digits only (18 chars)?
    digits_only = re.sub(r'\D', '', raw)
    if len(digits_only) == 18:
        return (
            f"{digits_only[0:2]}-{digits_only[2:4]}-{digits_only[4:6]}"
            f"-{digits_only[6:11]}-{digits_only[11:14]}-{digits_only[14:18]}"
        )

    raise ValueError(
        f"Cannot parse as Pinellas STRAP parcel ID: {raw!r}. "
        "Expected 'SS-TT-RR-SSSSS-LLL-LLLL' or 18-digit compact form."
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def normalize_parcel_id(raw: Optional[str], county_id: str) -> str:
    """
    Normalize a raw parcel ID string for the given county.

    Dispatches to the county-specific normalizer based on the
    parcel_id_format field in the county config.

    Args:
        raw:       Raw parcel ID string from a scraper or CSV.
        county_id: County identifier (e.g. 'hillsborough', 'pinellas').

    Returns:
        Canonical parcel ID string.

    Raises:
        ValueError: If raw is empty/None or cannot be parsed for the county format.
    """
    if not raw or (isinstance(raw, float)):
        raise ValueError(f"Empty or null parcel ID for county '{county_id}'")

    raw = str(raw).strip()
    if not raw or raw.lower() == 'nan':
        raise ValueError(f"Empty parcel ID for county '{county_id}'")

    cfg = get_county_config(county_id)
    fmt = cfg.get("parcel_id_format", "folio")

    if fmt == "strap":
        return _normalize_strap(raw)
    return _normalize_folio(raw)
