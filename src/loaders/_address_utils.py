"""
Address parsing helper for the loader matching cascade.

Source records arrive with freeform addresses ("123 MAIN ST, TAMPA, FL 33602",
sometimes "123 MAIN ST APT 5 TAMPA FL 33602-1234", sometimes garbage).
`split_address` extracts the components the cascade needs.

Never raises — bad/missing input returns `(None, None, None)`. Callers can pass
the results straight into `find_property_cascade` and the cascade will skip
stages whose required inputs are missing.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

# Match a 5-digit ZIP, optionally followed by -NNNN (ZIP+4). We only return
# the 5-digit prefix because that's what properties.zip stores.
_ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")

# Two-letter US state abbreviation (we only really expect FL but be tolerant).
_STATE_RE = re.compile(r"\b([A-Z]{2})\b")

# Comma-anchored city extractor: "STREET, CITY, ST ZIP" or "STREET, CITY ST ZIP"
_CITY_COMMA_RE = re.compile(r",\s*([A-Z][A-Z .\-']{1,40})\s*,?\s*[A-Z]{2}\b")

# Tokens that look like address noise and should not be treated as the city
_CITY_BLACKLIST = {
    "FL", "FLORIDA", "USA", "US", "UNITED STATES",
    "APT", "UNIT", "STE", "SUITE", "LOT", "BLDG", "BLOCK", "FLOOR", "FL.",
}


def split_address(addr_str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse a freeform address into (normalized_street, city, zip5).

    All three components are Optional — any may be None when not parseable.

    Examples
    --------
    >>> split_address("2617 BUCKHORN PRESERVE BLVD, VALRICO, FL 33594-6511")
    ('2617 BUCKHORN PRESERVE BLVD', 'VALRICO', '33594')

    >>> split_address("123 MAIN ST APT 5 TAMPA FL 33602")
    ('123 MAIN ST APT 5', 'TAMPA', '33602')

    >>> split_address(None)
    (None, None, None)

    >>> split_address("")
    (None, None, None)
    """
    if addr_str is None:
        return None, None, None

    s = str(addr_str).strip().upper()
    if not s or s in {"NAN", "NONE", "NULL"}:
        return None, None, None

    # ── ZIP (most reliable anchor) ────────────────────────────────────────
    zip_code: Optional[str] = None
    zip_match = _ZIP_RE.search(s)
    if zip_match:
        zip_code = zip_match.group(1)

    # ── City ──────────────────────────────────────────────────────────────
    # Prefer a comma-anchored city; fall back to "...CITY FL ZIP" pattern.
    city: Optional[str] = None
    cm = _CITY_COMMA_RE.search(s)
    if cm:
        candidate = cm.group(1).strip().rstrip(",").strip()
        if candidate and candidate not in _CITY_BLACKLIST:
            city = candidate
    elif zip_code:
        # No comma: look for "WORD WORD... ST ZIP" — city is the words before ST.
        # Slice off everything from the state code onwards.
        # Use a simpler pattern: the city is whatever non-digit/non-comma run
        # immediately precedes the state code + zip.
        m = re.search(r"\b([A-Z][A-Z .\-']{1,40})\s+[A-Z]{2}\s+\d{5}", s)
        if m:
            candidate = m.group(1).strip().rstrip(",").strip()
            if candidate and candidate not in _CITY_BLACKLIST:
                city = candidate

    # ── Street (normalized) ───────────────────────────────────────────────
    # Strip the city/state/zip tail to get the street component.
    street: Optional[str] = None
    if city:
        # Drop everything from the first occurrence of ", CITY" or " CITY " onward
        idx = s.find(f", {city}")
        if idx == -1:
            idx = s.find(f" {city} ")
        if idx == -1:
            idx = s.find(city)
        if idx > 0:
            street = s[:idx].rstrip(", ").strip()
    elif zip_code:
        # No city found but a ZIP did — chop at the state code if present.
        m = re.search(r"\s+[A-Z]{2}\s+\d{5}", s)
        if m:
            street = s[: m.start()].rstrip(", ").strip()
    else:
        # No anchors at all — treat the whole string as the street.
        street = s.rstrip(", ").strip()

    # Final sanity: an empty/whitespace street isn't useful
    if street is not None:
        street = " ".join(street.split())  # collapse whitespace
        if not street:
            street = None

    return street, city, zip_code
