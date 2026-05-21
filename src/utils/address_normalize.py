"""
Canonical street-address normalization for ingestion and match-time use.

`normalize_street_address(addr)` is the single function called by both
property ingestion (MasterPropertyLoader) and the matching waterfall
(BaseLoader.normalize_address wraps this and adds county-specific
city-token stripping). The function is stateless, idempotent, and has no
DB access.

Strategy:
  1. Reject obviously invalid inputs (intersections, "right of way", etc).
  2. Cheap pre-parse cleanup (lowercase, strip city/state/ZIP, FL highway
     two-word types).
  3. Hand off to `usaddress.tag()` which returns USPS-labelled components
     and naturally distinguishes pre- vs. post-directionals.
  4. Reassemble: house# (zero-padding stripped) + pre-dir + street + USPS
     suffix abbreviation + post-dir. Drop occupancy/city/state/ZIP.
  5. On `usaddress.RepeatedLabelError` or any other parse failure, fall
     back to a regex pipeline with the same suffix/directional tables.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd
import usaddress


# USPS standard suffix abbreviations — both long-form and pre-abbreviated
# variants map to the same canonical form so the result is idempotent.
SUFFIX_MAP = {
    "ALLEY": "aly", "ALY": "aly",
    "AVENUE": "ave", "AVE": "ave", "AV": "ave",
    "BOULEVARD": "blvd", "BLVD": "blvd",
    "CIRCLE": "cir", "CIR": "cir",
    "COURT": "ct", "CT": "ct",
    "COVE": "cv", "CV": "cv",
    "CROSSING": "xing", "XING": "xing",
    "DRIVE": "dr", "DR": "dr",
    "EXPRESSWAY": "expy", "EXPY": "expy",
    "EXTENSION": "ext", "EXT": "ext",
    "FREEWAY": "fwy", "FWY": "fwy",
    "GROVE": "grv", "GRV": "grv",
    "HARBOR": "hbr", "HBR": "hbr",
    "HIGHWAY": "hwy", "HWY": "hwy",
    "HOLLOW": "holw", "HOLW": "holw",
    "JUNCTION": "jct", "JCT": "jct",
    "LAKE": "lk", "LK": "lk",
    "LANE": "ln", "LN": "ln",
    "LOOP": "loop",
    "MANOR": "mnr", "MNR": "mnr",
    "MEADOW": "mdw", "MDW": "mdw",
    "PARKWAY": "pkwy", "PKWY": "pkwy",
    "PASS": "pass",
    "PATH": "path",
    "PLACE": "pl", "PL": "pl",
    "PLAZA": "plz", "PLZ": "plz",
    "POINT": "pt", "PT": "pt",
    "RIDGE": "rdg", "RDG": "rdg",
    "RISE": "rise",
    "ROAD": "rd", "RD": "rd",
    "ROW": "row",
    "RUN": "run",
    "SHORE": "shr", "SHR": "shr",
    "SQUARE": "sq", "SQ": "sq",
    "STREET": "st", "ST": "st",
    "TERRACE": "ter", "TER": "ter", "TERR": "ter",
    "TRACE": "trce", "TRCE": "trce",
    "TRAIL": "trl", "TRL": "trl",
    "TURNPIKE": "tpke", "TPKE": "tpke",
    "VIEW": "vw", "VW": "vw",
    "VILLAGE": "vlg", "VLG": "vlg",
    "WALK": "walk",
    "WAY": "way",  # USPS abbreviation for WAY is WAY (WY = Wyoming)
    "WOODS": "wds", "WDS": "wds",
}

DIRECTIONAL = {
    "NORTH": "n", "SOUTH": "s", "EAST": "e", "WEST": "w",
    "NORTHEAST": "ne", "NORTHWEST": "nw",
    "SOUTHEAST": "se", "SOUTHWEST": "sw",
    "N": "n", "S": "s", "E": "e", "W": "w",
    "NE": "ne", "NW": "nw", "SE": "se", "SW": "sw",
}

_INVALID_PATTERNS = (
    "not provided", "landlord/tenant", "progress residential",
    "right of way", "right of wy", "right-of-way",
    "processed", "row at", "intersection",
    "final", "piles at", "accumulations", "county facility",
)

# Compiled once at module load
_RE_ZIP = re.compile(r"^\d{5}(-\d{4})?$")
_RE_UNIT = re.compile(
    r"\s+(apt|unit|lot|ste|suite|bldg|building|fl|floor|#)\s*[\w-]+",
    re.IGNORECASE,
)
_RE_TRAILING_HASH = re.compile(r"\s+#[\w-]+$")
_RE_DIRECTIONAL_WORD = re.compile(
    r"(?<!\w)(northeast|northwest|southeast|southwest|north|south|east|west)(?!\w)",
    re.IGNORECASE,
)


def normalize_street_address(addr: Optional[str]) -> str:
    """
    Normalize a US street address to a canonical lowercase form for match-time
    comparison.

    Returns "" for null/empty input, intersections, or any address that does
    not start with a house number. No DB access. Idempotent.
    """
    if addr is None or (isinstance(addr, float) and pd.isna(addr)):
        return ""
    s = str(addr).lower().strip()
    if not s:
        return ""

    for pat in _INVALID_PATTERNS:
        if pat in s:
            return ""
    if " & " in s or " and " in s:
        return ""

    s = s.split(";")[0].strip()
    s = s.replace(".", "")
    # FL two-word type usaddress doesn't tag cleanly
    s = re.sub(r"\bstate road\b", "sr", s)
    s = s.split(",")[0].strip()
    # Strip trailing " fl " state suffix before ZIP
    s = s.split(" fl ")[0].strip()
    # Strip trailing 5-digit or ZIP+4
    parts = s.split()
    if parts and _RE_ZIP.match(parts[-1]):
        s = " ".join(parts[:-1])

    if not s:
        return ""

    try:
        components, _ = usaddress.tag(s)
        result = _assemble(components)
        if result:
            return result
    except usaddress.RepeatedLabelError:
        pass
    except Exception:
        pass

    return _regex_fallback(s)


def _assemble(components) -> str:
    """Reassemble normalized form from usaddress component dict."""
    num_raw = (components.get("AddressNumber") or "").strip()
    if not num_raw or not any(c.isdigit() for c in num_raw):
        return ""
    # Strip leading zeros from pure-digit house numbers
    num = str(int(num_raw)) if num_raw.isdigit() else num_raw

    num_suffix = (components.get("AddressNumberSuffix") or "").strip().lower()

    pre_dir_raw = (components.get("StreetNamePreDirectional") or "").strip().upper()
    pre_dir = DIRECTIONAL.get(pre_dir_raw, pre_dir_raw.lower())

    pre_type_raw = (components.get("StreetNamePreType") or "").strip().upper()
    pre_type = SUFFIX_MAP.get(pre_type_raw, pre_type_raw.lower())

    street_raw = (components.get("StreetName") or "").strip()
    post_type_raw = (components.get("StreetNamePostType") or "").strip().upper()

    # usaddress sometimes folds suffixes like GROVE/VILLAGE into StreetName.
    # If the last token of StreetName is a known suffix and we have no
    # explicit PostType, split it off so abbreviation applies.
    if not post_type_raw and street_raw:
        street_tokens = street_raw.split()
        if len(street_tokens) > 1 and street_tokens[-1].upper() in SUFFIX_MAP:
            post_type_raw = street_tokens[-1].upper()
            street_raw = " ".join(street_tokens[:-1])

    street = street_raw.lower()
    post_type = SUFFIX_MAP.get(post_type_raw, post_type_raw.lower())

    post_dir_raw = (components.get("StreetNamePostDirectional") or "").strip().upper()
    post_dir = DIRECTIONAL.get(post_dir_raw, post_dir_raw.lower())

    tokens = [num, num_suffix, pre_dir, pre_type, street, post_type, post_dir]
    return " ".join(t for t in tokens if t)


def _regex_fallback(addr: str) -> str:
    """Fallback path when usaddress raises RepeatedLabelError or fails."""
    s = addr

    # Long-form → abbreviated suffix replacement (longest first to avoid
    # partial matches: "boulevard" before "blvd", "crossing" before "xing").
    long_forms = sorted(
        (k for k in SUFFIX_MAP if len(k) > 4),
        key=len,
        reverse=True,
    )
    for k in long_forms:
        s = re.sub(rf"\b{k.lower()}\b", SUFFIX_MAP[k], s)

    # Directionals with word-boundary regex (handles start/middle/end of string)
    s = _RE_DIRECTIONAL_WORD.sub(
        lambda m: DIRECTIONAL[m.group(1).upper()],
        s,
    )

    s = " ".join(s.split())

    # Strip unit/apt/suite/bldg designators
    s = _RE_UNIT.sub("", s)
    s = _RE_TRAILING_HASH.sub("", s)

    parts = s.split()
    if not parts or not any(c.isdigit() for c in parts[0]):
        return ""

    # Zero-pad strip on pure-digit house number
    if parts[0].isdigit():
        parts[0] = str(int(parts[0]))
    s = " ".join(parts)

    return s.strip()
