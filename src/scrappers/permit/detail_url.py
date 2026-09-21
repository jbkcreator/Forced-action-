"""Stage A — Accela CapDetail URL derivation (pure, no network)."""
from urllib.parse import parse_qs, urlparse
from typing import Optional

_DETAIL_BASE = "https://aca-prod.accela.com/{agency}/Cap/CapDetail.aspx"


def build_detail_url(agency_code: str, cap_id1: str, cap_id2: str, cap_id3: str) -> str:
    base = _DETAIL_BASE.format(agency=agency_code)
    return (
        f"{base}?Module=Building&TabName=Building"
        f"&capID1={cap_id1}&capID2={cap_id2}&capID3={cap_id3}"
        f"&agencyCode={agency_code}"
    )


def parse_cap_ids(href: str) -> Optional[tuple[str, str, str, str]]:
    """Extract (agency_code, cap_id1, cap_id2, cap_id3) from a CapDetail anchor href.

    Returns None if the href is not a CapDetail link or is missing required params.
    """
    if not href or "CapDetail" not in href:
        return None
    parsed = urlparse(href)
    qs = parse_qs(parsed.query)
    agency = qs.get("agencyCode", [None])[0]
    id1 = qs.get("capID1", [None])[0]
    id2 = qs.get("capID2", [None])[0]
    id3 = qs.get("capID3", [None])[0]
    if not all([agency, id1, id2, id3]):
        return None
    return agency, id1, id2, id3
