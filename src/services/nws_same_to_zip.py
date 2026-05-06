"""
NWS SAME / FIPS code → ZIP-code lookup (fa008+, 2026-05-04).

Raw NWS CAP alerts include `geocode.SAME` (county FIPS encoded as 0 + 5-digit
FIPS) and `geocode.UGC` (UGC zone codes like FLZ248). Without a translation
layer, our `_extract_zip_codes` regex only catches alerts that happen to
include explicit ZIP codes in their description — most don't.

This file ships a static crosswalk for the platform's active + dormant
counties so the NWS webhook can convert FIPS to ZIP and dispatch the
storm-pack signal at ZIP resolution.

Source: USPS / HUD county-ZIP crosswalk (cleaned to PO ZIPs only — PO
boxes and unique-ZIP corporate codes excluded). Update when new counties
are activated. Keep alphabetised inside each list.

Format:
  SAME code is "0" + 5-digit FIPS (NWS encoding convention).
  UGC zone is "<state><type><nnn>" — type 'C' for county, 'Z' for zone.
"""

# ── SAME codes (NWS prefixes a leading '0' to the 5-digit FIPS) ─────────────

SAME_TO_ZIPS: dict[str, list[str]] = {
    # Hillsborough County, FL (FIPS 12057) — primary active county
    "012057": [
        "33510", "33511", "33527", "33534", "33547", "33548", "33549", "33556",
        "33558", "33559", "33563", "33565", "33566", "33567", "33569", "33570",
        "33572", "33573", "33578", "33579", "33584", "33592", "33594", "33596",
        "33598", "33602", "33603", "33604", "33605", "33606", "33607", "33609",
        "33610", "33611", "33612", "33613", "33614", "33615", "33616", "33617",
        "33618", "33619", "33620", "33624", "33625", "33626", "33629", "33634",
        "33635", "33637", "33647",
    ],
    # Pinellas County, FL (FIPS 12103) — dormant
    "012103": [
        "33701", "33702", "33703", "33704", "33705", "33706", "33707", "33708",
        "33709", "33710", "33711", "33712", "33713", "33714", "33715", "33716",
        "33755", "33756", "33759", "33760", "33761", "33762", "33763", "33764",
        "33765", "33767", "33770", "33771", "33772", "33773", "33774", "33776",
        "33777", "33778", "33781", "33782", "33785", "33786",
    ],
    # Pasco County, FL (FIPS 12101) — dormant
    "012101": [
        "33523", "33525", "33540", "33541", "33542", "33543", "33544", "33545",
        "33576", "34610", "34637", "34638", "34639", "34652", "34653", "34654",
        "34655", "34667", "34668", "34669", "34690", "34691",
    ],
    # Polk County, FL (FIPS 12105) — dormant
    "012105": [
        "33801", "33803", "33805", "33809", "33810", "33811", "33812", "33813",
        "33815", "33823", "33825", "33827", "33830", "33837", "33838", "33839",
        "33841", "33843", "33844", "33846", "33847", "33850", "33853", "33860",
        "33867", "33868", "33880", "33881", "33884", "33896", "33897", "33898",
        "34759",
    ],
    # Manatee County, FL (FIPS 12081) — dormant
    "012081": [
        "34201", "34202", "34203", "34205", "34207", "34208", "34209", "34210",
        "34211", "34212", "34215", "34217", "34219", "34221", "34222", "34228",
        "34243", "34251",
    ],
}

# ── UGC zone codes — keep as a thin alias to the same county SAME entry ────
# UGC is the NWS "warning zone" partition that doesn't always align with
# counties, but for FL the zones map cleanly to single counties for our
# active set. If a future county splits into multiple zones, expand this.

UGC_TO_ZIPS: dict[str, list[str]] = {
    "FLC057": SAME_TO_ZIPS["012057"],   # Hillsborough
    "FLC103": SAME_TO_ZIPS["012103"],   # Pinellas
    "FLC101": SAME_TO_ZIPS["012101"],   # Pasco
    "FLC105": SAME_TO_ZIPS["012105"],   # Polk
    "FLC081": SAME_TO_ZIPS["012081"],   # Manatee
}


def expand_codes(same_codes: list[str], ugc_codes: list[str]) -> list[str]:
    """Resolve any combination of SAME + UGC codes to a deduped ZIP list.

    Unknown codes are silently ignored — the regex / parameters fallback in
    `nws_webhook._extract_zip_codes` covers anything we don't have in the
    crosswalk.
    """
    out: set[str] = set()
    for code in same_codes or []:
        out.update(SAME_TO_ZIPS.get(code, []))
    for code in ugc_codes or []:
        out.update(UGC_TO_ZIPS.get(code, []))
    return sorted(out)
