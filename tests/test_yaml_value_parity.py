"""Parity test: every literal moved from Python to YAML must keep its exact value.

Scaffolding only — the per-engine assertions are filled in as each engine
migrates. Each parametrize case pins the literal value (or list of values)
that lived in the pre-refactor Python module on `dev`, then asserts the YAML
loader returns the same value byte-for-byte.

Workflow per engine migration:
    1. Before editing the engine file, snapshot the literal in the assertion
       below — copy/paste from the dev branch, do not retype.
    2. Move the literal to its YAML.
    3. Edit the engine to call `scraper_config.get_*`.
    4. Re-run this test. Must pass before the engine PR merges.

Engines marked `pytest.skip` are not yet migrated; remove the skip when they are.
"""

import pytest


# ---------------------------------------------------------------------------
# Helpers — wired in once `src/utils/scraper_config.py` exists.
# ---------------------------------------------------------------------------

def _loader():
    """Lazy import so this file collects even before scraper_config.py exists."""
    from src.utils import scraper_config

    return scraper_config


# ---------------------------------------------------------------------------
# Per-engine parity assertions. Add one test per migrated engine.
# Pin literal values inline; do not reference module attributes from the
# engine being tested, otherwise we'd just be comparing the YAML to itself.
# ---------------------------------------------------------------------------


def test_fire_keywords_parity():
    # Snapshot from src/scrappers/fire/fire_engine.py @ dev (FIRE_INCIDENT_TYPES).
    expected = [
        "structure fire",
        "fire",
        "smoke",
        "explosion",
        "arson",
        "wildland fire",
        "vehicle fire",
    ]
    assert _loader().get_keywords("fire") == expected


def test_fire_agent_finalize_wait_parity():
    # Pre-refactor: hardcoded `await asyncio.sleep(15)` after the AI agent run.
    assert _loader().get_timeout("fire", "agent_finalize_wait") == 15


def test_flood_keywords_parity():
    expected = [
        "Flash Flood Warning",
        "Flash Flood Watch",
        "Flood Warning",
        "Flood Watch",
        "Flood Advisory",
        "Coastal Flood Warning",
        "Coastal Flood Advisory",
        "Areal Flood Warning",
    ]
    assert _loader().get_keywords("flood") == expected


def test_storm_keywords_parity():
    expected = [
        "Tornado Warning",
        "Tornado Watch",
        "Severe Thunderstorm Warning",
        "Severe Thunderstorm Watch",
        "Hurricane Warning",
        "Hurricane Watch",
        "Tropical Storm Warning",
        "Tropical Storm Watch",
        "High Wind Warning",
        "Wind Advisory",
        "Special Weather Statement",
        "Flash Flood Warning",
        "Flood Warning",
    ]
    assert _loader().get_keywords("storm") == expected


def test_insurance_keywords_parity():
    expected = [
        "insurance",
        "adjuster",
        "claim",
        "damage assessment",
        "damage repair",
        "storm damage",
        "flood damage",
        "fire damage",
        "wind damage",
        "hail damage",
    ]
    assert _loader().get_keywords("insurance") == expected


def test_foreclosure_selectors_parity():
    expected = {
        "party_grid_rows": "#obpa-grid tbody tr",
        "bid_window":      "#BID_WINDOW_CONTAINER",
    }
    assert _loader().get_selectors("foreclosure") == expected


def test_foreclosure_page_wait_parity():
    # Pre-refactor: `await page.wait_for_timeout(2_000)` in section pagination.
    assert _loader().get_timeout("foreclosure", "page_wait_ms") == 2000


def test_evictions_patterns_parity():
    expected = {
        "filename_regex": r"CivilFiling_(\d{8})\.csv",
        "case_types": [
            "LT Residential Eviction",
            "LT Commercial Eviction",
            "Eviction",
        ],
    }
    assert _loader().get_patterns("evictions") == expected


def test_probate_patterns_parity():
    expected = {
        "filename_regex": r"ProbateFiling_(\d{8})\.csv",
    }
    assert _loader().get_patterns("probate") == expected


def test_divorce_patterns_parity():
    expected = {
        "filename_regex": r"CivilFiling_(\d{8})\.csv",
        "case_types": [
            "DR Dissolution of Marriage",
            "DR Dissolution",
            "Dissolution of Marriage",
            "Domestic Relations",
            "Family Law",
        ],
    }
    assert _loader().get_patterns("divorce") == expected


@pytest.mark.skip(
    reason="liens not migrated: doc-type codes are inlined in MODE_CONFIGS dict alongside "
    "Playwright option-value strings — extraction would require a structural refactor "
    "beyond the tidy-up scope. Stub YAML retained for future."
)
def test_liens_doc_types_parity():
    pass


@pytest.mark.skip(
    reason="bankruptcy not migrated: court_code + division_prefix already live in "
    "config/counties.json with constants.py fallbacks. Per plan, jurisdictional config "
    "stays in counties.json."
)
def test_bankruptcy_patterns_parity():
    pass


def test_master_patterns_parity():
    expected = {
        "chunk_size": 50000,
        "raw_file_encoding": "cp1252",
        "dbf_signature_bytes": [0x02, 0x03, 0x04, 0x05, 0x83, 0x8B, 0x8C],
    }
    assert _loader().get_patterns("master") == expected


def test_permits_selectors_parity():
    # Snapshot from src/scrappers/permit/permit_engine.py @ dev (Accela ctl00_* IDs).
    expected = {
        "start_date":      "ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate",
        "end_date":        "ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate",
        "search_button":   "ctl00_PlaceHolderMain_btnNewSearch",
        "download_button": "ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport",
    }
    assert _loader().get_selectors("permits") == expected


def test_violations_selectors_parity():
    # Snapshot from src/scrappers/violation/violation_engine.py @ dev.
    expected = {
        "start_date":    "ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate",
        "end_date":      "ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate",
        "search_button": "ctl00_PlaceHolderMain_btnNewSearch",
    }
    assert _loader().get_selectors("violations") == expected


def test_roofing_permit_keywords_parity():
    expected = [
        "roof", "shingle", "tpo", "tile", "fascia",
        "soffit", "gutters", "flashing", "underlayment",
        "re-roof", "reroof",
    ]
    assert _loader().get_keywords("roofing_permit") == expected


def test_dbpr_license_to_vertical_parity():
    expected = {
        "CCC":  "roofing",
        "CRC":  "roofing",
        "CGC":  "general",
        "CBC":  "general",
        "RGC":  "general",
        "RBC":  "general",
        "CFC":  "plumbing",
        "RFC":  "plumbing",
        "CAC":  "hvac",
        "CMC":  "hvac",
        "RAC":  "hvac",
        "RMC":  "hvac",
        "MRSA": "remediation",
        "MRSR": "remediation",
    }
    assert _loader().get_scraper_config("dbpr")["license_to_vertical"] == expected


def test_sunbiz_polite_crawl_delay_parity():
    # Pre-refactor: `_DELAY_SECONDS = 1.5`
    assert _loader().get_scraper_config("sunbiz")["polite_crawl_delay_seconds"] == 1.5


@pytest.mark.skip(reason="tax_delinquent engine disabled in production — stub-only YAML")
def test_tax_delinquent_parity():
    pass
