"""
Application-wide constants for the Distressed Property Intelligence Platform.

This module centralizes all configuration constants used across different scrapers
and pipelines. Constants are organized by category for easy maintenance and reference.

Author: Distressed Property Intelligence Platform
"""

import tempfile
from pathlib import Path

# =============================================================================
# DIRECTORY PATHS
# =============================================================================

# Base directories
DATA_DIR = Path("data")
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REFERENCE_DATA_DIR = DATA_DIR / "reference"
DEBUG_DATA_DIR = DATA_DIR / "debug"

# Type-specific processed directories (lien engine produces 3 separate document types
# that must be stored in isolated folders to prevent cross-type deduplication errors)
PROCESSED_LIENS_DIR = PROCESSED_DATA_DIR / "liens"
PROCESSED_DEEDS_DIR = PROCESSED_DATA_DIR / "deeds"
PROCESSED_JUDGMENTS_DIR = PROCESSED_DATA_DIR / "judgments"

# Scraper-specific raw data directories
RAW_EVICTIONS_DIR = RAW_DATA_DIR / "evictions"
RAW_PROBATE_DIR = RAW_DATA_DIR / "probate"
RAW_PERMIT_DIR = RAW_DATA_DIR / "permits"
RAW_LIEN_DIR = RAW_DATA_DIR / "liens"
RAW_FORECLOSURE_DIR = RAW_DATA_DIR / "foreclosures"
RAW_VIOLATIONS_DIR = RAW_DATA_DIR / "violations"
RAW_BANKRUPTCY_DIR = RAW_DATA_DIR / "bankruptcy"
RAW_TAX_DELINQUENCIES_DIR = RAW_DATA_DIR / "tax_delinquencies"
RAW_MASTER_DIR = RAW_DATA_DIR / "master"
RAW_JUDGMENTS_DIR = RAW_DATA_DIR / "judgments"
RAW_DEEDS_DIR = RAW_DATA_DIR / "deeds"
RAW_FIRE_DIR = RAW_DATA_DIR / "fire"

# Temporary download directory for browser-use (cross-platform)
TEMP_DOWNLOADS_DIR = Path(tempfile.gettempdir())

# =============================================================================
# PORTAL URLS — loaded from config/counties.json via county_config utility.
#
# Use get_portal(county_id, key) in scraper functions instead of these
# module-level constants. The constants below are kept only for modules
# that have not yet been refactored to accept county_id.
# =============================================================================

from src.utils.county_config import get_county as _get_county

_hc = _get_county("hillsborough")["portals"]

HILLSCLERK_BASE_URL          = _hc["clerk_base_url"]
HILLSCLERK_PUBLIC_ACCESS_URL = _hc["clerk_public_access_url"]
CIVIL_FILINGS_URL            = _hc["civil_filings_url"]
PROBATE_FILINGS_URL          = _hc["probate_filings_url"]
ACCELA_BASE_URL              = _hc["accela_base_url"]
PERMIT_SEARCH_URL            = _hc["permit_search_url"]
VIOLATION_SEARCH_URL         = _hc["violation_search_url"]
REALFORECLOSE_BASE_URL       = _hc["realforeclose_base_url"]
TAX_COLLECTOR_BASE_URL       = _hc["tax_collector_base_url"]
PARCEL_LOOKUP_URL            = _hc["parcel_lookup_url"]
MASTER_DATA_URL              = _hc["master_data_url"]

# =============================================================================
# COURT LISTENER API - Federal Bankruptcy Court
# =============================================================================

COURTLISTENER_API_URL = "https://www.courtlistener.com/api/rest/v4/dockets/"
COURT_CODE_FLORIDA_MIDDLE_BANKRUPTCY = "flmb"
TAMPA_DIVISION_PREFIX = "8:"

# =============================================================================
# FILE PATTERNS
# =============================================================================

# Download file patterns for detecting completed downloads
DOWNLOAD_FILE_PATTERNS = ("*.csv", "*.xls", "*.xlsx", "*.json", "*.zip")

# Specific file name patterns
CIVIL_FILING_PATTERN = r"CivilFiling_(\d{8})\.csv"
PROBATE_FILING_PATTERN = r"ProbateFiling_(\d{8})\.csv"
MASTER_PARCEL_FILE = "PARCEL_SPREADSHEET.xls"

# Browser download temp directory pattern
BROWSER_DOWNLOAD_TEMP_PATTERN = "browser-use-downloads-*"

# =============================================================================
# EVICTION CASE TYPE PATTERNS
# =============================================================================

EVICTION_CASE_PATTERNS = [
    "LT Residential Eviction",
    "LT Commercial Eviction",
    "Eviction",
]

# =============================================================================
# TAX DELINQUENCY CONFIGURATION
# =============================================================================

DEFAULT_TAX_YEAR = 2026
DEFAULT_ACCOUNT_STATUS = "Unpaid"
MIN_YEARS_DELINQUENT = 2
REQUEST_DELAY_RANGE = (2.0, 4.0)  # seconds between requests for SNIPER phase

# =============================================================================
# DATE & TIME FORMATS
# =============================================================================

AUCTION_DATE_FORMAT = "%m/%d/%Y"
FILING_DATE_FORMAT = "%Y%m%d"
OUTPUT_DATE_FORMAT = "%Y%m%d"

# =============================================================================
# HTTP CONFIGURATION
# =============================================================================

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
API_USER_AGENT = "DistressedPropertyApp/1.0"

REQUEST_TIMEOUT_DEFAULT = 60   # seconds
REQUEST_TIMEOUT_LONG = 120     # seconds
REQUEST_TIMEOUT_EXTENDED = 300 # seconds

# =============================================================================
# BROWSER AUTOMATION CONFIGURATION
# =============================================================================

BROWSER_MODEL = "claude-sonnet-4-5-20250929"
BROWSER_TEMPERATURE = 0  # Deterministic for scraping

# Wait times (seconds)
DOWNLOAD_WAIT_DEFAULT = 10
DOWNLOAD_WAIT_PERMIT = 30
DOWNLOAD_WAIT_VIOLATION = 30
DOWNLOAD_WAIT_MASTER = 65

# =============================================================================
# LOGGING & OUTPUT
# =============================================================================

LOG_DIR = Path("logs")
OUTPUT_SEPARATOR = "=" * 80
OUTPUT_SEPARATOR_SHORT = "-" * 40

# =============================================================================
# LIEN & JUDGMENT DOCUMENT TYPES
# =============================================================================

LIEN_DOCUMENT_TYPES = {
    "LIEN": "General Liens",
    "LNCORPTX": "Corporate Tax Liens",
    "JUD": "Judgments",
    "CCJ": "Certified Judgments",
    "D": "Deeds",
    "TAXDEED": "Tax Deeds",
}

# =============================================================================
# COUNTY CONFIG
# Central registry of all per-county configuration.
# Add a new county here — scrapers, loaders, and API pick it up automatically.
# =============================================================================

COUNTY_CONFIG = {
    "hillsborough": {
        "display_name": "Hillsborough County",
        "state": "FL",
        "urls": {
            "permit":       PERMIT_SEARCH_URL,
            "violation":    VIOLATION_SEARCH_URL,
            "probate":      PROBATE_FILINGS_URL,
            "civil":        CIVIL_FILINGS_URL,
            "foreclosure":  REALFORECLOSE_BASE_URL,
            "tax":          TAX_COLLECTOR_BASE_URL,
            "parcel":       PARCEL_LOOKUP_URL,
            "master":       MASTER_DATA_URL,
            "clerk_base":   HILLSCLERK_BASE_URL,
            "clerk_access": HILLSCLERK_PUBLIC_ACCESS_URL,
        },
        "court": {
            "bankruptcy_code": COURT_CODE_FLORIDA_MIDDLE_BANKRUPTCY,
            "division_prefix": TAMPA_DIVISION_PREFIX,
        },
        "accela_code": "HCFL",
    },
}


def get_county_config(county_id: str) -> dict:
    """Return config for a county, raising clearly if unknown."""
    if county_id not in COUNTY_CONFIG:
        raise ValueError(
            f"Unknown county '{county_id}'. "
            f"Supported: {list(COUNTY_CONFIG.keys())}"
        )
    return COUNTY_CONFIG[county_id]
