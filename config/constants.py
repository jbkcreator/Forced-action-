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
RAW_DIVORCE_DIR = RAW_DATA_DIR / "divorce"
RAW_TAX_DEED_DIR = RAW_DATA_DIR / "tax_deed_auctions"
RAW_VACANT_LAND_DIR = RAW_DATA_DIR / "vacant_land"

# Temporary download directory for browser-use (cross-platform)
TEMP_DOWNLOADS_DIR = Path(tempfile.gettempdir())

# =============================================================================
# PORTAL URLS — legacy fallback constants kept for scraper backwards-compat.
# All URLs now come from the DB via get_county_config(county_id)["sources"].
# These are empty strings; scrapers fall through to the DB value in practice.
# =============================================================================

HILLSCLERK_BASE_URL          = ""
HILLSCLERK_PUBLIC_ACCESS_URL = ""
CIVIL_FILINGS_URL            = ""
PROBATE_FILINGS_URL          = ""
ACCELA_BASE_URL              = ""
PERMIT_SEARCH_URL            = ""
VIOLATION_SEARCH_URL         = ""
REALFORECLOSE_BASE_URL       = ""
TAX_COLLECTOR_BASE_URL       = ""
PARCEL_LOOKUP_URL            = ""
MASTER_DATA_URL              = ""

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

DIVORCE_CASE_PATTERNS = [
    "DR Dissolution of Marriage",
    "DR Dissolution",
    "Dissolution of Marriage",
    "Domestic Relations",
    "Family Law",
]

PROBATE_CASE_PATTERNS = [
    "Probate",
    "PR ",
    "Estate",
    "Guardianship",
    "Trust Administration",
]

# Keywords used to select case types in the Pinellas courtrecords #caseTypesList
# filter during the merged scrape+detail search (scrape_pinellas_civil_with_detail).
# Each list is matched (case-insensitively) against the live dropdown options.
PINELLAS_CASE_TYPE_KEYWORDS: dict[str, list[str]] = {
    "eviction": ["eviction"],
    "probate": ["estate", "guardianship"],
    "divorce": ["dissolution"],
}

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
# CONSENT & COMPLIANCE CONSTANTS (TCPA / T&C)
# =============================================================================

TERMS_VERSION = "2026.06"
PRIVACY_VERSION = "2026.06"
TCPA_CONSENT_VERSION = "2026.06.1"

TCPA_CONSENT_TEXT = (
    "I agree to receive recurring automated marketing calls and text messages "
    "from Forced Action at the phone number provided. Consent is not a "
    "condition of purchase. Msg & data rates may apply. "
    "Reply STOP to opt out."
)

CONSENT_SOURCE_FLOWS = frozenset({
    "waitlist",
    "signup",
    "checkout",
    "county_launch",
    "free_signup",
})

CONSENT_SCOPES = frozenset({
    "marketing",
    "waitlist_notify",
    "lead_alerts",
})


TIER_DISPLAY = {
    "starter": {
        "label": "Starter — 1 ZIP",
        "zip_limit": 1,
        "features": [
            "1 ZIP territory",
            "Daily lead feed",
            "CDS scoring",
            "All event types",
            "Rate locked forever",
        ],
    },
    "pro": {
        "label": "Pro — 3 ZIPs",
        "zip_limit": 3,
        "features": [
            "3 ZIP territories",
            "Priority lead delivery",
            "Skip-traced phone numbers",
            "All event types",
            "Rate locked forever",
        ],
    },
    "dominator": {
        "label": "Dominator — 10 ZIPs",
        "zip_limit": 10,
        "features": [
            "Unlimited ZIPs in county",
            "First-access lead delivery",
            "Skip-traced phone numbers",
            "Dedicated account manager",
            "Rate locked forever",
        ],
    },
    "annual_lock": {
        "label": "Annual — 2 Months Free",
        "zip_limit": 1,
        "features": [
            "1 ZIP territory",
            "Daily lead feed",
            "CDS scoring",
            "All event types",
            "Rate locked for a full year",
        ],
    },
    # Placeholder copy — product to supply final founder bullets before launch.
    # Do not promise unbuilt surfaces (deal-intake / Lifecycle SLA) until they ship.
    "founder": {
        "label": "Founder — 10 ZIPs",
        "zip_limit": 10,
        "features": [
            "10 ZIP territories",
            "Everything in Pro",
            "Founder-tier priority access",
            "Rate locked forever",
        ],
    },
}

