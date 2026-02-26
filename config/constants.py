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

# Temporary download directory for browser-use (cross-platform)
TEMP_DOWNLOADS_DIR = Path(tempfile.gettempdir())

# =============================================================================
# PUBLIC RECORDS URLS - Hillsborough County Clerk
# =============================================================================

# Base domain
HILLSCLERK_BASE_URL = "https://publicrec.hillsclerk.com"
HILLSCLERK_PUBLIC_ACCESS_URL = "https://publicaccess.hillsclerk.com/oripublicaccess/"

# Daily filings
CIVIL_FILINGS_URL = f"{HILLSCLERK_BASE_URL}/Civil/dailyfilings/"
PROBATE_FILINGS_URL = f"{HILLSCLERK_BASE_URL}/Probate/dailyfilings/"

# =============================================================================
# ACCELA PORTAL URLS - Hillsborough County
# =============================================================================

ACCELA_BASE_URL = "https://aca-prod.accela.com/HCFL"
PERMIT_SEARCH_URL = f"{ACCELA_BASE_URL}/Cap/CapHome.aspx?module=Building"
VIOLATION_SEARCH_URL = (
    f"{ACCELA_BASE_URL}/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement"
)

# =============================================================================
# FORECLOSURE & TAX URLS
# =============================================================================

REALFORECLOSE_BASE_URL = "https://www.hillsborough.realforeclose.com/index.cfm"
TAX_COLLECTOR_BASE_URL = "https://hillsborough.county-taxes.com"
PARCEL_LOOKUP_URL = f"{TAX_COLLECTOR_BASE_URL}/public/real_estate/parcels"
MASTER_DATA_URL = "https://downloads.hcpafl.org/"

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

REQUEST_TIMEOUT_DEFAULT = 30  # seconds
REQUEST_TIMEOUT_LONG = 60  # seconds
REQUEST_TIMEOUT_EXTENDED = 180  # seconds

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
