"""Partner Door (P-1): vocabularies and limits for partner records and partner-submitted files."""
from __future__ import annotations

from src.lending.contracts import LoanType

PARTNER_TYPES = ("wholesaler", "broker", "referral", "other")
AGREEMENT_STATUSES = ("pending", "signed", "terminated")

# Form value -> lender-box loan type. "other" is stored but not scored or pre-qualified.
PRODUCTS: dict[str, LoanType | None] = {
    "flip": LoanType.FIX_AND_FLIP,
    "ground_up": LoanType.GROUND_UP_CONSTRUCTION,
    "dscr": LoanType.DSCR_RENTAL,
    "other": None,
}
PRODUCT_LABELS = {"flip": "Fix and flip", "ground_up": "Ground-up construction", "dscr": "DSCR rental",
                  "other": "Other"}

# Credit bands shown on the form -> the band's lower bound, as the lender box reads it
# (0 means "below 640", matching evaluate_lender_fit).
CREDIT_BANDS: dict[str, int] = {
    "Below 640": 0,
    "640-679": 640,
    "680-719": 680,
    "720-759": 720,
    "760+": 760,
}
TIMELINES = ("Under 2 weeks", "2-4 weeks", "1-3 months", "3+ months", "Not sure")

# Shared-file statuses. R-1 moves files between them; P-1 only sets received / held.
FILE_STATUSES = ("received", "held", "routed", "declined", "approved", "funded")
PARTNER_NOTIFY_STATUSES = frozenset({"routed", "declined", "approved", "funded"})
SOURCE_PARTNER = "partner"
AWAITING_ROUTER_REASON = "awaiting router"

MAX_UPLOADS = 5
MAX_UPLOAD_BYTES = 10 * 1024 * 1024
UPLOAD_TYPES = {  # extension -> accepted content types
    ".pdf": {"application/pdf"},
    ".jpg": {"image/jpeg"},
    ".jpeg": {"image/jpeg"},
    ".png": {"image/png"},
    ".heic": {"image/heic", "image/heif", "application/octet-stream"},
}

MAX_MONEY = 100_000_000
TOKEN_BYTES = 24  # 192-bit link token; only its SHA-256 is stored

RATE_LIMIT_SCOPE = "lending_partner_submit"
RATE_LIMIT_PER_WINDOW = 20
RATE_LIMIT_WINDOW_SECONDS = 3600
# Sweep: a partner file still 'received' this long after it arrived lost its background run (crash/restart).
SWEEP_AFTER_MINUTES = 10
SWEEP_BATCH_SIZE = 20
