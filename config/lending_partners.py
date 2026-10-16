"""Partner Door (P-1): vocabularies and limits for partner records and partner-submitted files."""
from __future__ import annotations

from typing import Optional

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
# Upper bound of each closed band, so a band wholly below a lender's floor is a decline, not "possible".
CREDIT_BAND_MAX_FICO: dict[str, Optional[int]] = {
    "Below 640": 639,
    "640-679": 679,
    "680-719": 719,
    "720-759": 759,
    "760+": None,
}
TIMELINES = ("Under 2 weeks", "2-4 weeks", "1-3 months", "3+ months", "Not sure")

# Shared-file statuses. R-1 moves files between them; P-1 only sets received / held.
FILE_STATUSES = ("received", "held", "routed", "declined", "approved", "funded")
# What the partner sees on their dashboard for each status.
STATUS_LABELS = {
    "received": "In review",
    "held": "In review",
    "routed": "With a lender",
    "declined": "Declined by a lender, looking at next option",
    "approved": "Approved",
    "funded": "Funded",
}
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
# A file whose Slack card, partner confirmation, GHL card or partner letter failed is retried by the sweep for this long.
NOTICE_RETRY_HOURS = 24

# GHL: a partner file becomes a borrower contact + an opportunity in the "Partner - New" stage, like a LendingFlow lead.
# The borrower gave no consent of their own, so the contact never gets an SMS-trigger tag.
GHL_SOURCE_PARTNER = "Partner Door"
GHL_TAG_PARTNER_FILE = "partner-file"
GHL_TAG_NO_BORROWER_CONSENT = "partner-no-borrower-consent"

# Pre-qual letter to the partner (the borrower is never contacted). Disclaimer wording as T-07.
PARTNER_LETTER_SUBJECT = "Pre-qualification estimate for {place} (ref #{file_id})"
PARTNER_LETTER_HTML = (
    "<p>Attached is the non-binding pre-qualification estimate for this file. It is not a commitment to lend or "
    "an offer of specific terms; all figures are subject to lender confirmation. Please share it with the borrower "
    "as you see fit.</p>"
    "<p>Questions? Call or text {callback}.</p>"
    "<p>{signer}</p>"
)
