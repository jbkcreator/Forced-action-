"""P2.4 closing blocker card and document chase (src/lending/closing/).

Stages are GHL opportunity stage names, lower-cased (``lending.ghl_opportunity_stage.stage_key``). Team-lead defaults
(Nov 13): Josh confirms the stages, the owner per stage and the message wording before the flags go on.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import time, timedelta

# Deals on the 7 AM card. New lead / Booked / Held / No show are leads (other alerts cover them).
ACTIVE_STAGE_KEYS = frozenset({"application", "ready to fund", "submitted to lender"})
# A document chase on a deal in one of these stops.
CLOSED_STAGE_KEYS = frozenset({"funded", "paid", "lost", "nurture"})  # "Paid": after Funded (R1 0.1)
STUCK_AFTER = timedelta(days=3)  # brief: "stuck for more than 3 days"
# GHL's stage webhook body carries no event time. A stage event repeated (same deal, same stage) within this long
# of its first delivery is a redelivery and never moves the deal; a later repeat is a real move (a revived deal).
REDELIVERY_WINDOW = timedelta(hours=1)

# Who holds a stuck deal up when no documents are missing. Missing documents always mean borrower (or partner).
STAGE_OWNER = {"application": "borrower", "ready to fund": "title", "submitted to lender": "lender"}
OWNER_LABELS = {"borrower": "borrower", "partner": "partner", "lender": "lender", "title": "closing (title)"}

CARD_TIME = time(7, 0)  # ET; posted once a day between these two times
CARD_LAST_TIME = time(12, 0)

CHASE_DAYS = (0, 2, 5, 8)  # ET calendar days after acceptance: the request, then the brief's Day 2 / 5 / 8
TOKEN_TTL = timedelta(days=30)
MAX_UPLOADS_PER_ITEM = 5  # a leaked link cannot fill the disk: newer copies are allowed up to this
MAX_UPLOADS_PER_CHASE = 40
UPLOAD_LINK_PATH = "/api/lending/docs/{token}"
# R1 0.2: an admin's link to view or download one stored document, and a staff API token's life.
DOC_VIEW_LINK_TTL = timedelta(minutes=15)
DOC_VIEW_LINK_PATH = "/api/lending/docs/view/{token}"
STAFF_TOKEN_TTL = timedelta(days=90)
REQUESTED_BY = "doc_chase"  # pending_actions.requested_by for every P2.4 draft


@dataclass(frozen=True)
class ChecklistItem:
    code: str
    label: str


# Josh's Term Sheet Intake Checklist (Nov 2), "what can wait until after the term sheet". Central Lending only:
# Backflip stays blocked until Josh approves it in writing.
# R1 1.7: pricing_sheet is a new item type (client Q25 for expiry); required by no lender yet.
# Secondary lenders (corevest / rcn_capital / temple_view / abl) start as a copy of Central Lending's 6 items,
# marked placeholder until the client answers Q24 (required documents per secondary lender).
_PLACEHOLDER_SECONDARY: tuple[ChecklistItem, ...] = (
    ChecklistItem("purchase_agreement", "Signed purchase agreement [placeholder]"),
    ChecklistItem("valuation", "Valuation or appraisal [placeholder]"),
    ChecklistItem("scope_of_work", "Scope of work and budget [placeholder]"),
    ChecklistItem("entity_docs", "LLC documents [placeholder]"),
    ChecklistItem("government_id", "Photo ID for each guarantor [placeholder]"),
    ChecklistItem("bank_statements", "Last 30 days of bank statements (down payment plus reserves) [placeholder]"),
)
LENDER_CHECKLISTS: dict[str, tuple[ChecklistItem, ...]] = {
    "central_lending": (
        ChecklistItem("purchase_agreement", "Signed purchase agreement"),
        ChecklistItem("valuation", "Valuation or appraisal"),
        ChecklistItem("scope_of_work", "Scope of work and budget"),
        ChecklistItem("entity_docs", "LLC documents"),
        ChecklistItem("government_id", "Photo ID for each guarantor"),
        ChecklistItem("bank_statements", "Last 30 days of bank statements (down payment plus reserves)"),
    ),
    "corevest": _PLACEHOLDER_SECONDARY,
    "rcn_capital": _PLACEHOLDER_SECONDARY,
    "temple_view": _PLACEHOLDER_SECONDARY,
    "abl": _PLACEHOLDER_SECONDARY,
}

# R1 1.7: pricing_sheet item type. Expiry (max age in days) set via LENDING_PRICING_SHEET_MAX_AGE_DAYS.
# No lender requires it yet; the validator blocks any pricing_sheet with no readable date.
PRICING_SHEET_ITEM = ChecklistItem("pricing_sheet", "Pricing sheet")

# P2.2 instant-quote condition codes that add a document.
CONDITION_ITEMS: dict[str, ChecklistItem] = {
    "INTEREST_RESERVE_FULL_TERM": ChecklistItem("interest_reserve_funds",
                                                "Proof of funds for the full-term interest reserve"),
}


@dataclass(frozen=True)
class ChaseTemplate:
    version_sms: str
    version_email: str
    sms: str
    email_subject: str
    email_body: str


# Draft wording (no figures, no approval language); Josh approves it before LENDING_DOC_CHASE_ENABLED goes on.
# Placeholders: {first_name}, {link}, {missing} (comma-separated labels), {count}.
_HELP = "Questions? Call (656) 221-1100."
TEMPLATES: dict[str, ChaseTemplate] = {
    "request": ChaseTemplate(
        "DOCS-REQUEST-SMS-v1", "DOCS-REQUEST-EMAIL-v1",
        "Hi {first_name}, this is Next Deal Lending. Thanks for moving forward. Please upload the documents for your "
        "loan file here: {link} Reply STOP to opt out.",
        "Documents for your loan file",
        "Hi {first_name},\n\nThanks for moving forward with Next Deal Lending. To keep your file on track, please "
        "upload these documents: {missing}.\n\nSecure upload link: {link}\nThis link is just for you and expires in "
        "30 days.\n\n" + _HELP),
    "d2": ChaseTemplate(
        "DOCS-CHASE-D2-SMS-v1", "DOCS-CHASE-D2-EMAIL-v1",
        "Hi {first_name}, quick reminder from Next Deal Lending: we're still waiting on {count} document(s) for your "
        "file. Upload here: {link} Reply STOP to opt out.",
        "Reminder: documents still needed",
        "Hi {first_name},\n\nWe're still missing: {missing}.\n\nUpload here: {link}\n\n" + _HELP),
    "d5": ChaseTemplate(
        "DOCS-CHASE-D5-SMS-v1", "DOCS-CHASE-D5-EMAIL-v1",
        "Hi {first_name}, your loan file is on hold until we receive {count} document(s). Upload here: {link} or "
        "call (656) 221-1100 if you need help. Reply STOP to opt out.",
        "Your file is waiting on documents",
        "Hi {first_name},\n\nYour file can't move forward until these arrive: {missing}.\n\nUpload here: {link}\n\n"
        + _HELP),
    "d8": ChaseTemplate(
        "DOCS-CHASE-D8-SMS-v1", "DOCS-CHASE-D8-EMAIL-v1",
        "Hi {first_name}, last reminder from Next Deal Lending: we still need {count} document(s) to keep your file "
        "moving. Upload here: {link} Reply STOP to opt out.",
        "Final reminder: documents needed",
        "Hi {first_name},\n\nWe still need: {missing}.\n\nUpload here: {link}\n\nReply to this email if something is "
        "holding you up. " + _HELP),
    "blocker_borrower": ChaseTemplate(
        "BLOCKER-BORROWER-SMS-v1", "BLOCKER-BORROWER-EMAIL-v1",
        "Hi {first_name}, checking in from Next Deal Lending on your loan file. Is there anything we can help with to "
        "keep it moving? Reply STOP to opt out.",
        "Checking in on your loan file",
        "Hi {first_name},\n\nChecking in on your loan file with Next Deal Lending. Is there anything we can help with "
        "to keep it moving?\n\n" + _HELP),
}

# Lender and title follow-ups are never sent by Cora (no contacts held): the card shows this text for Josh to send.
LENDER_TITLE_DRAFTS = {
    "lender": "Hi, checking on the status of {name}'s file (in {stage} for {days} days). Anything you need from us?",
    "title": "Hi, checking where {name}'s closing stands (ready to fund for {days} days). Anything holding it up?",
}
