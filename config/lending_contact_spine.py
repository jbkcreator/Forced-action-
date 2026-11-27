"""R1 1.1 Unified Contact Spine: one canonical contact per person (docs/lending/contact-spine.md).

Records that share any merge key (a non-landline phone, an email, a normalized LLC name, a GHL contact id or a
BatchDialer contact id) are one contact. Read by ``src/lending/spine/``.
"""
from __future__ import annotations

PHONE, EMAIL, ENTITY, GHL_CONTACT, BATCHDIALER_CONTACT = "phone", "email", "entity", "ghl_contact", "batchdialer_contact"
IDENTIFIER_KINDS: tuple[str, ...] = (PHONE, EMAIL, ENTITY, GHL_CONTACT, BATCHDIALER_CONTACT)

SYSTEMS: tuple[str, ...] = ("ghl", "batchdialer", "instantly")
# client 27 Nov: slot only, no connector (HubSpot and Salesmsg are Backflip systems and are excluded completely)
FUTURE_SYSTEMS: tuple[str, ...] = ("gmail", "hunter")

# Identity sources, best display name first (a loan file's borrower name beats a dialer record's).
SOURCE_NAMES: tuple[str, ...] = (
    "loan_files", "lendingflow_leads", "web_leads", "after_hours_conversations", "dialer_load_records",
    "instantly_replies", "sunbiz_llc_contacts", "call_dispositions", "contacts",
)
NAME_RANK: dict[str, int] = {name: rank for rank, name in enumerate(SOURCE_NAMES)}

MIN_ENTITY_CHARS = 3
GHL_HISTORY_DAYS = 365
TIMELINE_LIMIT = 50
CARD_MATCH_LIMIT = 5
