"""WP-GL-9 text-back: client-approved wording and the rules around it (values only, no logic)."""
from __future__ import annotations

from config.lending_queues import BUILDERS, NURTURE, TRANSACTION_READY, VERIFIED_MATURITY

# Statuses that occupy a contact's one text slot for the Eastern day. A skipped, dry-run or
# failed event does NOT hold it, so a later call the same day can still be texted once consent
# exists. send_unknown (crash after the claim) holds it: we cannot prove nothing went out.
SLOT_HOLDING_STATUSES = ("pending", "sending", "sent", "send_unknown")
SLOT_HOLDING_SQL = "status IN (" + ", ".join(f"'{s}'" for s in SLOT_HOLDING_STATUSES) + ")"

MAX_TEXT_CHARS = 320

# Client-approved wording (questionnaire F2: "Option 1 for Verified maturity, Option 2 for Transaction
# ready and Builders, Option 3 for Nurture and anything without an address"). Verbatim except the
# greeting, which is "Hi <first name>" or just "Hi" when the borrower has no usable first name.
TEMPLATES: dict[str, str] = {
    "maturity": (
        "{greeting}, it's {caller} with Next Deal Lending. Sorry I missed you. "
        "I was calling about the loan on {property}. "
        "Call or text me back at {number} when it suits you. Reply STOP to opt out."
    ),
    "deal_drop": (
        "{greeting}, {caller} from Next Deal Lending here. Just tried you about {property}. "
        "We help investors in {county} fund their next deal. Text back if you'd like to chat. "
        "Reply STOP to opt out."
    ),
    "general": (
        "{greeting}, this is {caller} with Next Deal Lending. Sorry we missed each other. "
        "Reply here or call {number} whenever works. Reply STOP to opt out."
    ),
}
GENERAL = "general"
QUEUE_TEMPLATES: dict[str, str] = {
    VERIFIED_MATURITY: "maturity",
    TRANSACTION_READY: "deal_drop",
    BUILDERS: "deal_drop",
    NURTURE: "general",
}
TEMPLATE_NEEDS: dict[str, tuple[str, ...]] = {  # fields a template cannot be sent without
    "maturity": ("property_address",),
    "deal_drop": ("property_address", "county"),
    "general": (),
}

# Per-field caps keep the worst case under MAX_TEXT_CHARS so the STOP language is never cut.
CAPS = {"first_name": 20, "caller": 30, "property": 50, "county": 30}
FALLBACK_CALLER = "our team"
ENTITY_TOKENS = frozenset({
    "llc", "inc", "corp", "corporation", "co", "company", "ltd", "lp", "llp", "pllc", "trust",
    "holdings", "properties", "investments", "capital", "group", "partners", "enterprises", "realty",
})
