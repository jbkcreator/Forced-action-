"""Go Live: source lists -> four ranked launch queues, dialed in strict order.

Dial order follows the Launch Protocol Runbook (Principle 1, "Dial Order Flipped"), which
overrides the Oct 4 email's "Partners only when ranks 1 to 3 are empty" (open Q-8, Josh):
  1. Partners = List 4 brokers/LOs: dialed first; partner script only, never pitched or booked
     as borrowers.
  1b. Wholesalers = the wholesaler lists (Sunbiz, Hunter): their own BatchDialer campaign
     ("Create the Wholesalers campaign", client, Oct 2026); dialed right after Partners, same
     partner script, never booked as borrowers.
  2. Transaction ready = Lists 2, 9, 6 (List 2 cash buyers moved out of nurture, Oct 4 §2).
  3. Builders = Lists 3, 7.
  4. Verified maturity = Lists 1, 5, 8: the 525-record FL maturity list, filler only,
     touched when the queues above are cleared.
The relative order of 2 and 3 is unchanged from Oct 4 (the Runbook names only first and
last). LendingFlow inbound is not a dialer queue: it routes to GHL and Josh (Principle 2).
Each queue is its own BatchDialer campaign; MAX_LINES is the Principle 4 line rule.
"""
from __future__ import annotations

VERIFIED_MATURITY = "verified_maturity"
TRANSACTION_READY = "transaction_ready"
BUILDERS = "builders"
PARTNERS = "partners"
WHOLESALERS = "wholesalers"
NURTURE = "nurture"

SOURCE_TAG_QUEUES: dict[str, str] = {
    "list_1": VERIFIED_MATURITY, "list_5": VERIFIED_MATURITY, "list_8": VERIFIED_MATURITY,
    "list_2": TRANSACTION_READY, "list_9": TRANSACTION_READY, "list_6": TRANSACTION_READY,
    "list_3": BUILDERS, "list_7": BUILDERS,
    "list_4": PARTNERS,
    "wholesaler_sunbiz": WHOLESALERS, "wholesaler_hunter": WHOLESALERS,
}
# No source tag is nurture-only any more (List 2 and List 4 both moved to real
# ranked queues above); kept as a mechanism for a future list that genuinely has
# no rank of its own.
NURTURE_ONLY_TAGS: frozenset[str] = frozenset()
LAUNCH_QUEUES = frozenset(SOURCE_TAG_QUEUES.values())
# Partners (List 4) are dialed, but never pitched or booked as borrowers (Oct 4 §2:
# "partner script only, never pitched as borrowers").
NEVER_BOOKABLE_QUEUES = frozenset({PARTNERS, WHOLESALERS})

# Strict dial order: a queue is dialed only when every queue before it is empty.
DIAL_ORDER: tuple[str, ...] = (PARTNERS, WHOLESALERS, TRANSACTION_READY, BUILDERS, VERIFIED_MATURITY)

# Runbook Principle 4: max 3 lines per seat on partner, wholesaler/broker and landline
# numbers; 5 lines only on the cold maturity queue. Set by hand per BatchDialer campaign.
# The caps are the rule; MAX_LINES is the chosen setting, checked against them.
LINE_CAP = 3
COLD_MATURITY_LINE_CAP = 5
MAX_LINES: dict[str, int] = {
    PARTNERS: 3, WHOLESALERS: 3, TRANSACTION_READY: 3, BUILDERS: 3, VERIFIED_MATURITY: 5,
}


def validate_queue_config() -> None:
    if len(DIAL_ORDER) != len(set(DIAL_ORDER)) or set(DIAL_ORDER) != LAUNCH_QUEUES:
        raise ValueError("DIAL_ORDER must list every launch queue exactly once")
    if set(MAX_LINES) != LAUNCH_QUEUES:
        raise ValueError("MAX_LINES must cover exactly the launch queues")
    for queue, lines in MAX_LINES.items():
        cap = COLD_MATURITY_LINE_CAP if queue == VERIFIED_MATURITY else LINE_CAP
        if not 1 <= lines <= cap:
            raise ValueError(f"{queue} may run at most {cap} lines")
    if NURTURE_ONLY_TAGS & set(SOURCE_TAG_QUEUES):
        raise ValueError("a nurture-only list cannot also map to a queue")
    if NEVER_BOOKABLE_QUEUES - LAUNCH_QUEUES:
        raise ValueError("NEVER_BOOKABLE_QUEUES must be a subset of the launch queues")
