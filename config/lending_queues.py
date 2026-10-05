"""Go Live: nine source lists -> four ranked launch queues (Josh, Oct 4 email §2).

Rank 1 Verified maturity = Lists 1, 5, 8 (50% dial share).
Rank 2 Cash buyers and Transaction ready = Lists 2, 9, 6 (25%) — List 2 moved out of
  nurture here: "cash buyers move out of Nurture into rank 2, because delayed
  financing is a real borrower conversation."
Rank 3 Builders = Lists 3, 7 (20%).
Rank 4 Partners = List 4, brokers and LOs (5%), "only when ranks 1 to 3 are empty" —
  partner script only, dialed as a real queue but never pitched or booked as borrowers.
  The "only when empty" sequencing is a dial-time ordering rule, not a share; nothing
  currently paces dials against DIAL_SHARE (no consumer exists yet), so this is the
  declared target split for whenever that pacing is built, not an enforced one.
"""
from __future__ import annotations

VERIFIED_MATURITY = "verified_maturity"
TRANSACTION_READY = "transaction_ready"
BUILDERS = "builders"
PARTNERS = "partners"
NURTURE = "nurture"

SOURCE_TAG_QUEUES: dict[str, str] = {
    "list_1": VERIFIED_MATURITY, "list_5": VERIFIED_MATURITY, "list_8": VERIFIED_MATURITY,
    "list_2": TRANSACTION_READY, "list_9": TRANSACTION_READY, "list_6": TRANSACTION_READY,
    "list_3": BUILDERS, "list_7": BUILDERS,
    "list_4": PARTNERS,
}
# No source tag is nurture-only any more (List 2 and List 4 both moved to real
# ranked queues above); kept as a mechanism for a future list that genuinely has
# no rank of its own.
NURTURE_ONLY_TAGS: frozenset[str] = frozenset()
LAUNCH_QUEUES = frozenset(SOURCE_TAG_QUEUES.values())
# Partners (List 4) are dialed, but never pitched or booked as borrowers (Oct 4 §2:
# "partner script only, never pitched as borrowers").
NEVER_BOOKABLE_QUEUES = frozenset({PARTNERS})

# Share of dial time per launch queue (see module docstring: declared target, not
# yet enforced by any pacing engine).
DIAL_SHARE: dict[str, float] = {
    VERIFIED_MATURITY: 0.5, TRANSACTION_READY: 0.25, BUILDERS: 0.2, PARTNERS: 0.05,
}


def validate_queue_config() -> None:
    if set(DIAL_SHARE) != LAUNCH_QUEUES:
        raise ValueError("DIAL_SHARE must cover exactly the launch queues")
    if abs(sum(DIAL_SHARE.values()) - 1.0) > 1e-9:
        raise ValueError("DIAL_SHARE must sum to 1")
    if NURTURE_ONLY_TAGS & set(SOURCE_TAG_QUEUES):
        raise ValueError("a nurture-only list cannot also map to a queue")
    if NEVER_BOOKABLE_QUEUES - LAUNCH_QUEUES:
        raise ValueError("NEVER_BOOKABLE_QUEUES must be a subset of the launch queues")
