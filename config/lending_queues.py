"""Go Live: nine source lists -> three launch queues (brief section on lead supply).

Verified maturity = Lists 1, 5, 8. Transaction ready = Lists 9, 6. Builders = Lists 3, 7.
Lists 2 and 4 (cash buyers, brokers/LOs) are dialed for data and nurture in their
own queue but are never bookable until December (brief 2.5).
"""
from __future__ import annotations

VERIFIED_MATURITY = "verified_maturity"
TRANSACTION_READY = "transaction_ready"
BUILDERS = "builders"
NURTURE = "nurture"

SOURCE_TAG_QUEUES: dict[str, str] = {
    "list_1": VERIFIED_MATURITY, "list_5": VERIFIED_MATURITY, "list_8": VERIFIED_MATURITY,
    "list_9": TRANSACTION_READY, "list_6": TRANSACTION_READY,
    "list_3": BUILDERS, "list_7": BUILDERS,
}
NURTURE_ONLY_TAGS = frozenset({"list_2", "list_4"})
LAUNCH_QUEUES = frozenset(SOURCE_TAG_QUEUES.values())

# Share of dial time per launch queue (nurture dials fill the remainder).
DIAL_SHARE: dict[str, float] = {VERIFIED_MATURITY: 0.5, TRANSACTION_READY: 0.3, BUILDERS: 0.2}


def validate_queue_config() -> None:
    if set(DIAL_SHARE) != LAUNCH_QUEUES:
        raise ValueError("DIAL_SHARE must cover exactly the launch queues")
    if abs(sum(DIAL_SHARE.values()) - 1.0) > 1e-9:
        raise ValueError("DIAL_SHARE must sum to 1")
    if NURTURE_ONLY_TAGS & set(SOURCE_TAG_QUEUES):
        raise ValueError("a nurture-only list cannot also map to a queue")
