"""
Stage 10 — Seed initial message_variant_tests rows.

Registers the 3-variant test configuration for each Lifecycle sequence that
participates in Stage 10 mutation. Safe to re-run (idempotent via
get_or_create_test).

Usage:
    python scripts/seed_stage10_sequences.py
    python scripts/seed_stage10_sequences.py --dry-run
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)


SEQUENCES = [
    {
        "sequence_name": "wallet_push_v1",
        "segment": "wallet_eligible",
        "traffic_pct": 10,
        "slot_a_body": (
            "Your credits are running low. Grab the Starter Wallet — "
            "20 leads/mo for $49. Reply WALLET to activate."
        ),
        "slot_b_body": (
            "3 distress signals dropped in your ZIP this week. "
            "Unlock them now with a $49 wallet — reply YES to start."
        ),
        "slot_c_body": (
            "Your free leads expire soon. Lock in unlimited access for $49/mo "
            "before the window closes. Reply LOCK to secure your spot."
        ),
    },
    {
        "sequence_name": "fomo_v1",
        "segment": "high_intent",
        "traffic_pct": 10,
        "slot_a_body": (
            "A competitor just made an offer on a lead in your ZIP. "
            "Move now — reply VIEW to see it."
        ),
        "slot_b_body": (
            "New distress signal in your territory — foreclosure filed today. "
            "First buyer who calls wins. Reply LEADS."
        ),
        "slot_c_body": (
            "Your ZIP flagged a motivated seller. Someone else already checked it. "
            "See it before they close — reply NOW."
        ),
    },
    {
        "sequence_name": "abandonment_wave1_v1",
        "segment": "abandonment",
        "traffic_pct": 10,
        "slot_a_body": (
            "You left 3 distress leads on the table. "
            "They're still available — reply BACK to pick up where you left off."
        ),
        "slot_b_body": (
            "Your session timed out — your leads are still waiting. "
            "Come back before another buyer grabs them. Reply RESUME."
        ),
        "slot_c_body": (
            "You were one step away from your next deal. "
            "Your saved leads expire in 2 hours — reply SAVE to lock them in."
        ),
    },
    {
        "sequence_name": "retention_v1",
        "segment": "payer",
        "traffic_pct": 10,
        "slot_a_body": (
            "Your ZIP had 5 distress events this week. "
            "Here's your weekly brief — reply BRIEF for the full list."
        ),
        "slot_b_body": (
            "You've been active for {tenure} days. Your territory generated "
            "{lead_count} new signals. Keep the streak going — reply LEADS."
        ),
        "slot_c_body": (
            "Weekly check-in: {lead_count} new distress leads in your ZIP. "
            "Top signal: {top_signal}. Reply SEE to review."
        ),
    },
    {
        "sequence_name": "lock_close_v1",
        "segment": "wallet_to_lock",
        "traffic_pct": 10,
        "slot_a_body": (
            "You've unlocked {unlock_count} leads this month. "
            "Lock your ZIP for $197/mo and stop competing — reply LOCK."
        ),
        "slot_b_body": (
            "3 other buyers are eyeing your ZIP. Territory lock is $197/mo "
            "and removes the competition. Reply MINE to lock it now."
        ),
        "slot_c_body": (
            "Your deal activity qualifies you for ZIP exclusivity. "
            "$197/mo locks out competitors in {zip}. Reply LOCK to claim it."
        ),
    },
]


def seed(dry_run: bool = False) -> None:
    from src.core.database import get_db_context
    from src.services.variant_engine import get_or_create_test

    with get_db_context() as db:
        for seq in SEQUENCES:
            name = seq["sequence_name"]
            if dry_run:
                logger.info("[dry-run] would register sequence: %s", name)
                continue

            test = get_or_create_test(
                sequence_name=name,
                slot_a_body=seq["slot_a_body"],
                slot_b_body=seq["slot_b_body"],
                slot_c_body=seq["slot_c_body"],
                segment=seq.get("segment"),
                traffic_pct=seq.get("traffic_pct", 10),
                db=db,
            )
            logger.info(
                "registered sequence=%s test_id=%s status=%s",
                name, test.id, test.status,
            )


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    seed(dry_run=dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
