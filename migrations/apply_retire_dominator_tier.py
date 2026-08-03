"""
Retire dominator tier.

1. Migrate any subscriber with tier='dominator' to 'pro' (only test accounts
   exist pre-launch; no paying customers affected).
2. Drop and recreate check_subscriber_tier on subscribers without dominator.
3. Drop and recreate check_founding_tier on founding_subscriber_counts without
   dominator.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text
from config.settings import get_settings
from sqlalchemy import create_engine

def run() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        migrated = conn.execute(
            text("UPDATE subscribers SET tier = 'pro' WHERE tier = 'dominator'")
        ).rowcount
        if migrated:
            print(f"Migrated {migrated} dominator subscriber(s) to pro.")

        conn.execute(text("ALTER TABLE subscribers DROP CONSTRAINT IF EXISTS check_subscriber_tier"))
        conn.execute(text(
            "ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_tier "
            "CHECK (tier IN ('free','starter','pro','data_only','autopilot_lite',"
            "'autopilot_pro','partner','annual_lock','founder'))"
        ))

        conn.execute(text("ALTER TABLE founding_subscriber_counts DROP CONSTRAINT IF EXISTS check_founding_tier"))
        conn.execute(text(
            "ALTER TABLE founding_subscriber_counts ADD CONSTRAINT check_founding_tier "
            "CHECK (tier IN ('starter','pro','founder'))"
        ))

        print("Constraints updated. dominator tier retired.")

if __name__ == "__main__":
    run()
