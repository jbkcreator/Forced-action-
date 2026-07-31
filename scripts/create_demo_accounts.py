"""Create demo subscriber accounts for sales demos.

Creates two accounts — one per county — both with is_demo=True so the feed
endpoint serves all county ZIPs without requiring locked zip_territories rows.
Safe to re-run: skips creation if email already exists.

Usage (on prod server):
    PYTHONPATH=. python scripts/create_demo_accounts.py
"""
from __future__ import annotations

import secrets
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select, text
from src.core.database import get_db_context
from src.core.models import Subscriber

DEMO_ACCOUNTS = [
    {
        "email": "demo-hillsborough@forcedaction.io",
        "name": "Demo Account — Hillsborough",
        "county_id": "hillsborough",
    },
    {
        "email": "demo-pinellas@forcedaction.io",
        "name": "Demo Account — Pinellas",
        "county_id": "pinellas",
    },
]

TIER     = "pro"
VERTICAL = "fix_flip"


def _hash_password(password: str) -> str:
    import bcrypt
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def main() -> None:
    with get_db_context() as db:
        for acct in DEMO_ACCOUNTS:
            existing = db.execute(
                select(Subscriber).where(Subscriber.email == acct["email"])
            ).scalar_one_or_none()

            if existing:
                print(f"[skip] {acct['email']} already exists (id={existing.id})")
                print(f"       feed URL: /dashboard/{existing.event_feed_uuid}")
                continue

            password = secrets.token_urlsafe(12)
            feed_uuid = str(uuid.uuid4())

            db.execute(text("""
                INSERT INTO subscribers (
                    stripe_customer_id, tier, vertical, county_id, status,
                    is_demo, founding_member, event_feed_uuid,
                    email, name, password_hash,
                    has_saved_card, auto_mode_enabled, onboarding_completed,
                    created_at, updated_at
                ) VALUES (
                    :cid, :tier, :vertical, :county, 'active',
                    true, false, :feed_uuid,
                    :email, :name, :password_hash,
                    false, false, true,
                    NOW(), NOW()
                )
            """), {
                "cid": f"demo_{acct['county_id']}",
                "tier": TIER,
                "vertical": VERTICAL,
                "county": acct["county_id"],
                "feed_uuid": feed_uuid,
                "email": acct["email"],
                "name": acct["name"],
                "password_hash": _hash_password(password),
            })

            print(f"[created] {acct['email']}")
            print(f"          county   : {acct['county_id']}")
            print(f"          password : {password}")
            print(f"          feed URL : /dashboard/{feed_uuid}")
            print()

        db.commit()


if __name__ == "__main__":
    main()
