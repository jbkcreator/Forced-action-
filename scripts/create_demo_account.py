"""Create or upsert the demo subscriber account for the sales closer.

Account: team@forcedactionleads.com
  - is_demo = true  → bypass territory restrictions, auto-redirect to /demo on login
  - is_test = true  → excluded from MRR / Vera / win-back flows
  - stripe_customer_id = 'demo_internal_forcedaction' (placeholder, no real Stripe charge)

Idempotent: if the account already exists (by stripe_customer_id), updates flags + password.

Usage:
    PYTHONPATH=. python scripts/create_demo_account.py

IMPORTANT: Record the printed password in 1Password / team password manager.
The closer uses: team@forcedactionleads.com + this password to log in.
JWT lasts 7 days — re-enter password weekly.
"""
import logging
import secrets
import sys
import uuid

from sqlalchemy import select

from src.core.database import Database
from src.core.models import Subscriber
from src.services.subscriber_auth import hash_password

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEMO_STRIPE_ID = "demo_internal_forcedaction"


def run() -> None:
    password = secrets.token_urlsafe(18)
    pw_hash = hash_password(password)
    db = Database()

    with db.session_scope() as session:
        sub = session.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == DEMO_STRIPE_ID)
        ).scalar_one_or_none()

        if sub is None:
            sub = Subscriber(
                stripe_customer_id=DEMO_STRIPE_ID,
                tier="pro",
                vertical="roofing",
                county_id="hillsborough",
                status="active",
                email="team@forcedactionleads.com",
                name="Demo Account",
                event_feed_uuid=str(uuid.uuid4()),
                password_hash=pw_hash,
                is_demo=True,
                is_test=True,
                onboarding_completed=True,
                founding_member=False,
            )
            session.add(sub)
            session.flush()
            action = "CREATED"
        else:
            sub.is_demo = True
            sub.is_test = True
            sub.status = "active"
            sub.tier = "pro"
            sub.password_hash = pw_hash
            if not sub.event_feed_uuid:
                sub.event_feed_uuid = str(uuid.uuid4())
            action = "UPDATED"

    logger.info("=" * 60)
    logger.info("DEMO ACCOUNT %s", action)
    logger.info("  Email     : %s", sub.email)
    logger.info("  Password  : %s  ← SAVE IN PASSWORD MANAGER", password)
    logger.info("  Feed UUID : %s", sub.event_feed_uuid)
    logger.info("  Login URL : /dashboard/%s/login", sub.event_feed_uuid)
    logger.info("  Demo URL  : /dashboard/%s/demo", sub.event_feed_uuid)
    logger.info("  is_demo   : %s", sub.is_demo)
    logger.info("  is_test   : %s", sub.is_test)
    logger.info("  status    : %s", sub.status)
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
    sys.exit(0)
