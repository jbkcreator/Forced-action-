"""Create three test subscribers for Auto Mode end-to-end testing.

One Starter (no add-on — should hit paywall), one Growth, one Power.
Creates a real Stripe (test-mode) customer for each so the checkout endpoint
passes its `no_stripe_customer` guard.

Run:
    .\\.venv\\Scripts\\python.exe -m scripts.seed_auto_mode_test_users
"""

import uuid

import stripe

from config.settings import settings
from src.core.database import db as _db
from src.core.models import Subscriber, WalletBalance


stripe.api_key = settings.active_stripe_secret_key.get_secret_value()

scenarios = [
    ("starter_no_addon@test.com", "starter_wallet"),
    ("growth_user@test.com",      "growth"),
    ("power_user@test.com",       "power"),
]


def main() -> None:
    s = _db.get_session()
    try:
        print(f"\nStripe mode: {'TEST' if settings.stripe_test_mode else 'LIVE'}")
        print("-" * 80)
        for email, wallet_tier in scenarios:
            cust = stripe.Customer.create(email=email, name=email.split("@")[0])
            feed = str(uuid.uuid4())
            sub = Subscriber(
                stripe_customer_id=cust.id,
                tier="starter",
                vertical="roofing",
                county_id="hillsborough",
                status="active",
                email=email,
                event_feed_uuid=feed,
                auto_mode_enabled=False,
            )
            s.add(sub)
            s.flush()
            s.add(WalletBalance(
                subscriber_id=sub.id,
                wallet_tier=wallet_tier,
                credits_remaining=0,
            ))
            s.commit()
            print(f"  {email:<32}  feed={feed}")
            print(f"  {'':32}  customer={cust.id}  wallet={wallet_tier}  sub_id={sub.id}")
            print()
    finally:
        s.close()


if __name__ == "__main__":
    main()
