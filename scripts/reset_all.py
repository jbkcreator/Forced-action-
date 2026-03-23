"""
Full reset — deletes all Stripe test customers and clears subscription DB tables.
Delete after use.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import stripe
from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import Subscriber, ZipTerritory, FoundingSubscriberCount

settings = get_settings()
stripe.api_key = settings.stripe_secret_key.get_secret_value()

# ── 1. Delete all Stripe test customers ───────────────────────────────────────
print("Deleting Stripe customers...")
deleted_stripe = 0
has_more = True
starting_after = None

while has_more:
    params = {"limit": 100}
    if starting_after:
        params["starting_after"] = starting_after
    response = stripe.Customer.list(**params)
    customers = response["data"]
    has_more = response["has_more"]
    if customers:
        starting_after = customers[-1]["id"]

    for customer in customers:
        stripe.Customer.delete(customer["id"])
        print(f"  Deleted Stripe customer: {customer['id']} ({customer.get('email', '—')})")
        deleted_stripe += 1

print(f"Stripe: deleted {deleted_stripe} customers\n")

# ── 2. Reset DB ────────────────────────────────────────────────────────────────
print("Resetting DB...")
with get_db_context() as db:
    zips   = db.query(ZipTerritory).delete()
    subs   = db.query(Subscriber).delete()
    counts = db.query(FoundingSubscriberCount).delete()
    print(f"  Deleted: {subs} subscribers, {zips} zip territories, {counts} founding counts")

print("\nDone — clean slate. Go through checkout from scratch.")
