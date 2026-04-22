"""
Seed Stripe test account with all Phase 2B products and prices.

Creates products/prices using lookup_key for idempotency — safe to run multiple times.
After creation prints the .env block with STRIPE_TEST_PRICE_* keys populated.

Usage:
    python scripts/seed_stripe.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import stripe
from config.settings import settings

PRODUCTS = [
    # Subscriptions
    {"key": "starter_founding",     "name": "Starter (Founding)",      "amount": 4700,  "recurring": "month"},
    {"key": "starter_regular",      "name": "Starter (Regular)",        "amount": 9700,  "recurring": "month"},
    {"key": "pro_founding",         "name": "Pro (Founding)",           "amount": 9700,  "recurring": "month"},
    {"key": "pro_regular",          "name": "Pro (Regular)",            "amount": 14700, "recurring": "month"},
    {"key": "dominator_founding",   "name": "Dominator (Founding)",     "amount": 14700, "recurring": "month"},
    {"key": "dominator_regular",    "name": "Dominator (Regular)",      "amount": 19700, "recurring": "month"},
    # Wallet tiers
    {"key": "wallet_starter",       "name": "Wallet Starter",           "amount": 4900,  "recurring": "month"},
    {"key": "wallet_growth",        "name": "Wallet Growth",            "amount": 9900,  "recurring": "month"},
    {"key": "wallet_power",         "name": "Wallet Power",             "amount": 19900, "recurring": "month"},
    # One-time
    {"key": "lead_pack",            "name": "Lead Pack",                "amount": 9900,  "recurring": None},
    {"key": "hot_lead_unlock",      "name": "Hot Lead Unlock",          "amount": 15000, "recurring": None},
    # Plans
    {"key": "data_only",            "name": "Data Only",                "amount": 9700,  "recurring": "month"},
    {"key": "autopilot_lite",       "name": "AutoPilot Lite",           "amount": 29900, "recurring": "month"},
    {"key": "autopilot_pro",        "name": "AutoPilot Pro",            "amount": 49700, "recurring": "month"},
    {"key": "annual_lock",          "name": "Annual Lock",              "amount": 197000, "recurring": "year"},
    {"key": "auto_mode",            "name": "Auto Mode",                "amount": 7900,  "recurring": "month"},
    {"key": "partner",              "name": "Partner",                  "amount": 200000, "recurring": "month"},
    # Bundles (one-time)
    {"key": "bundle_weekend",       "name": "Bundle: Weekend Pack",     "amount": 1900,  "recurring": None},
    {"key": "bundle_storm",         "name": "Bundle: Storm Pack",       "amount": 3900,  "recurring": None},
    {"key": "bundle_zip_booster",   "name": "Bundle: ZIP Booster",      "amount": 2900,  "recurring": None},
    {"key": "bundle_monthly_reload", "name": "Bundle: Monthly Reload",  "amount": 8900,  "recurring": "month"},
]


def seed():
    key = settings.active_stripe_secret_key
    if not key:
        print("ERROR: STRIPE_TEST_SECRET_KEY not set in .env")
        sys.exit(1)
    stripe.api_key = key.get_secret_value()

    results = {}
    for item in PRODUCTS:
        lookup_key = f"fa_{item['key']}"
        try:
            prod = stripe.Product.create(
                name=item["name"],
                metadata={"fa_key": item["key"]},
            )
        except stripe.error.StripeError as e:
            print(f"  Product error ({item['key']}): {e}")
            continue

        price_params = {
            "unit_amount": item["amount"],
            "currency": "usd",
            "product": prod.id,
            "lookup_key": lookup_key,
            "transfer_lookup_key": True,
        }
        if item["recurring"]:
            price_params["recurring"] = {"interval": item["recurring"]}

        try:
            price = stripe.Price.create(**price_params)
            results[item["key"]] = price.id
            print(f"  OK  {item['key']:30s}  {price.id}")
        except stripe.error.StripeError as e:
            print(f"  Price error ({item['key']}): {e}")

    print("\n# Add to your .env file:")
    for key_name, price_id in results.items():
        env_key = f"STRIPE_TEST_PRICE_{key_name.upper()}"
        print(f"{env_key}={price_id}")


if __name__ == "__main__":
    seed()
