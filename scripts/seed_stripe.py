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
    # ── Phase 1 subscription plans (per spec) ────────────────────────────────
    {"key": "starter_founding",     "name": "Starter (Founding)",       "amount": 60000,  "recurring": "month"},   # $600/mo
    {"key": "starter_regular",      "name": "Starter (Regular)",        "amount": 80000,  "recurring": "month"},   # $800/mo
    {"key": "pro_founding",         "name": "Pro (Founding)",           "amount": 110000, "recurring": "month"},   # $1,100/mo
    {"key": "pro_regular",          "name": "Pro (Regular)",            "amount": 150000, "recurring": "month"},   # $1,500/mo
    {"key": "dominator_founding",   "name": "Dominator (Founding)",     "amount": 200000, "recurring": "month"},   # $2,000/mo
    {"key": "dominator_regular",    "name": "Dominator (Regular)",      "amount": 280000, "recurring": "month"},   # $2,800/mo
    # ── Phase 2B wallet tiers ────────────────────────────────────────────────
    {"key": "wallet_starter",       "name": "Wallet Starter",           "amount": 4900,   "recurring": "month"},   # $49/mo,  20 credits
    {"key": "wallet_growth",        "name": "Wallet Growth",            "amount": 9900,   "recurring": "month"},   # $99/mo,  50 credits + Auto Mode
    {"key": "wallet_power",         "name": "Wallet Power",             "amount": 19900,  "recurring": "month"},   # $199/mo, 120 credits + Auto Mode
    # ── One-time lead products ───────────────────────────────────────────────
    {"key": "lead_pack",            "name": "Lead Pack",                "amount": 9900,   "recurring": None},      # $99, 5 leads, 72hr exclusivity
    {"key": "hot_lead_unlock",      "name": "Hot Lead Unlock",          "amount": 15000,  "recurring": None},      # $150
    # ── Phase 2B subscription tiers ──────────────────────────────────────────
    {"key": "data_only",            "name": "Data-Only Save",           "amount": 9700,   "recurring": "month"},   # $97/mo, data feed only
    {"key": "autopilot_lite",       "name": "AutoPilot Lite",           "amount": 29900,  "recurring": "month"},   # $299/mo, lock holders 10+ actions/wk
    {"key": "autopilot_pro",        "name": "AutoPilot Pro",            "amount": 49700,  "recurring": "month"},   # $497/mo, Lite 30+ days high close rate
    {"key": "annual_lock",          "name": "Annual Lock",              "amount": 197000, "recurring": "year"},    # $1,970/yr ≈ $164/mo effective
    {"key": "auto_mode",            "name": "Auto Mode Add-On",         "amount": 7900,   "recurring": "month"},   # $79/mo, add-on for Starter Wallet
    {"key": "partner",              "name": "Partner",                  "amount": 200000, "recurring": "month"},   # $2,000/mo, multi-ZIP power users
    # ── Phase 2B bundles ─────────────────────────────────────────────────────
    {"key": "bundle_weekend",       "name": "Bundle: Weekend Pack",     "amount": 1900,   "recurring": None},      # $19, Fri-Sun only
    {"key": "bundle_storm",         "name": "Bundle: Storm Pack",       "amount": 3900,   "recurring": None},      # $39, NWS-activated
    {"key": "bundle_zip_booster",   "name": "Bundle: ZIP Booster",      "amount": 2900,   "recurring": None},      # $29, 10 extra leads in ZIP for 48h
    {"key": "bundle_monthly_reload","name": "Bundle: Monthly Reload",   "amount": 8900,   "recurring": "month"},   # $89/mo, 30 recurring credits
]


def _fmt_price(amount_cents: int, recurring: str | None) -> str:
    """Human-readable price label: '$1,970/yr', '$49/mo', '$99'."""
    dollars = amount_cents / 100
    if dollars == int(dollars):
        amount_str = f"${int(dollars):,}"
    else:
        amount_str = f"${dollars:,.2f}"
    if recurring == "year":
        return f"{amount_str}/yr"
    if recurring == "month":
        return f"{amount_str}/mo"
    return amount_str


def seed():
    # Explicitly use the TEST key — never fall back to live, regardless of
    # STRIPE_TEST_MODE. This script creates test-mode objects only.
    key = settings.stripe_test_secret_key
    if not key:
        print("ERROR: STRIPE_TEST_SECRET_KEY not set in .env / /etc/forced-action/env")
        sys.exit(1)

    key_value = key.get_secret_value()
    if not key_value.startswith("sk_test_"):
        print(
            f"ERROR: STRIPE_TEST_SECRET_KEY does not start with 'sk_test_' "
            f"(got {key_value[:8]}…). Refusing to run — this script must never "
            f"hit the live account."
        )
        sys.exit(1)

    stripe.api_key = key_value
    print(f"Stripe mode:  TEST  (key {key_value[:12]}…)")
    print(f"Creating {len(PRODUCTS)} products + prices in the TEST account.\n")

    results = {}
    for item in PRODUCTS:
        lookup_key = f"fa_{item['key']}"
        price_label = _fmt_price(item["amount"], item["recurring"])

        try:
            prod = stripe.Product.create(
                name=item["name"],
                metadata={"fa_key": item["key"]},
            )
        except stripe.error.StripeError as e:
            print(f"  FAIL  {item['key']:30s}  {price_label:>12s}  product error: {e}")
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
            results[item["key"]] = {"price_id": price.id, "label": price_label}
            print(f"  OK    {item['key']:30s}  {price_label:>12s}  {price.id}")
        except stripe.error.StripeError as e:
            print(f"  FAIL  {item['key']:30s}  {price_label:>12s}  price error: {e}")

    print(f"\nCreated {len(results)} of {len(PRODUCTS)} price objects.\n")
    print("# ─── Add to your .env file ───────────────────────────────────────────")
    print("# (comments precede the KEY=value line — inline '# ...' comments are")
    print("#  NOT stripped by pydantic-settings and become part of the value.)")
    print()
    for key_name, meta in results.items():
        env_key = f"STRIPE_TEST_PRICE_{key_name.upper()}"
        print(f"# {meta['label']}")
        print(f"{env_key}={meta['price_id']}")


if __name__ == "__main__":
    seed()
