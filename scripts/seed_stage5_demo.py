"""
Stage 5 demo seeder — one-shot, idempotent.

Creates everything you need to walk through every Stage 5 UI flow:

  - 3 demo subscribers (lead + 2 referees, same county+vertical)
  - 1 demo property in their county
  - Wallet topped up to 100 credits for the lead
  - Locked ZIP per subscriber (so the team-view has shared_zips)
  - 2 confirmed referrals → triggers the team unlock
  - 5 deal_outcomes spread over the last few days → populates the proof wall
  - Extra confirmed referrals across cohorts → populates the leaderboard
  - Runs the leaderboard aggregator → writes data/leaderboards/latest.json

At the end, prints click-here URLs for every Stage 5 flow plus the SQL
checks that verify the result.

Idempotent: safe to run multiple times — looks up existing demo rows by
their well-known emails / parcel IDs and updates instead of duplicating.

Usage:
    python -m scripts.seed_stage5_demo
    python -m scripts.seed_stage5_demo --reset      # tear down + reseed
    python -m scripts.seed_stage5_demo --stripe     # ALSO create real
                                                    # Stripe test customer +
                                                    # subscription so the
                                                    # bundle / annual / upgrade
                                                    # flows work end-to-end.
                                                    # Requires STRIPE_TEST_*
                                                    # keys + autopilot_lite
                                                    # price configured.
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import (
    BundlePurchase,
    DealOutcome,
    MessageOutcome,
    PremiumPurchase,
    Property,
    ReferralEvent,
    ReferralTeam,
    Subscriber,
    WalletBalance,
    WalletTransaction,
    ZipTerritory,
)


# ── Configuration ────────────────────────────────────────────────────────────

DEMO_COUNTY = "hillsborough"
DEMO_VERTICAL = "roofing"

DEMO_LEAD = {
    "marker": "stage5-demo-lead@local",
    "name": "Alice Adams",
    "feed_uuid": "stage5-demo-lead-uuid",
    "stripe_customer_id": "cus_stage5_demo_lead",
    "stripe_subscription_id": "sub_stage5_demo_lead",
    "zip": "33601",
}

DEMO_REFEREES = [
    {
        "marker": "stage5-demo-ref-a@local",
        "name": "Bob Brown",
        "feed_uuid": "stage5-demo-ref-a-uuid",
        "stripe_customer_id": "cus_stage5_demo_a",
        "zip": "33602",
    },
    {
        "marker": "stage5-demo-ref-b@local",
        "name": "Carol Chen",
        "feed_uuid": "stage5-demo-ref-b-uuid",
        "stripe_customer_id": "cus_stage5_demo_b",
        "zip": "33603",
    },
]

# Extra subscribers used as competing referrers for the leaderboard.
DEMO_LEADERBOARD_PEERS = [
    {
        "marker": "stage5-demo-peer-1@local",
        "name": "Dan Diaz",
        "feed_uuid": "stage5-demo-peer-1-uuid",
        "stripe_customer_id": "cus_stage5_demo_peer1",
    },
    {
        "marker": "stage5-demo-peer-2@local",
        "name": "Eve Edwards",
        "feed_uuid": "stage5-demo-peer-2-uuid",
        "stripe_customer_id": "cus_stage5_demo_peer2",
    },
]

DEMO_PROPERTY_PARCEL = "STAGE5-DEMO-PROP"


# ── Helpers ──────────────────────────────────────────────────────────────────

def upsert_subscriber(db: Session, *, marker: str, name: str, feed_uuid: str,
                      stripe_customer_id: str, vertical: str = DEMO_VERTICAL,
                      county: str = DEMO_COUNTY,
                      stripe_subscription_id: str | None = None,
                      tier: str = "starter") -> Subscriber:
    sub = db.execute(
        select(Subscriber).where(Subscriber.email == marker)
    ).scalar_one_or_none()
    if sub:
        sub.name = name
        sub.event_feed_uuid = feed_uuid
        # Only overwrite Stripe IDs if they're still the fake placeholder.
        # Real IDs created by --stripe must survive subsequent reseeds.
        if not sub.stripe_customer_id or sub.stripe_customer_id.startswith("cus_stage5_demo_"):
            sub.stripe_customer_id = stripe_customer_id
        if not sub.stripe_subscription_id or (sub.stripe_subscription_id or "").startswith("sub_stage5_demo_"):
            sub.stripe_subscription_id = stripe_subscription_id
        sub.tier = tier
        sub.vertical = vertical
        sub.county_id = county
        sub.status = "active"
        db.flush()
        return sub

    sub = Subscriber(
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=stripe_subscription_id,
        tier=tier,
        vertical=vertical,
        county_id=county,
        founding_member=False,
        status="active",
        email=marker,
        name=name,
        event_feed_uuid=feed_uuid,
        has_saved_card=True,
    )
    db.add(sub)
    db.flush()
    return sub


def upsert_property(db: Session) -> Property:
    prop = db.execute(
        select(Property).where(Property.parcel_id == DEMO_PROPERTY_PARCEL)
    ).scalar_one_or_none()
    if prop:
        return prop
    prop = Property(
        parcel_id=DEMO_PROPERTY_PARCEL,
        address="100 Demo Way",
        city="Tampa",
        state="FL",
        zip="33601",
        county_id=DEMO_COUNTY,
        property_type="Single Family",
        year_built=1995,
        sq_ft=1850,
    )
    db.add(prop)
    db.flush()
    return prop


def upsert_wallet(db: Session, *, subscriber_id: int, credits: int,
                  tier: str = "growth") -> WalletBalance:
    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    if wallet:
        wallet.credits_remaining = credits
        wallet.wallet_tier = tier
        wallet.auto_reload_enabled = True
        db.flush()
        return wallet
    wallet = WalletBalance(
        subscriber_id=subscriber_id,
        wallet_tier=tier,
        credits_remaining=credits,
        credits_used_total=0,
        auto_reload_enabled=True,
    )
    db.add(wallet)
    db.flush()
    return wallet


def upsert_zip(db: Session, *, subscriber_id: int, zip_code: str) -> ZipTerritory:
    territory = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code == zip_code,
            ZipTerritory.vertical == DEMO_VERTICAL,
            ZipTerritory.county_id == DEMO_COUNTY,
        )
    ).scalar_one_or_none()
    if territory:
        territory.subscriber_id = subscriber_id
        territory.status = "locked"
        territory.locked_at = datetime.now(timezone.utc)
        db.flush()
        return territory
    territory = ZipTerritory(
        zip_code=zip_code,
        vertical=DEMO_VERTICAL,
        county_id=DEMO_COUNTY,
        subscriber_id=subscriber_id,
        status="locked",
        locked_at=datetime.now(timezone.utc),
    )
    db.add(territory)
    db.flush()
    return territory


def upsert_referral(db: Session, *, referrer_id: int, referee_id: int,
                    code: str, days_ago: int = 0) -> ReferralEvent:
    event = db.execute(
        select(ReferralEvent).where(
            ReferralEvent.referrer_subscriber_id == referrer_id,
            ReferralEvent.referee_subscriber_id == referee_id,
        )
    ).scalar_one_or_none()
    confirmed_at = datetime.now(timezone.utc) - timedelta(days=days_ago)
    if event:
        event.status = "confirmed"
        event.confirmed_at = confirmed_at
        db.flush()
        return event
    event = ReferralEvent(
        referrer_subscriber_id=referrer_id,
        referee_subscriber_id=referee_id,
        referral_code=code,
        status="confirmed",
        reward_type="credits",
        reward_value="20",
        confirmed_at=confirmed_at,
    )
    db.add(event)
    db.flush()
    return event


def insert_proof_wall_deals(db: Session, *, subscriber_id: int, property_id: int) -> int:
    """Spread 5 deals across the last 7 days for a populated proof wall."""
    # Wipe existing demo wins so dates always look fresh
    existing = db.execute(
        select(DealOutcome).where(DealOutcome.subscriber_id == subscriber_id)
    ).scalars().all()
    count = 0
    today = datetime.now(timezone.utc).date()
    target_specs = [
        ("10_25k", 15000, 0),
        ("25k_plus", 42000, 1),
        ("5_10k", 7500, 2),
        ("10_25k", 18000, 4),
        ("25k_plus", 31000, 6),
    ]
    by_date = {d.deal_date: d for d in existing if d.deal_date}
    for bucket, amount, days in target_specs:
        d = today - timedelta(days=days)
        if d in by_date:
            row = by_date[d]
            row.deal_size_bucket = bucket
            row.deal_amount = amount
            row.property_id = property_id
            count += 1
            continue
        db.add(DealOutcome(
            subscriber_id=subscriber_id,
            property_id=property_id,
            deal_size_bucket=bucket,
            deal_amount=amount,
            deal_date=d,
            created_at=datetime.now(timezone.utc) - timedelta(days=days),
        ))
        count += 1
    db.flush()
    return count


# ── Stripe provisioning (optional) ───────────────────────────────────────────

def _ensure_stripe_loaded() -> bool:
    """Init the stripe SDK with the active test key. Returns False if not configured."""
    import stripe
    from config.settings import settings
    key = settings.active_stripe_secret_key
    if not key:
        return False
    stripe.api_key = key.get_secret_value()
    return True


def provision_stripe_for_lead(db: Session, lead: Subscriber) -> dict:
    """
    Create (or reuse) a real Stripe test customer + a real subscription on
    the autopilot_lite price for the demo lead. Patches the real IDs onto
    the subscriber row so bundle PIs / annual switch / tier upgrade work.

    Idempotent: if the lead already has real-looking Stripe IDs (not the
    fake `cus_stage5_demo_lead` placeholder), it verifies they exist in
    Stripe and reuses; otherwise creates fresh.

    Returns a dict describing what happened.
    """
    if not _ensure_stripe_loaded():
        return {"status": "skipped", "reason": "Stripe key not configured"}

    import stripe
    from config.settings import settings

    base_price = settings.active_stripe_price("autopilot_lite")
    if not base_price:
        return {"status": "skipped", "reason": "STRIPE_TEST_PRICE_AUTOPILOT_LITE not set in .env"}

    fake_marker = "cus_stage5_demo_"
    has_real_customer = (
        lead.stripe_customer_id
        and not lead.stripe_customer_id.startswith(fake_marker)
    )

    customer_id = lead.stripe_customer_id
    if has_real_customer:
        # Verify customer still exists in Stripe; if it 404s, recreate
        try:
            stripe.Customer.retrieve(customer_id)
        except stripe.error.InvalidRequestError:
            has_real_customer = False
            customer_id = None

    if not has_real_customer:
        # Stripe requires a real-looking TLD; replace local-only markers
        stripe_email = (lead.email or "").replace("@local", "@example.com")
        cust = stripe.Customer.create(
            email=stripe_email or f"stage5-demo-{lead.id}@example.com",
            name=lead.name,
            description=f"Stage 5 demo customer (subscriber.id={lead.id})",
            metadata={"stage5_demo": "true", "subscriber_id": str(lead.id)},
        )
        customer_id = cust.id

    # Attach a default-success test payment method so the subscription succeeds
    try:
        pm = stripe.PaymentMethod.create(
            type="card",
            card={"token": "tok_visa"},   # Stripe-provided test token (always succeeds)
        )
        stripe.PaymentMethod.attach(pm.id, customer=customer_id)
        stripe.Customer.modify(
            customer_id,
            invoice_settings={"default_payment_method": pm.id},
        )
        payment_method_id = pm.id
    except stripe.error.StripeError as exc:
        # Card attach failed — log and continue (subscription may still work in some test modes)
        print(f"[stripe] payment method attach failed: {exc}")
        payment_method_id = None

    # Reuse subscription if it exists, else create one
    fake_sub_marker = "sub_stage5_demo_"
    subscription_id = lead.stripe_subscription_id
    has_real_sub = (
        subscription_id and not subscription_id.startswith(fake_sub_marker)
    )
    if has_real_sub:
        try:
            sub_obj = stripe.Subscription.retrieve(subscription_id)
            # Validate it's tied to the right customer; if not, recreate
            if sub_obj.customer != customer_id:
                has_real_sub = False
        except stripe.error.InvalidRequestError:
            has_real_sub = False

    if not has_real_sub:
        sub_obj = stripe.Subscription.create(
            customer=customer_id,
            items=[{"price": base_price}],
            payment_behavior="default_incomplete",
            metadata={"stage5_demo": "true", "subscriber_id": str(lead.id)},
        )
        subscription_id = sub_obj.id

    # Patch the DB so the lead has real Stripe identifiers
    lead.stripe_customer_id = customer_id
    lead.stripe_subscription_id = subscription_id
    if payment_method_id:
        lead.stripe_payment_method_id = payment_method_id
        lead.has_saved_card = True
    db.flush()

    return {
        "status": "ok",
        "customer_id": customer_id,
        "subscription_id": subscription_id,
        "payment_method_id": payment_method_id,
        "base_price": base_price,
    }


# ── Main ────────────────────────────────────────────────────────────────────

def reset_demo_data(db: Session) -> None:
    """Tear down all stage5-demo-* rows so the next seed starts clean."""
    markers = [DEMO_LEAD["marker"]] + [r["marker"] for r in DEMO_REFEREES] + [r["marker"] for r in DEMO_LEADERBOARD_PEERS]

    subs = db.execute(select(Subscriber).where(Subscriber.email.in_(markers))).scalars().all()
    sub_ids = [s.id for s in subs]
    if not sub_ids:
        print("[reset] No demo data found to remove.")
        return

    db.execute(
        ReferralTeam.__table__.delete().where(
            ReferralTeam.lead_subscriber_id.in_(sub_ids)
        )
    )
    db.execute(
        ReferralEvent.__table__.delete().where(
            (ReferralEvent.referrer_subscriber_id.in_(sub_ids))
            | (ReferralEvent.referee_subscriber_id.in_(sub_ids))
        )
    )
    db.execute(
        DealOutcome.__table__.delete().where(DealOutcome.subscriber_id.in_(sub_ids))
    )
    db.execute(
        PremiumPurchase.__table__.delete().where(PremiumPurchase.subscriber_id.in_(sub_ids))
    )
    db.execute(
        BundlePurchase.__table__.delete().where(BundlePurchase.subscriber_id.in_(sub_ids))
    )
    db.execute(
        MessageOutcome.__table__.delete().where(MessageOutcome.subscriber_id.in_(sub_ids))
    )
    # WalletTransaction has FK to wallet_balances — must delete first
    db.execute(
        WalletTransaction.__table__.delete().where(WalletTransaction.subscriber_id.in_(sub_ids))
    )
    db.execute(
        WalletBalance.__table__.delete().where(WalletBalance.subscriber_id.in_(sub_ids))
    )
    db.execute(
        ZipTerritory.__table__.delete().where(ZipTerritory.subscriber_id.in_(sub_ids))
    )
    for s in subs:
        db.delete(s)
    db.flush()
    db.execute(Property.__table__.delete().where(Property.parcel_id == DEMO_PROPERTY_PARCEL))
    db.flush()
    print(f"[reset] Removed {len(sub_ids)} demo subscriber(s) and related rows.")


def seed(db: Session) -> dict:
    # 1. Property
    prop = upsert_property(db)

    # 2. Lead subscriber + 2 referees
    lead = upsert_subscriber(
        db,
        marker=DEMO_LEAD["marker"],
        name=DEMO_LEAD["name"],
        feed_uuid=DEMO_LEAD["feed_uuid"],
        stripe_customer_id=DEMO_LEAD["stripe_customer_id"],
        stripe_subscription_id=DEMO_LEAD["stripe_subscription_id"],
        tier="autopilot_lite",  # so AP Pro flow works inline
    )
    referees = []
    for r in DEMO_REFEREES:
        sub = upsert_subscriber(
            db,
            marker=r["marker"],
            name=r["name"],
            feed_uuid=r["feed_uuid"],
            stripe_customer_id=r["stripe_customer_id"],
            tier="starter",
        )
        referees.append(sub)

    # Bonus peers for leaderboard variety
    peers = []
    for p in DEMO_LEADERBOARD_PEERS:
        sub = upsert_subscriber(
            db,
            marker=p["marker"],
            name=p["name"],
            feed_uuid=p["feed_uuid"],
            stripe_customer_id=p["stripe_customer_id"],
            tier="starter",
        )
        peers.append(sub)

    # 3. Wallet for the lead
    upsert_wallet(db, subscriber_id=lead.id, credits=100, tier="growth")

    # 4. Locked ZIPs (one per team member) so the team view has shared_zips
    upsert_zip(db, subscriber_id=lead.id, zip_code=DEMO_LEAD["zip"])
    for r, cfg in zip(referees, DEMO_REFEREES):
        upsert_zip(db, subscriber_id=r.id, zip_code=cfg["zip"])

    # 5. Confirmed referrals — lead -> ref_a -> ref_b
    upsert_referral(db, referrer_id=lead.id, referee_id=referees[0].id, code="DEMO1", days_ago=2)
    upsert_referral(db, referrer_id=lead.id, referee_id=referees[1].id, code="DEMO2", days_ago=1)

    # Bonus referrals so the leaderboard has multiple cohort entries.
    # Lead picks up two more referrals from peers; peer1 makes one referral.
    upsert_referral(db, referrer_id=peers[0].id, referee_id=peers[1].id, code="DEMOP1", days_ago=3)
    upsert_referral(db, referrer_id=lead.id, referee_id=peers[0].id, code="DEMO3", days_ago=4)
    upsert_referral(db, referrer_id=lead.id, referee_id=peers[1].id, code="DEMO4", days_ago=5)

    # 6. Trigger team unlock
    from src.services.referral_engine import _check_team_unlock
    team = _check_team_unlock(lead.id, db)

    # 7. Proof-wall deals
    deal_count = insert_proof_wall_deals(db, subscriber_id=lead.id, property_id=prop.id)

    db.flush()

    return {
        "lead": lead,
        "referees": referees,
        "peers": peers,
        "property": prop,
        "team": team,
        "deal_count": deal_count,
    }


def print_summary(result: dict) -> None:
    lead: Subscriber = result["lead"]
    prop: Property = result["property"]
    team: ReferralTeam | None = result["team"]
    referees = result["referees"]
    peers = result["peers"]

    sep = "=" * 78
    print()
    print(sep)
    print("STAGE 5 DEMO READY")
    print(sep)
    print()
    print("Test subscriber (lead):")
    print(f"  id:            {lead.id}")
    print(f"  feed_uuid:     {lead.event_feed_uuid}")
    print(f"  email:         {lead.email}")
    print(f"  name:          {lead.name}")
    print(f"  tier:          {lead.tier}")
    print(f"  vertical:      {lead.vertical}")
    print(f"  county:        {lead.county_id}")
    print(f"  stripe_sub:    {lead.stripe_subscription_id}")
    fake_marker = "cus_stage5_demo_"
    if lead.stripe_customer_id and lead.stripe_customer_id.startswith(fake_marker):
        print(f"  stripe state:  PLACEHOLDER (run with --stripe to provision real test resources)")
    else:
        print(f"  stripe state:  REAL (bundle / annual / upgrade flows work end-to-end)")
    print()
    print("Demo property:")
    print(f"  id:            {prop.id}")
    print(f"  address:       {prop.address}, {prop.city} {prop.state} {prop.zip}")
    print()
    print("Wallet balance: 100 credits (Growth tier).")
    print(f"Proof-wall deals seeded: {result['deal_count']}")
    if team:
        print(f"Referral team:  id={team.id}  members={team.member_subscriber_ids}  zips={team.shared_zips}")
    else:
        print("Referral team:  (not unlocked — re-run seeder if this is unexpected)")
    print()
    print("Other demo subscribers:")
    for r in referees:
        print(f"  referee:       id={r.id}  feed_uuid={r.event_feed_uuid}  ({r.name})")
    for p in peers:
        print(f"  leaderboard:   id={p.id}  feed_uuid={p.event_feed_uuid}  ({p.name})")
    print()
    print(sep)
    print("CLICK-HERE FLOW URLS")
    print(sep)
    print()

    base = "http://localhost:5173"
    feed = lead.event_feed_uuid
    print(f"  Landing + Social Proof Wall:")
    print(f"    {base}/")
    print()
    print(f"  Dashboard (baseline — Premium button, Team tile, Leaderboard):")
    print(f"    {base}/dashboard/{feed}")
    print()
    print(f"  Dashboard with Annual Offer banner deep link:")
    print(f"    {base}/dashboard/{feed}?annual=accept")
    print()
    print(f"  Dashboard with AP Pro upgrade banner deep link:")
    print(f"    {base}/dashboard/{feed}?upgrade=autopilot_pro")
    print()
    print(f"  Bundle deep links:")
    for bundle in ("weekend", "storm", "zip_booster", "monthly_reload"):
        for variant in ("a", "b"):
            print(f"    {base}/dashboard/{feed}?bundle={bundle}&variant={variant}")
    print()
    print(f"  Team-view from each member's dashboard:")
    print(f"    {base}/dashboard/{feed}                  (team lead)")
    for r in referees:
        print(f"    {base}/dashboard/{r.event_feed_uuid}  ({r.name})")
    print()
    print(f"  Sandbox (manual exerciser — paste feed_uuid + property_id below):")
    print(f"    {base}/stage5-sandbox")
    print()
    print(sep)
    print("NEXT STEPS")
    print(sep)
    print()
    print("  1. Run leaderboard aggregator so the widget has data:")
    print("     python -m src.tasks.leaderboard")
    print()
    print("  2. Start backend:")
    print("     uvicorn src.api.main:app --reload --port 8001")
    print()
    print("  3. Start UI (in Forced-action-ui):")
    print("     npm run dev")
    print()
    print("  4. Sandbox page values to paste:")
    print(f"     Feed UUID:    {feed}")
    print(f"     Property ID:  {prop.id}")
    print()
    print(sep)
    print("VERIFICATION QUERIES (run after clicking through the flows)")
    print(sep)
    print()
    print("  -- Premium SKUs purchased")
    print("  SELECT sku, paid_via, count(*) FROM premium_purchases GROUP BY 1,2;")
    print()
    print("  -- Bundle A/B split")
    print("  SELECT bundle_type, ab_variant, count(*) FROM bundle_purchases GROUP BY 1,2;")
    print()
    print("  -- Annual offers fired")
    print("  SELECT template_id, count(*) FROM message_outcomes WHERE template_id LIKE 'annual_offer_%' GROUP BY 1;")
    print()
    print("  -- Active referral teams")
    print("  SELECT id, lead_subscriber_id, county_id, vertical, array_length(member_subscriber_ids, 1) AS members FROM referral_teams WHERE status='active';")
    print()
    print("  -- Wallet remaining")
    print(f"  SELECT credits_remaining FROM wallet_balances WHERE subscriber_id={lead.id};")
    print()
    print(sep)
    print()
    print("To tear down the demo data later: python -m scripts.seed_stage5_demo --reset")
    print()


def main() -> int:
    do_reset = "--reset" in sys.argv
    reset_only = "--reset-only" in sys.argv
    do_stripe = "--stripe" in sys.argv

    with get_db_context() as db:
        if do_reset or reset_only:
            reset_demo_data(db)
            db.commit()
            if reset_only:
                print("[done] reset complete.")
                return 0

        result = seed(db)
        db.commit()

        if do_stripe:
            print()
            print("=" * 78)
            print("PROVISIONING REAL STRIPE TEST RESOURCES")
            print("=" * 78)
            stripe_info = provision_stripe_for_lead(db, result["lead"])
            db.commit()
            if stripe_info["status"] == "ok":
                print(f"  customer_id:     {stripe_info['customer_id']}")
                print(f"  subscription_id: {stripe_info['subscription_id']}")
                print(f"  pm_id:           {stripe_info['payment_method_id']}")
                print(f"  base_price:      {stripe_info['base_price']}")
                print()
                print("  These IDs are now patched onto the demo lead row so the bundle")
                print("  modal / annual accept / tier upgrade flows work end-to-end.")
            else:
                print(f"  [skipped] {stripe_info['reason']}")
            print()

        print_summary(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
