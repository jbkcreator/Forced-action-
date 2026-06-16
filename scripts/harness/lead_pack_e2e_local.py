"""
Local end-to-end harness for the Lead Pack flow (ADR 0018).

Seeds clearly-tagged test data in the real DB, drives the REAL code paths
(_on_lead_pack_payment reserve + fulfill_purchase), asserts each lifecycle
outcome, then DELETES everything it created.

Tracerfy is mocked by default (no TRACERFY_API_KEY locally); pass --real-tracerfy
to hit the live API if a key is set. Stripe refunds are mocked in-process.

Two modes:
  (default, in-process) — no server needed; exercises reserve + fulfill + all
    failure paths directly against Postgres.
  --via-stripe          — creates a REAL test-mode PaymentIntent with metadata
    and confirms it, so a running `stripe listen` + uvicorn deliver the webhook
    over HTTP (proves the ingress). Then runs the sweep to finish + asserts.

Usage:
    python scripts/harness/lead_pack_e2e_local.py            # all in-process scenarios
    python scripts/harness/lead_pack_e2e_local.py --cleanup  # just purge tagged rows
    python scripts/harness/lead_pack_e2e_local.py --via-stripe
"""
from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import text

sys.path.insert(0, ".")

from config.settings import get_settings              # noqa: E402
from src.core.database import get_db_context           # noqa: E402

TAG_PARCEL = "HARNESS-"
TAG_UUID = "harness-"
# Hard guardrail: every harness subscriber gets THIS address only, so any real
# delivery/refund email lands in one inbox and never a customer.
TEST_EMAIL = "lesly.vj@heu.ai"


# ── seeding ────────────────────────────────────────────────────────────────

def _mk_subscriber(db, uuid, county, vertical="roofing"):
    from src.core.models import Subscriber
    s = Subscriber(
        stripe_customer_id=f"cus_{uuid}", tier="pro", vertical=vertical,
        county_id=county, status="active", event_feed_uuid=uuid,
        email=TEST_EMAIL,
    )
    db.add(s)
    db.flush()
    return s


def _mk_property(db, parcel, zip_code, county, score):
    from src.core.models import Property, DistressScore, Owner
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county, address=f"{parcel} Test St",
                 city="Tampa", state="FL")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=score,
        vertical_scores={"roofing": score, "restoration": score},
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high",
                 county_id=county))
    db.flush()
    return p.id


def _pi(pi_id, uuid, zip_code, county, vertical="roofing"):
    return {"id": pi_id, "metadata": {
        "product": "lead_pack", "feed_uuid": uuid, "zip_code": zip_code,
        "vertical": vertical, "county_id": county}}


def cleanup(db):
    """Delete every harness-tagged row in FK-safe order."""
    db.execute(text("""
        DELETE FROM sent_leads WHERE subscriber_id IN
          (SELECT id FROM subscribers WHERE event_feed_uuid LIKE :u)
    """), {"u": TAG_UUID + "%"})
    db.execute(text("""
        DELETE FROM lead_exclusivity WHERE property_id IN
          (SELECT id FROM properties WHERE parcel_id LIKE :p)
    """), {"p": TAG_PARCEL + "%"})
    db.execute(text("""
        DELETE FROM lead_pack_purchases WHERE subscriber_id IN
          (SELECT id FROM subscribers WHERE event_feed_uuid LIKE :u)
    """), {"u": TAG_UUID + "%"})
    for tbl in ("enriched_contacts", "owners", "distress_scores"):
        db.execute(text(f"""
            DELETE FROM {tbl} WHERE property_id IN
              (SELECT id FROM properties WHERE parcel_id LIKE :p)
        """), {"p": TAG_PARCEL + "%"})
    db.execute(text("DELETE FROM properties WHERE parcel_id LIKE :p"), {"p": TAG_PARCEL + "%"})
    db.execute(text("DELETE FROM subscribers WHERE event_feed_uuid LIKE :u"), {"u": TAG_UUID + "%"})
    db.commit()


# ── mock helpers ─────────────────────────────────────────────────────────────

def _all_hit(lead_ids):
    return {pid: {"match_success": True, "mobile_phone": "8135550100",
                  "landline": None, "email": None, "mailing_address": None}
            for pid in lead_ids}


def _one_miss(lead_ids):
    out = _all_hit(lead_ids)
    out[lead_ids[0]] = {"match_success": False, "mobile_phone": None,
                        "landline": None, "email": None, "mailing_address": None}
    return out


@contextmanager
def _mocks(enriched_map=None, real_tracerfy=False, real_email=False):
    """Patch Tracerfy (unless real) + Stripe refund + outbound email (unless real).

    real_email=True lets the REAL delivery/refund emails send — they go to
    TEST_EMAIL only, since that's the only address the harness ever seeds.
    """
    patches = []
    if not real_email:
        patches.append(patch("src.services.email.send_email"))
    if not real_tracerfy:
        patches.append(patch("src.services.tracerfy_fallback.hot_enrich_properties",
                             side_effect=lambda db, pids, **k: enriched_map(pids)))
    patches.append(patch("stripe.Refund.create", return_value={"id": "re_harness"}))
    for p in patches:
        p.start()
    try:
        yield
    finally:
        for p in patches:
            p.stop()


# ── scenarios (in-process) ───────────────────────────────────────────────────

def _check(label, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    return cond


def scenario_deliver(county, real_tracerfy, real_email=False):
    from src.services.stripe_webhooks import _on_lead_pack_payment
    from src.tasks.lead_pack_fulfillment_sweep import fulfill_purchase
    from src.core.models import LeadPackPurchase
    print("\n== scenario: DELIVER (floor clears) ==")
    ok = True
    with get_db_context() as db:
        _mk_subscriber(db, TAG_UUID + "deliver", county)
        for i in range(6):
            _mk_property(db, f"{TAG_PARCEL}A{i}", "90001", county, 90 - i)
        db.commit()
        _on_lead_pack_payment(_pi("pi_h_deliver", TAG_UUID + "deliver", "90001", county), db)
        db.commit()
        purchase = db.query(LeadPackPurchase).filter_by(stripe_payment_intent_id="pi_h_deliver").one()
        ok &= _check("reserved status=enriching", purchase.status == "enriching")
        ok &= _check("5 leads reserved", len(purchase.lead_ids or []) == 5)
        with _mocks(_all_hit, real_tracerfy, real_email):
            outcome = fulfill_purchase(db, purchase)
            db.commit()
        if real_email:
            print(f"  -> delivery email sent to {TEST_EMAIL}")
        ok &= _check("fulfill -> delivered", outcome == "delivered")
        ok &= _check("status=delivered", purchase.status == "delivered")
        sent = db.execute(text("SELECT count(*) FROM sent_leads WHERE subscriber_id=:s AND source='lead_pack'"),
                          {"s": purchase.subscriber_id}).scalar()
        ok &= _check("5 SentLead rows", sent == 5)
        excl = db.execute(text("SELECT count(*) FROM lead_exclusivity WHERE source_id=:i AND source='lead_pack'"),
                          {"i": purchase.id}).scalar()
        ok &= _check("5 exclusivity rows kept", excl == 5)
    return ok


def scenario_floor_miss(county, real_tracerfy, real_email=False):
    from src.services.stripe_webhooks import _on_lead_pack_payment
    from src.tasks.lead_pack_fulfillment_sweep import fulfill_purchase
    from src.core.models import LeadPackPurchase
    print("\n== scenario: FLOOR MISS (refund + release) ==")
    ok = True
    with get_db_context() as db:
        _mk_subscriber(db, TAG_UUID + "floor", county)
        for i in range(6):
            _mk_property(db, f"{TAG_PARCEL}F{i}", "90002", county, 90 - i)
        db.commit()
        _on_lead_pack_payment(_pi("pi_h_floor", TAG_UUID + "floor", "90002", county), db)
        db.commit()
        purchase = db.query(LeadPackPurchase).filter_by(stripe_payment_intent_id="pi_h_floor").one()
        with _mocks(_one_miss, real_tracerfy, real_email):
            outcome = fulfill_purchase(db, purchase)
            db.commit()
        if real_email:
            print(f"  -> refund email sent to {TEST_EMAIL}")
        ok &= _check("fulfill -> refunded", outcome == "refunded")
        ok &= _check("reason quality_floor_4_of_5", purchase.refund_reason == "quality_floor_4_of_5")
        excl = db.execute(text("SELECT count(*) FROM lead_exclusivity WHERE source_id=:i AND source='lead_pack'"),
                          {"i": purchase.id}).scalar()
        ok &= _check("exclusivity released (0 rows)", excl == 0)
    return ok


def scenario_short_pack(county, real_tracerfy):
    from src.services.stripe_webhooks import _on_lead_pack_payment
    from src.core.models import LeadPackPurchase
    print("\n== scenario: SHORT PACK (<5 -> webhook refund) ==")
    ok = True
    with get_db_context() as db:
        _mk_subscriber(db, TAG_UUID + "short", county)
        for i in range(4):
            _mk_property(db, f"{TAG_PARCEL}S{i}", "90003", county, 80)
        db.commit()
        with _mocks(_all_hit, real_tracerfy):
            _on_lead_pack_payment(_pi("pi_h_short", TAG_UUID + "short", "90003", county), db)
            db.commit()
        purchase = db.query(LeadPackPurchase).filter_by(stripe_payment_intent_id="pi_h_short").one()
        ok &= _check("status=refunded", purchase.status == "refunded")
        ok &= _check("reason short_pack_4_of_5", purchase.refund_reason == "short_pack_4_of_5")
    return ok


def scenario_unlaunched(real_tracerfy):
    from src.services.stripe_webhooks import _on_lead_pack_payment
    from src.core.models import LeadPackPurchase
    print("\n== scenario: UNLAUNCHED COUNTY (webhook refund) ==")
    ok = True
    with get_db_context() as db:
        _mk_subscriber(db, TAG_UUID + "ghost", "ghost_county")
        for i in range(6):
            _mk_property(db, f"{TAG_PARCEL}G{i}", "90004", "ghost_county", 80)
        db.commit()
        with _mocks(_all_hit, real_tracerfy):
            _on_lead_pack_payment(_pi("pi_h_ghost", TAG_UUID + "ghost", "90004", "ghost_county"), db)
            db.commit()
        purchase = db.query(LeadPackPurchase).filter_by(stripe_payment_intent_id="pi_h_ghost").one()
        ok &= _check("status=refunded", purchase.status == "refunded")
        ok &= _check("reason county_not_launched", purchase.refund_reason == "county_not_launched")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cleanup", action="store_true", help="purge tagged rows and exit")
    ap.add_argument("--real-tracerfy", action="store_true", help="hit live Tracerfy (needs key)")
    ap.add_argument("--real-email", action="store_true",
                    help=f"actually send delivery/refund emails (to {TEST_EMAIL} only)")
    ap.add_argument("--via-stripe", action="store_true", help="drive a real test PaymentIntent over HTTP")
    args = ap.parse_args()

    s = get_settings()
    county = s.county_launch_source_county or "hillsborough"

    # Always start clean.
    with get_db_context() as db:
        cleanup(db)
    if args.cleanup:
        print("Cleaned up harness rows. Done.")
        return 0

    if args.via_stripe:
        return _via_stripe(s, county)

    if args.real_email:
        print(f"REAL EMAIL MODE — delivery + refund emails will be sent to {TEST_EMAIL}")

    results = {}
    try:
        results["deliver"] = scenario_deliver(county, args.real_tracerfy, args.real_email)
        results["floor_miss"] = scenario_floor_miss(county, args.real_tracerfy, args.real_email)
        results["short_pack"] = scenario_short_pack(county, args.real_tracerfy)
        results["unlaunched"] = scenario_unlaunched(args.real_tracerfy)
    finally:
        with get_db_context() as db:
            cleanup(db)
        print("\nCleaned up harness rows.")

    print("\n==== SUMMARY ====")
    for k, v in results.items():
        print(f"  {k:12s}: {'PASS' if v else 'FAIL'}")
    return 0 if all(results.values()) else 1


def _via_stripe(s, county) -> int:
    """Create + confirm a real test PaymentIntent so a running `stripe listen`
    delivers the webhook over HTTP. Then run the sweep to finish."""
    import time
    import stripe
    from src.core.models import LeadPackPurchase
    from src.tasks.lead_pack_fulfillment_sweep import fulfill_purchase

    key = s.active_stripe_secret_key
    if not key or not key.get_secret_value().startswith("sk_test"):
        print("ABORT: not in Stripe TEST mode — refusing to create a real charge.")
        return 1
    stripe.api_key = key.get_secret_value()

    uuid = TAG_UUID + "stripe"
    with get_db_context() as db:
        _mk_subscriber(db, uuid, county)
        for i in range(6):
            _mk_property(db, f"{TAG_PARCEL}T{i}", "90009", county, 90 - i)
        db.commit()

    print("Creating + confirming test PaymentIntent (metadata: lead_pack)...")
    pi = stripe.PaymentIntent.create(
        amount=9900, currency="usd", payment_method="pm_card_visa",
        confirm=True, automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
        metadata={"product": "lead_pack", "feed_uuid": uuid, "zip_code": "90009",
                  "vertical": "roofing", "county_id": county},
    )
    print(f"  PI {pi['id']} status={pi['status']} — webhook should arrive at stripe listen now")

    # Poll for the reservation written by the HTTP webhook path.
    pid = None
    for _ in range(30):
        with get_db_context() as db:
            row = db.query(LeadPackPurchase).filter_by(stripe_payment_intent_id=pi["id"]).one_or_none()
            if row:
                pid = row.id
                status = row.status
                break
        time.sleep(1)

    if pid is None:
        print("  [FAIL] no LeadPackPurchase row — is uvicorn + `stripe listen` running, "
              "and STRIPE_TEST_WEBHOOK_SECRET set to the listen secret?")
        with get_db_context() as db:
            cleanup(db)
        return 1
    print(f"  [PASS] webhook delivered over HTTP -> purchase {pid} status={status}")

    # Finish via the sweep core (mock Tracerfy — no key locally).
    with get_db_context() as db:
        purchase = db.get(LeadPackPurchase, pid)
        with _mocks(_all_hit, real_tracerfy=False):
            outcome = fulfill_purchase(db, purchase)
            db.commit()
        print(f"  fulfill -> {outcome} (status={purchase.status})")
        cleanup(db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
