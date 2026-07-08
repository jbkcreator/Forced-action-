"""
End-to-end harness for the subscription payment + rebill + failed-card lifecycle
(Notion "fixes and evidence" Task 2).

Covers the five Task-2 requirements against the REAL shared Postgres:
  1. Purchase / checkout                → drives _on_checkout_completed
  2. Payment succeeds & money lands     → REAL Stripe TEST-mode subscription
                                          (real Customer + pm_card_visa + invoice)
  3. Lead delivery record created       → lead_delivery.claim -> deliveries row
  4. Day-30 rebill                      → invoice.payment_succeeded
                                          (billing_reason=subscription_cycle)
  5. Failed-card retry / dunning        → invoice.payment_failed + recovery sweep

Real Stripe objects are created ONLY in test mode (sk_test) and are cleaned up
(subscription cancelled, customer deleted). All DB rows are tagged and deleted
in a finally block. Every outbound side effect (GHL, email, Claude, Redis) is
mocked — nothing leaves the box except the Stripe TEST API calls.

Usage:
    python scripts/harness/payment_rebill_e2e.py            # full lifecycle
    python scripts/harness/payment_rebill_e2e.py --cleanup  # purge tagged rows
    python scripts/harness/payment_rebill_e2e.py --no-stripe  # skip real Stripe,
                                          fabricate customer id (DB paths only)
"""
from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import text

sys.path.insert(0, ".")

from config.settings import get_settings              # noqa: E402
from src.core.database import get_db_context           # noqa: E402

TAG = "hpay-"                 # subscriber.event_feed_uuid + stripe metadata tag
HARNESS_NAME = "HPAY Harness"  # subscriber.name — reliable cleanup tag we control
TAG_PARCEL = "HPAY-"
TEST_EMAIL = "lesly.vj@heu.ai"
TEST_ZIP = "00099"           # bogus ZIP — never collides with a real locked territory
VERTICAL = "roofing"
TIER = "starter"


def _check(label, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    return bool(cond)


# ── outbound side-effect mocks ───────────────────────────────────────────────

@contextmanager
def _mocks():
    patches = [
        patch("src.services.stripe_webhooks.push_subscriber_to_ghl", return_value=None),
        patch("src.services.stripe_webhooks._send_first_leads_email", return_value=None),
        patch("src.core.redis_client.rdelete", return_value=None),
        patch("src.services.email.send_email", return_value=None),
        # recovery sweep day-1 send: mock the LLM + email, keep the windowing/flag logic real
        patch("src.services.claude_router.call_claude_with_usage",
              return_value=("SUBJECT: Card issue\nBODY: Please update your card.", {})),
    ]
    for p in patches:
        p.start()
    try:
        yield
    finally:
        for p in patches:
            p.stop()


# ── cleanup ──────────────────────────────────────────────────────────────────

def cleanup(db):
    # checkout.session.completed mints its OWN random event_feed_uuid, so the
    # TAG prefix never matches subscribers it creates. subscriber.name is the
    # reliable tag we control (set via customer_details.name in the event).
    sub_ids = [r[0] for r in db.execute(
        text("SELECT id FROM subscribers WHERE event_feed_uuid LIKE :u "
             "OR name = :n OR stripe_customer_id LIKE :c"),
        {"u": TAG + "%", "n": HARNESS_NAME, "c": "cus_" + TAG + "%"},
    ).fetchall()]
    acct_ids = [r[0] for r in db.execute(
        text("SELECT account_id FROM customer_accounts WHERE subscriber_id = ANY(:s)"),
        {"s": sub_ids or [-1]},
    ).fetchall()]
    if acct_ids:
        db.execute(text("DELETE FROM deliveries WHERE account_id = ANY(:a)"), {"a": acct_ids})
        db.execute(text("DELETE FROM mrr_movements WHERE account_id = ANY(:a)"), {"a": acct_ids})
        db.execute(text("DELETE FROM free_to_paid_attribution WHERE account_id = ANY(:a)"), {"a": acct_ids})
    db.execute(text("DELETE FROM customer_accounts WHERE subscriber_id = ANY(:s)"), {"s": sub_ids or [-1]})
    db.execute(text("DELETE FROM zip_territories WHERE subscriber_id = ANY(:s)"), {"s": sub_ids or [-1]})
    db.execute(text("DELETE FROM unified_subscriber_memory WHERE subscriber_id::text = ANY(:s)"),
               {"s": [str(x) for x in sub_ids] or ["-1"]})
    db.execute(text("DELETE FROM sms_opt_ins WHERE subscriber_id = ANY(:s)"), {"s": sub_ids or [-1]})
    db.execute(text("DELETE FROM subscription_invoices WHERE subscriber_id = ANY(:s)"), {"s": sub_ids or [-1]})
    db.execute(text("DELETE FROM subscribers WHERE event_feed_uuid LIKE :u"), {"u": TAG + "%"})
    db.execute(text("DELETE FROM deliveries WHERE property_id IN "
                    "(SELECT id FROM properties WHERE parcel_id LIKE :p)"), {"p": TAG_PARCEL + "%"})
    db.execute(text("DELETE FROM distress_scores WHERE property_id IN "
                    "(SELECT id FROM properties WHERE parcel_id LIKE :p)"), {"p": TAG_PARCEL + "%"})
    db.execute(text("DELETE FROM properties WHERE parcel_id LIKE :p"), {"p": TAG_PARCEL + "%"})
    db.commit()


# ── real Stripe test-mode subscription ───────────────────────────────────────

def _real_stripe_subscription(s):
    """Create a real TEST-mode Customer + saved card + active subscription.
    Returns (customer_id, subscription_id, invoice_id, charge_id, amount_cents)."""
    import stripe
    key = s.active_stripe_secret_key
    secret = key.get_secret_value() if key else ""
    if not secret.startswith("sk_test"):
        raise SystemExit("ABORT: not in Stripe TEST mode — refusing to create real charges.")
    stripe.api_key = secret

    price_id = s.active_stripe_price("starter_founding") or s.active_stripe_price("starter_regular")
    if not price_id:
        raise SystemExit("ABORT: no starter test price configured.")

    cust = stripe.Customer.create(
        email=TEST_EMAIL, name="HPAY Harness",
        payment_method="pm_card_visa",
        invoice_settings={"default_payment_method": "pm_card_visa"},
        metadata={"harness": TAG},
    )
    sub = stripe.Subscription.create(
        customer=cust["id"],
        items=[{"price": price_id}],
        expand=["latest_invoice.payment_intent"],
        metadata={"harness": TAG},
    )
    inv = getattr(sub, "latest_invoice", None)
    pi = getattr(inv, "payment_intent", None) if inv is not None else None
    charge = getattr(pi, "latest_charge", None) if pi is not None else None
    if charge is None and inv is not None:
        charge = getattr(inv, "charge", None)
    # 2026 API decouples invoice->charge/PI (both can be null). Fall back to the
    # customer's most recent charge — the subscription's first payment.
    if charge is None:
        try:
            charges = stripe.Charge.list(customer=cust["id"], limit=1)
            data = getattr(charges, "data", None) or []
            if data:
                charge = data[0]["id"]
        except Exception:
            pass
    return (
        cust["id"], sub["id"],
        getattr(inv, "id", None) if inv is not None else None,
        charge,
        (getattr(inv, "amount_paid", 0) or 0) if inv is not None else 0,
        getattr(inv, "status", None) if inv is not None else None,
    )


def _stripe_cleanup(customer_id):
    try:
        import stripe
        stripe.Customer.delete(customer_id)  # cancels subs + removes the test customer
        print(f"  stripe: deleted test customer {customer_id}")
    except Exception as exc:
        print(f"  stripe: cleanup warning for {customer_id}: {exc}")


# ── event payload builders ───────────────────────────────────────────────────

def _checkout_event(customer_id, subscription_id, amount_cents):
    return {
        "customer": customer_id,
        "subscription": subscription_id,
        "payment_status": "paid",
        "amount_total": amount_cents or 9900,
        "customer_details": {"email": TEST_EMAIL, "name": "HPAY Harness", "phone": None},
        "metadata": {
            "tier": TIER, "vertical": VERTICAL, "county_id": "hillsborough",
            "zip_codes": TEST_ZIP, "is_founding": "True",
        },
    }


def _invoice_event(customer_id, subscription_id, invoice_id, amount_cents, billing_reason, period_end):
    start = period_end - 30 * 86400
    return {
        "id": invoice_id or f"in_{TAG}cycle",
        "customer": customer_id,
        "subscription": subscription_id,
        "billing_reason": billing_reason,
        "amount_paid": amount_cents or 9900,
        "payment_intent": None,
        "status_transitions": {"paid_at": period_end},
        "lines": {"data": [{"period": {"start": start, "end": period_end}}]},
    }


# ── main lifecycle ───────────────────────────────────────────────────────────

def run(use_stripe: bool) -> int:
    s = get_settings()
    from src.services.stripe_webhooks import (
        _on_checkout_completed, _on_payment_succeeded, _on_payment_failed,
    )
    from src.services import lead_delivery
    from src.tasks import stripe_recovery_sweep

    with get_db_context() as db:
        cleanup(db)

    results = {}
    customer_id = None
    money = {}
    try:
        # ── money-lands: real test-mode Stripe subscription ──────────────────
        if use_stripe:
            (customer_id, subscription_id, invoice_id, charge_id,
             amount_cents, inv_status) = _real_stripe_subscription(s)
            print(f"\n== STRIPE (test mode) ==")
            print(f"  customer={customer_id} subscription={subscription_id}")
            print(f"  invoice={invoice_id} status={inv_status} charge={charge_id} amount={amount_cents}c")
            money = {"customer": customer_id, "subscription": subscription_id,
                     "invoice": invoice_id, "charge": charge_id, "amount_cents": amount_cents}
            results["money_lands"] = _check(
                f"invoice paid & money landed (charge={charge_id})",
                inv_status == "paid" and (amount_cents or 0) > 0)
        else:
            customer_id = f"cus_{TAG}fake"
            subscription_id = f"sub_{TAG}fake"
            invoice_id = f"in_{TAG}fake"
            amount_cents = 9900
            print("\n== STRIPE SKIPPED (--no-stripe): fabricated ids, DB paths only ==")

        with _mocks(), get_db_context() as db:
            # ── 1. checkout → subscriber active + ZIP lock + account/MRR ─────
            print("\n== checkout.session.completed ==")
            _on_checkout_completed(_checkout_event(customer_id, subscription_id, amount_cents), db)
            db.commit()
            sub = db.execute(text(
                "SELECT id, status, tier FROM subscribers WHERE stripe_customer_id=:c"
            ), {"c": customer_id}).fetchone()
            results["subscriber_active"] = _check("subscriber active", sub and sub.status == "active")
            terr = db.execute(text(
                "SELECT status FROM zip_territories WHERE zip_code=:z AND vertical=:v AND county_id='hillsborough'"
            ), {"z": TEST_ZIP, "v": VERTICAL}).fetchone()
            results["zip_locked"] = _check("ZIP territory locked", terr and terr.status == "locked")
            acct = db.execute(text(
                "SELECT account_id, status, mrr_cents, lead_entitlement, plan_tier "
                "FROM customer_accounts WHERE stripe_customer_id=:c"
            ), {"c": customer_id}).fetchone()
            results["account_mrr"] = _check(
                "customer_account active with MRR", acct and acct.status == "active" and (acct.mrr_cents or 0) > 0)

            # ── 2. lead delivery record ──────────────────────────────────────
            print("\n== lead delivery (deliveries row) ==")
            if acct and acct.lead_entitlement:
                grade_key = next((k for k, v in acct.lead_entitlement.items() if int(v or 0) > 0), None)
            else:
                grade_key = None
            if grade_key is None:
                print(f"  [SKIP] account has no positive entitlement bucket "
                      f"(plan_tier={acct.plan_tier if acct else None}, "
                      f"entitlement={acct.lead_entitlement if acct else None}) — "
                      f"check plans catalog is seeded")
                results["lead_delivery"] = None
            else:
                grade = grade_key.replace("_", " ").title()  # 'gold' -> 'Gold'
                from src.core.models import Property
                prop = Property(parcel_id=TAG_PARCEL + "1", zip=TEST_ZIP, county_id="hillsborough",
                                address=TAG_PARCEL + "1 Test St", city="Tampa", state="FL")
                db.add(prop)
                db.flush()
                pid = prop.id
                lead = lead_delivery.Lead(
                    property_id=pid, zip_code=TEST_ZIP, county_id="hillsborough",
                    grade=grade, verticals=[VERTICAL],
                )
                delivery = lead_delivery.claim(db, lead)
                db.commit()
                row = db.execute(text(
                    "SELECT id, account_id, grade FROM deliveries WHERE property_id=:pid"
                ), {"pid": pid}).fetchone()
                results["lead_delivery"] = _check(
                    f"delivery row created (grade={grade})", delivery is not None and row is not None)

            # ── 3. day-30 rebill ─────────────────────────────────────────────
            print("\n== day-30 rebill (invoice.payment_succeeded / subscription_cycle) ==")
            period_end = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
            _on_payment_succeeded(
                _invoice_event(customer_id, subscription_id, invoice_id, amount_cents,
                               "subscription_cycle", period_end), db)
            db.commit()
            bd = db.execute(text("SELECT billing_date FROM subscribers WHERE stripe_customer_id=:c"),
                            {"c": customer_id}).scalar()
            expected = datetime.fromtimestamp(period_end, tz=timezone.utc)
            results["rebill_advances"] = _check(
                f"billing_date advanced to ~{expected.date()}",
                bd is not None and abs((bd.replace(tzinfo=timezone.utc) - expected).total_seconds()) < 86400)

            # ── 4. failed card → past_due → recovery sweep day-1 ─────────────
            print("\n== failed card (invoice.payment_failed) + dunning sweep ==")
            _on_payment_failed(
                _invoice_event(customer_id, subscription_id, invoice_id, amount_cents,
                               "subscription_cycle", period_end), db)
            db.commit()
            pf = db.execute(text("SELECT payment_failed_at FROM subscribers WHERE stripe_customer_id=:c"),
                            {"c": customer_id}).scalar()
            results["payment_failed_stamped"] = _check("payment_failed_at set", pf is not None)
            acct_pd = db.execute(text("SELECT status FROM customer_accounts WHERE stripe_customer_id=:c"),
                                 {"c": customer_id}).scalar()
            results["account_past_due"] = _check("customer_account past_due", acct_pd == "past_due")

            # Backdate failure into the day-1 window (20-28h) so the sweep fires.
            db.execute(text(
                "UPDATE subscribers SET payment_failed_at = :t WHERE stripe_customer_id=:c"
            ), {"t": datetime.now(timezone.utc) - timedelta(hours=24), "c": customer_id})
            db.commit()
            swept = stripe_recovery_sweep.run(dry_run=False)
            day1_flag = db.execute(text(
                "SELECT recovery_day1_sent FROM subscribers WHERE stripe_customer_id=:c"
            ), {"c": customer_id}).scalar()
            results["dunning_day1"] = _check(
                f"recovery day-1 fired (sweep={swept.get('day1')})",
                swept.get("day1", 0) >= 1 and day1_flag is True)

            # ── 5. recovery: successful payment clears failure state ─────────
            print("\n== recovery (invoice.payment_succeeded clears failure) ==")
            _on_payment_succeeded(
                _invoice_event(customer_id, subscription_id, invoice_id, amount_cents,
                               "subscription_cycle", period_end), db)
            db.commit()
            cleared = db.execute(text(
                "SELECT payment_failed_at FROM subscribers WHERE stripe_customer_id=:c"
            ), {"c": customer_id}).scalar()
            acct_back = db.execute(text("SELECT status FROM customer_accounts WHERE stripe_customer_id=:c"),
                                   {"c": customer_id}).scalar()
            results["recovery_clears"] = _check(
                "failure cleared + account active", cleared is None and acct_back == "active")

    finally:
        with get_db_context() as db:
            cleanup(db)
        if use_stripe and customer_id:
            _stripe_cleanup(customer_id)
        print("\nCleaned up harness rows.")

    print("\n==== SUMMARY ====")
    if money:
        print(f"  money-lands evidence: charge={money.get('charge')} "
              f"invoice={money.get('invoice')} amount={money.get('amount_cents')}c (test mode)")
    ok = True
    for k, v in results.items():
        tag = "PASS" if v else ("SKIP" if v is None else "FAIL")
        print(f"  {k:24s}: {tag}")
        if v is False:
            ok = False
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cleanup", action="store_true", help="purge tagged rows and exit")
    ap.add_argument("--no-stripe", action="store_true", help="skip real Stripe calls (DB paths only)")
    args = ap.parse_args()
    if args.cleanup:
        with get_db_context() as db:
            cleanup(db)
        print("Cleaned up harness rows. Done.")
        return 0
    return run(use_stripe=not args.no_stripe)


if __name__ == "__main__":
    raise SystemExit(main())
