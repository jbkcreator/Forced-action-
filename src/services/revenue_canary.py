"""
QUALITY-v2.2 Q4 — continuous revenue-path canaries.

Six synthetic checks, run every 5 minutes (client decision E1) by
src/tasks/revenue_canary_sweep.py: checkout, payment_link, entitlement,
delivery, mail, model_api. Proves the revenue path works RIGHT NOW —
src/tasks/revenue_fulfillment_heartbeat.py's daily batch reconciliation
answers a different question (did yesterday's Stripe total match our
ledger), after the fact. This module is forward-looking and synthetic.

ALWAYS Stripe test mode (decision E2), regardless of the platform's global
STRIPE_TEST_MODE toggle (OFF in production — see config/settings.py's
active_stripe_secret_key, which this module never calls). Reads
settings.stripe_test_secret_key directly and hard-aborts if the resolved
key doesn't start with "sk_test" — the same safety gate already used by
scripts/harness/lead_pack_e2e_local.py's --via-stripe mode. A canary that
quietly ran against live keys would create 288 real charges/day and
corrupt revenue_fulfillment_heartbeat.py's Stripe-vs-ledger reconciliation.

checkout / payment_link genuinely round-trip Stripe's API in test mode —
"checkout code runs for real; only the final charge is faked" (E2).
entitlement / delivery exercise the exact DB-write idiom production
entitlement/delivery logic depends on (record_revenue's INSERT..ON
CONFLICT DO NOTHING; SentLead's INSERT..ON CONFLICT (subscriber_id,
property_id) DO UPDATE) against a dedicated revenue_canary_probe_log table
— NOT by calling stripe_webhooks._on_payment_intent_succeeded /
_on_lead_unlock_payment directly (which would also fire GHL pushes and
real emails every 5 minutes) and NOT by creating a permanent synthetic
Subscriber/Property row (this codebase has no is_test/is_canary flag real
sweeps could use to skip one — confirmed by grep).
"""
from __future__ import annotations

import logging
import smtplib
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.settings import get_settings

logger = logging.getLogger(__name__)

CHECK_NAMES: tuple[str, ...] = (
    "checkout", "payment_link", "entitlement", "delivery", "mail", "model_api",
)


@dataclass
class CanaryResult:
    name: str
    ok: bool
    detail: str
    latency_ms: int


def _ms(t0: float) -> int:
    return int((time.monotonic() - t0) * 1000)


def _require_stripe_test_key() -> str:
    """Return the Stripe TEST secret key, hard-aborting if it doesn't look
    like a test key. NEVER settings.active_stripe_secret_key — see module
    docstring."""
    settings = get_settings()
    key = settings.stripe_test_secret_key
    if not key:
        raise RuntimeError("STRIPE_TEST_SECRET_KEY not configured — revenue canary cannot run")
    value = key.get_secret_value()
    if not value.startswith("sk_test"):
        raise RuntimeError(
            "Refusing to run revenue canary: resolved Stripe key does not "
            "start with 'sk_test' — this must never be a live key."
        )
    return value


# ── checkout ──────────────────────────────────────────────────────────────

def _check_checkout(force_fail: bool = False) -> CanaryResult:
    """Create + confirm a real Stripe TEST-mode PaymentIntent using the
    guaranteed-success test payment method pm_card_visa — the exact pattern
    scripts/harness/lead_pack_e2e_local.py's --via-stripe mode already uses
    in this repo. Proves the checkout code path round-trips Stripe for
    real; only the money is fake (test mode + test card)."""
    t0 = time.monotonic()
    if force_fail:
        return CanaryResult("checkout", False, "[--kill-canary] forced failure", _ms(t0))
    try:
        import stripe
        stripe.api_key = _require_stripe_test_key()
        pi = stripe.PaymentIntent.create(
            amount=100, currency="usd", payment_method="pm_card_visa",
            confirm=True,
            automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
            metadata={"product": "revenue_canary_checkout"},
        )
        status = pi["status"]
        ok = status == "succeeded"
        return CanaryResult("checkout", ok, f"PaymentIntent {pi['id']} status={status}", _ms(t0))
    except Exception as exc:
        logger.error("[RevenueCanary] checkout check failed", exc_info=True)
        return CanaryResult("checkout", False, f"exception: {exc}", _ms(t0))


# ── payment_link ──────────────────────────────────────────────────────────

def _check_payment_link(force_fail: bool = False) -> CanaryResult:
    """Create a real Stripe TEST-mode Checkout Session in payment mode —
    the same stripe.checkout.Session.create() call
    src/services/stripe_service.py's hot-lead-unlock link creation makes for
    a real payment link. Only verifies session creation succeeds (a working
    link) — never completed, so no card is required.

    Uses an inline price_data rather than a configured test price id: no
    STRIPE_TEST_PRICE_HOT_LEAD_UNLOCK setting exists in this codebase today
    (confirmed — config/settings.py has no such field), and this canary's
    job is to prove Session.create() round-trips Stripe, not to exercise
    one specific product's price catalog entry."""
    t0 = time.monotonic()
    if force_fail:
        return CanaryResult("payment_link", False, "[--kill-canary] forced failure", _ms(t0))
    try:
        import stripe
        stripe.api_key = _require_stripe_test_key()
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            success_url="https://forcedactionleads.com/canary?ok=true",
            cancel_url="https://forcedactionleads.com/canary?ok=false",
            metadata={"product": "revenue_canary_payment_link"},
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {"name": "Revenue Canary Payment Link"},
                    "unit_amount": 100,
                },
                "quantity": 1,
            }],
        )
        ok = bool(session.get("url"))
        return CanaryResult(
            "payment_link", ok,
            f"Session {session['id']} url={'set' if ok else 'MISSING'}", _ms(t0),
        )
    except Exception as exc:
        logger.error("[RevenueCanary] payment_link check failed", exc_info=True)
        return CanaryResult("payment_link", False, f"exception: {exc}", _ms(t0))


# ── entitlement / delivery ──────────────────────────────────────────────────

def _probe_upsert(db: Session, check_name: str, force_fail: bool = False) -> CanaryResult:
    """Shared DB round-trip for entitlement/delivery. See module docstring
    for why this hits a dedicated revenue_canary_probe_log table rather
    than platform_revenue_ledger / sent_leads or a real webhook handler."""
    t0 = time.monotonic()
    if force_fail:
        return CanaryResult(check_name, False, "[--kill-canary] forced failure", _ms(t0))
    tick = int(time.time())
    try:
        db.execute(sa_text("""
            INSERT INTO revenue_canary_probe_log (check_name, probe_value, updated_at)
            VALUES (:name, :tick, NOW())
            ON CONFLICT (check_name) DO UPDATE
                SET probe_value = EXCLUDED.probe_value, updated_at = NOW()
        """), {"name": check_name, "tick": tick})
        db.commit()
        row = db.execute(sa_text(
            "SELECT probe_value, updated_at FROM revenue_canary_probe_log WHERE check_name = :name"
        ), {"name": check_name}).fetchone()
        if row is None or int(row.probe_value) != tick:
            return CanaryResult(check_name, False, f"round-trip mismatch: wrote {tick}, read {row}", _ms(t0))
        return CanaryResult(check_name, True, f"probe_value={tick} updated_at={row.updated_at}", _ms(t0))
    except Exception as exc:
        db.rollback()
        logger.error("[RevenueCanary] %s check failed", check_name, exc_info=True)
        return CanaryResult(check_name, False, f"exception: {exc}", _ms(t0))


def _check_entitlement(db: Session, force_fail: bool = False) -> CanaryResult:
    """Mirrors record_revenue()'s INSERT..ON CONFLICT DO NOTHING idiom —
    proves the entitlement-bookkeeping write path is alive."""
    return _probe_upsert(db, "entitlement", force_fail)


def _check_delivery(db: Session, force_fail: bool = False) -> CanaryResult:
    """Mirrors SentLead's INSERT..ON CONFLICT (subscriber_id, property_id)
    DO UPDATE idiom — proves the delivery-bookkeeping write path is alive."""
    return _probe_upsert(db, "delivery", force_fail)


# ── mail ──────────────────────────────────────────────────────────────────

def _check_mail(force_fail: bool = False) -> CanaryResult:
    """SMTP send to a canary address, verify no exception (decision F11).
    Uses smtplib directly rather than src/services/email.py:send_email() —
    that function's Do-Not-Contact suppression gate does a DB lookup this
    infra probe shouldn't depend on, and CANARY_MAIL_TO is an ops inbox,
    never a real subscriber."""
    t0 = time.monotonic()
    if force_fail:
        return CanaryResult("mail", False, "[--kill-canary] forced failure", _ms(t0))
    settings = get_settings()
    if not all([settings.smtp_host, settings.smtp_user, settings.smtp_pass]):
        return CanaryResult("mail", False, "SMTP not configured", _ms(t0))
    to_addr = settings.canary_mail_to or settings.alert_email
    if not to_addr:
        return CanaryResult("mail", False, "no CANARY_MAIL_TO or ALERT_EMAIL configured", _ms(t0))
    try:
        msg = MIMEText(f"QUALITY-v2.2 Q4 mail probe — {datetime.now(timezone.utc).isoformat()}")
        msg["Subject"] = "[FA][Canary] mail probe"
        msg["From"] = settings.email_from or settings.smtp_user
        msg["To"] = to_addr
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_pass.get_secret_value())
            server.sendmail(msg["From"], [to_addr], msg.as_string())
        return CanaryResult("mail", True, f"sent to {to_addr}", _ms(t0))
    except Exception as exc:
        logger.error("[RevenueCanary] mail check failed", exc_info=True)
        return CanaryResult("mail", False, f"exception: {exc}", _ms(t0))


# ── model_api ─────────────────────────────────────────────────────────────

def _check_model_api(force_fail: bool = False) -> CanaryResult:
    """5-token Haiku call, verify a non-error response (decision F11).
    db=None so no api_usage_logs row is written — this is an infra probe,
    not a billed workflow, and force_tier="haiku" bypasses _TASK_ROUTING
    entirely so no new routing entry is needed."""
    t0 = time.monotonic()
    if force_fail:
        return CanaryResult("model_api", False, "[--kill-canary] forced failure", _ms(t0))
    try:
        from src.services.claude_router import call_claude
        response_text = call_claude(
            task_type="revenue_canary_probe",
            messages=[{"role": "user", "content": "Reply with the single word: OK"}],
            max_tokens=5,
            force_tier="haiku",
            db=None,
        )
        ok = bool(response_text and response_text.strip())
        return CanaryResult("model_api", ok, f"response={response_text!r}", _ms(t0))
    except Exception as exc:
        logger.error("[RevenueCanary] model_api check failed", exc_info=True)
        return CanaryResult("model_api", False, f"exception: {exc}", _ms(t0))


# ── runner ────────────────────────────────────────────────────────────────

def run_all_checks(db: Session, kill: Optional[str] = None) -> list[CanaryResult]:
    """Run all six canary checks. `kill` forces the named check to report
    failure — the --kill-canary deliberate-break acceptance test mechanism
    (decision E5, spec §1.8)."""
    if kill is not None and kill not in CHECK_NAMES:
        raise ValueError(f"Unknown canary check {kill!r}. Valid: {CHECK_NAMES}")
    return [
        _check_checkout(force_fail=(kill == "checkout")),
        _check_payment_link(force_fail=(kill == "payment_link")),
        _check_entitlement(db, force_fail=(kill == "entitlement")),
        _check_delivery(db, force_fail=(kill == "delivery")),
        _check_mail(force_fail=(kill == "mail")),
        _check_model_api(force_fail=(kill == "model_api")),
    ]
