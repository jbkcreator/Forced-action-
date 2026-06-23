"""
White-label tier Stripe billing helpers (Stage 12 / fa056).

Handles:
  - Checkout session creation for $2,500/mo and $5,000/mo plans
  - Stripe Customer Portal (upgrade/cancellation)
  - Webhook event processing for WL-specific events
    (identified by metadata.wl_client_id)
"""

import logging
import threading
from datetime import datetime, timezone
from typing import Optional

import stripe
from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.services.email import send_email
from src.services.white_label_auth import send_activation_email

logger = logging.getLogger(__name__)

_PLAN_PRICE_CENTS = {
    "standard": 250_000,
    "premium":  500_000,
}


def _init_stripe() -> None:
    s = get_settings()
    key = s.active_stripe_secret_key
    if not key:
        raise RuntimeError("Stripe secret key not configured")
    stripe.api_key = key.get_secret_value()


def _price_id(plan_tier: str) -> str:
    if plan_tier not in ("standard", "premium"):
        raise ValueError(f"Unknown WL plan tier: {plan_tier!r}")
    # active_stripe_price() picks test vs live based on STRIPE_TEST_MODE
    price = get_settings().active_stripe_price(f"wl_{plan_tier}")
    if not price:
        raise ValueError(f"STRIPE_PRICE_WL_{plan_tier.upper()} not configured")
    return price


# ---------------------------------------------------------------------------
# Checkout
# ---------------------------------------------------------------------------

def create_wl_checkout(client_id: int, company_slug: str, admin_email: str,
                       plan_tier: str, include_trial: bool = True) -> dict:
    """
    Create a Stripe Checkout Session for a white-label plan.
    Returns {url, session_id}.
    """
    _init_stripe()
    s = get_settings()

    trial_days = s.wl_trial_period_days if include_trial else 0

    subscription_data: dict = {
        "metadata": {"wl_client_id": str(client_id), "plan_tier": plan_tier},
    }
    if trial_days > 0:
        subscription_data["trial_period_days"] = trial_days

    session = stripe.checkout.Session.create(
        mode="subscription",
        customer_email=admin_email,
        line_items=[{"price": _price_id(plan_tier), "quantity": 1}],
        subscription_data=subscription_data,
        metadata={"wl_client_id": str(client_id), "plan_tier": plan_tier},
        success_url=f"{s.wl_frontend_base_url}/wl/dashboard?checkout=success",
        cancel_url=f"{s.wl_frontend_base_url}/wl/billing?checkout=cancelled",
        allow_promotion_codes=True,
    )
    return {"url": session.url, "session_id": session.id}


def create_billing_portal_session(stripe_customer_id: str) -> str:
    """Return a Stripe Billing Portal URL for the client."""
    _init_stripe()
    s = get_settings()
    session = stripe.billing_portal.Session.create(
        customer=stripe_customer_id,
        return_url=f"{s.wl_frontend_base_url}/wl/dashboard/billing",
    )
    return session.url


def get_or_create_stripe_customer(client_id: int, admin_email: str, company_name: str) -> str:
    """Return existing stripe_customer_id or create a new Stripe customer."""
    _init_stripe()
    customer = stripe.Customer.create(
        email=admin_email,
        name=company_name,
        metadata={"wl_client_id": str(client_id)},
    )
    return customer.id


# ---------------------------------------------------------------------------
# Webhook processing
# ---------------------------------------------------------------------------

def is_wl_event(event: stripe.Event) -> bool:
    """Return True if this Stripe event belongs to a white-label client."""
    obj = event.data.object
    meta = getattr(obj, "metadata", {}) or {}
    return "wl_client_id" in meta


def handle_wl_webhook_event(event: stripe.Event, db) -> None:
    """
    Route a WL Stripe event to the appropriate handler.
    Called from the /webhooks/stripe/white-label endpoint.
    """
    handlers = {
        "checkout.session.completed":    _on_checkout_completed,
        "invoice.payment_succeeded":     _on_payment_succeeded,
        "invoice.payment_failed":        _on_payment_failed,
        "customer.subscription.updated": _on_subscription_updated,
        "customer.subscription.deleted": _on_subscription_deleted,
    }
    handler = handlers.get(event.type)
    if handler:
        try:
            handler(event.data.object, db)
        except Exception as exc:
            logger.error("[wl_billing] webhook handler failed for %s: %s", event.type, exc, exc_info=True)
    else:
        logger.debug("[wl_billing] unhandled WL event type: %s", event.type)


def _client_id_from_obj(obj) -> Optional[int]:
    meta = getattr(obj, "metadata", {}) or {}
    raw = meta.get("wl_client_id")
    return int(raw) if raw else None


def _prewarm_clay_for_client(client_id: int, counties: list, verticals: list) -> None:
    """Pre-warm Clay contractor cache for all county+vertical pairs. Daemon thread only."""
    from src.services import clay_service
    from src.core.database import get_db_context
    for county in (counties or []):
        for vertical in (verticals or []):
            try:
                with get_db_context() as db:
                    clay_service.get_or_refresh_enrichment(client_id, county, vertical, db)
                logger.info("[wl_audit] clay_prewarm ok client_id=%d county=%s vertical=%s", client_id, county, vertical)
            except Exception as exc:
                logger.warning("[wl_audit] clay_prewarm failed client_id=%d county=%s vertical=%s err=%s", client_id, county, vertical, exc)


def _on_checkout_completed(session, db) -> None:
    client_id = _client_id_from_obj(session)
    if not client_id:
        return

    plan_tier = (session.metadata or {}).get("plan_tier", "standard")
    price_cents = _PLAN_PRICE_CENTS.get(plan_tier, 250_000)
    sub_id = session.subscription

    row = db.execute(
        sa_text("""
            SELECT id, admin_email, company_name, status, stripe_subscription_id
              FROM white_label_clients WHERE id = :cid
        """),
        {"cid": client_id},
    ).fetchone()
    if not row:
        logger.warning("[wl_billing] checkout.completed: client %d not found", client_id)
        return

    # Idempotency: skip only if THIS exact subscription is already recorded.
    # (Keying on status=='active' is wrong now that email-verification activates
    #  the account before checkout — it would drop the plan entirely.)
    if sub_id and row.stripe_subscription_id == sub_id:
        logger.info("[wl_billing] client %d already has subscription %s, skipping", client_id, sub_id)
        return

    customer_id = session.customer

    # Pull the trial window off the Stripe subscription (authoritative source).
    trial_ends_at = None
    if sub_id:
        try:
            sub = stripe.Subscription.retrieve(sub_id)
            if getattr(sub, "trial_end", None):
                trial_ends_at = datetime.fromtimestamp(sub.trial_end, tz=timezone.utc)
        except Exception as exc:
            logger.warning("[wl_billing] could not retrieve subscription %s for trial_end: %s", sub_id, exc)

    db.execute(
        sa_text("""
            UPDATE white_label_clients
               SET status = 'active',
                   stripe_customer_id = :cid_stripe,
                   stripe_subscription_id = :sub_id,
                   plan_tier = :plan_tier,
                   plan_price_cents = :price_cents,
                   trial_ends_at = :trial_ends_at,
                   activated_at = COALESCE(activated_at, now()),
                   updated_at = now()
             WHERE id = :id
        """),
        {
            "cid_stripe": customer_id,
            "sub_id": sub_id,
            "plan_tier": plan_tier,
            "price_cents": price_cents,
            "trial_ends_at": trial_ends_at,
            "id": client_id,
        },
    )
    db.commit()
    logger.info(
        "[wl_audit] event=checkout_completed client_id=%d plan=%s sub=%s trial_ends=%s",
        client_id, plan_tier, sub_id, trial_ends_at,
    )

    send_activation_email(row.admin_email, row.company_name)
    logger.info(
        "[wl_billing] client %d subscribed (plan=%s, trial_ends=%s)",
        client_id, plan_tier, trial_ends_at,
    )

    # Pre-warm Clay contractor cache in the background (non-blocking)
    config = db.execute(
        sa_text("SELECT counties_enabled, verticals_enabled FROM white_label_clients WHERE id = :cid"),
        {"cid": client_id},
    ).fetchone()
    if config and (config.counties_enabled or config.verticals_enabled):
        threading.Thread(
            target=_prewarm_clay_for_client,
            args=(client_id, config.counties_enabled or [], config.verticals_enabled or []),
            daemon=True,
        ).start()


def _on_payment_succeeded(invoice, db) -> None:
    customer_id = invoice.customer
    db.execute(
        sa_text("""
            UPDATE white_label_clients
               SET status = 'active'
             WHERE stripe_customer_id = :cid AND status != 'active'
        """),
        {"cid": customer_id},
    )
    db.commit()
    logger.info("[wl_audit] event=payment_succeeded customer=%s", customer_id)


def _on_payment_failed(invoice, db) -> None:
    customer_id = invoice.customer
    row = db.execute(
        sa_text("SELECT id, admin_email, company_name FROM white_label_clients WHERE stripe_customer_id = :cid"),
        {"cid": customer_id},
    ).fetchone()
    if not row:
        return
    send_email(
        to=row.admin_email,
        subject=f"Action required — payment failed for {row.company_name}",
        body_text=(
            "Your Forced Action white-label subscription payment failed. "
            "Please update your payment method to avoid service interruption."
        ),
    )
    logger.info("[wl_billing] payment failed for client %d", row.id)
    logger.info("[wl_audit] event=payment_failed client_id=%d customer=%s", row.id, customer_id)


def _on_subscription_updated(subscription, db) -> None:
    status_map = {
        "active": "active",
        "past_due": "past_due",
        "unpaid": "churned",
        "canceled": "churned",
        "trialing": "active",
    }
    new_status = status_map.get(subscription.status, "active")
    db.execute(
        sa_text("""
            UPDATE white_label_clients
               SET status = :status,
                   stripe_subscription_id = :sub_id
             WHERE stripe_customer_id = :cid
        """),
        {"status": new_status, "sub_id": subscription.id, "cid": subscription.customer},
    )
    db.commit()
    logger.info(
        "[wl_audit] event=subscription_updated customer=%s sub=%s stripe_status=%s→mapped=%s",
        subscription.customer, subscription.id, subscription.status, new_status,
    )


def _on_subscription_deleted(subscription, db) -> None:
    row = db.execute(
        sa_text("SELECT id, admin_email, company_name FROM white_label_clients WHERE stripe_customer_id = :cid"),
        {"cid": subscription.customer},
    ).fetchone()
    if not row:
        return

    db.execute(
        sa_text("""
            UPDATE white_label_clients
               SET status = 'churned', churned_at = now()
             WHERE id = :id
        """),
        {"id": row.id},
    )
    db.commit()

    send_email(
        to=row.admin_email,
        subject=f"Your {row.company_name} subscription has been cancelled",
        body_text="Your white-label subscription has been cancelled. Contact support to reactivate.",
    )
    logger.info("[wl_billing] client %d churned", row.id)
    logger.info("[wl_audit] event=subscription_deleted client_id=%d customer=%s", row.id, subscription.customer)
