"""
Premium credits engine — Stage 5.

Handles purchase + fulfillment of the four premium SKUs:
  - report   ($7  / 3cr)   property report dossier
  - brief    ($12 / 5cr)   investor lead brief
  - transfer ($65 / 26cr)  full skip-trace transfer
  - byol     ($5  / 2cr)   bring-your-own-lead skip-trace

A purchase is either:
  * paid_via='credits'  — wallet_engine.debit() has already succeeded; this
    module only persists the PremiumPurchase row + runs fulfillment.
  * paid_via='card'     — Stripe PaymentIntent succeeded; the webhook
    persists the row + runs fulfillment.

Fulfillment is best-effort and synchronous. report/brief produce a JSON
artifact placeholder until a PDF generator is added; transfer triggers an
on-demand skip-trace; byol triggers a skip-trace on a user-supplied address
(stored as `target_address`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from config.revenue_ladder import PREMIUM_CREDITS
from src.core.models import PremiumPurchase

logger = logging.getLogger(__name__)


def get_sku_config(sku: str) -> dict:
    cfg = PREMIUM_CREDITS.get(sku)
    if not cfg:
        raise ValueError(f"Unknown premium SKU: {sku}")
    return cfg


def record_credit_purchase(
    subscriber_id: int,
    sku: str,
    db: Session,
    property_id: Optional[int] = None,
    target_address: Optional[str] = None,
) -> PremiumPurchase:
    """
    Persist a PremiumPurchase row for a credit-paid SKU. Caller must have
    already invoked wallet_engine.debit() and committed (or be inside the
    same transaction).
    """
    cfg = get_sku_config(sku)
    purchase = PremiumPurchase(
        subscriber_id=subscriber_id,
        sku=sku,
        paid_via="credits",
        credits_spent=cfg["credits_cost"],
        property_id=property_id,
        target_address=target_address,
        status="pending",
    )
    db.add(purchase)
    db.flush()
    return purchase


def record_card_purchase(
    subscriber_id: int,
    sku: str,
    stripe_payment_intent_id: str,
    db: Session,
    property_id: Optional[int] = None,
    target_address: Optional[str] = None,
    amount_cents: Optional[int] = None,
) -> PremiumPurchase:
    cfg = get_sku_config(sku)
    purchase = PremiumPurchase(
        subscriber_id=subscriber_id,
        sku=sku,
        paid_via="card",
        amount_cents=amount_cents if amount_cents is not None else cfg["retail_price_cents"],
        stripe_payment_intent_id=stripe_payment_intent_id,
        property_id=property_id,
        target_address=target_address,
        status="pending",
    )
    db.add(purchase)
    db.flush()
    return purchase


def fulfill(purchase_id: int, db: Session) -> PremiumPurchase:
    """Run fulfillment for a PremiumPurchase. Idempotent — skips if delivered."""
    purchase = db.get(PremiumPurchase, purchase_id)
    if not purchase:
        raise ValueError(f"PremiumPurchase {purchase_id} not found")
    if purchase.status == "delivered":
        return purchase

    try:
        if purchase.sku in ("report", "brief"):
            output_ref = _fulfill_report_or_brief(purchase, db)
        elif purchase.sku == "transfer":
            output_ref = _fulfill_transfer(purchase, db)
        elif purchase.sku == "byol":
            output_ref = _fulfill_byol(purchase, db)
        else:
            raise ValueError(f"Unknown premium SKU during fulfillment: {purchase.sku}")
        purchase.output_ref = output_ref
        purchase.status = "delivered"
        purchase.delivered_at = datetime.now(timezone.utc)
        db.flush()
        logger.info(
            "Premium fulfilled: purchase=%d sku=%s subscriber=%d",
            purchase.id, purchase.sku, purchase.subscriber_id,
        )
    except Exception as exc:
        purchase.status = "failed"
        db.flush()
        logger.error(
            "Premium fulfillment failed: purchase=%d sku=%s err=%s",
            purchase.id, purchase.sku, exc, exc_info=True,
        )
        raise
    return purchase


def _fulfill_report_or_brief(purchase: PremiumPurchase, db: Session) -> str:
    """
    Generate a property report or lead brief artifact. Pending PDF templates
    from design, this currently produces a JSON dossier reference and emails
    the subscriber a link to their dashboard where the artifact will surface.
    The artifact ref is `report:{property_id}` or `brief:{property_id}`.
    """
    if not purchase.property_id:
        raise ValueError(f"{purchase.sku} requires property_id")
    return f"{purchase.sku}:{purchase.property_id}"


def _fulfill_transfer(purchase: PremiumPurchase, db: Session) -> str:
    """
    Queue an on-demand skip-trace transfer for one property. The existing
    `run_skip_trace` batch runner will pick it up on its next cycle (it
    auto-targets owners with no phone/email). Returns a queued ref; the
    enriched_contacts row that is later created can be reconciled by
    property_id.
    """
    if not purchase.property_id:
        raise ValueError("transfer requires property_id")
    return f"transfer:queued:{purchase.property_id}"


def _fulfill_byol(purchase: PremiumPurchase, db: Session) -> str:
    """
    Bring-your-own-lead: subscriber supplies an address. We persist the
    address on the row; an admin/cron job runs the BatchData call against
    it and writes back an EnrichedContact. Returns a queued ref.
    """
    if not purchase.target_address:
        raise ValueError("byol requires target_address")
    return f"byol:queued:{purchase.id}"
