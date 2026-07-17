"""Centralized revenue & cost-attribution ledger.

Every purchase-confirmation path (lead_unlock, lead_pack, premium
report/brief, subscription invoice) writes exactly one row via
record_revenue() below — never a hand-rolled INSERT at the call site. This
is what lets Task 6.1's margin reporting and Task 6.2's budget gate both
read one table with no per-product knowledge, and lets a future product
integrate by making one call here instead of touching every consumer.

SentLead, LeadPackPurchase, PremiumPurchase, and SubscriptionInvoice remain
each product's own operational source of truth (idempotency keys,
exclusivity windows, refund/dispute tracking specific to that product) —
this module does not replace them, it records the same real-world payment
event a second time for reporting purposes.

Cost attribution: direct-purchase attribution (record_direct_cost_attribution)
is written at the same moment as the matching revenue row, keyed by the
already-known property_id. Zip-territory attribution has no purchase event
to hook into (ownership is continuous, not a discrete transaction) — see
src/tasks/zip_territory_cost_attribution_refresh.py for that path.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def stripe_payment_intent_ledger_id(payment_intent_id: str) -> int:
    """Deterministic 63-bit integer derived from a Stripe payment_intent id.

    Use as source_id (with a dedicated source_table) when the underlying
    product row doesn't get a fresh primary key per payment — e.g. SentLead
    has a hard UniqueConstraint(subscriber_id, property_id), so a $2.50
    lead_unlock and a later $150 hot_lead_unlock on the same property share
    one SentLead row. Keying the ledger off sent_leads.id would collide: the
    (source_table, source_id) uniqueness meant to absorb a retried webhook
    for the SAME charge would instead silently drop the second, genuinely
    different charge. Hashing the payment_intent id gives every distinct
    charge its own ledger identity while a retried webhook for the same
    payment_intent still hashes to the same value, so idempotency holds.
    """
    digest = hashlib.sha256(payment_intent_id.encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def record_revenue(
    db: Session,
    subscriber_id: int,
    product_type: str,
    amount_cents: int,
    source_table: str,
    source_id: int,
    property_id: Optional[int] = None,
    occurred_at: Optional[datetime] = None,
) -> None:
    """Insert one platform_revenue_ledger row. Idempotent on (source_table,
    source_id) — calling this twice for the same origin row is a no-op, so
    webhook retries can never double-record revenue.

    Best-effort by design (matches src/services/enrichment_log.py:log_usage's
    convention): a ledger-write failure must never block the underlying
    purchase confirmation it's reporting on. Callers should invoke this
    inside the same transaction/session as their own write (e.g. the
    existing db.begin_nested() block in _on_lead_unlock_payment) so a
    genuine DB outage rolls back consistently, while a duplicate insert from
    a retried webhook is silently absorbed by the unique constraint.
    """
    try:
        db.execute(text("""
            INSERT INTO platform_revenue_ledger (
                subscriber_id, product_type, amount_cents, property_id,
                source_table, source_id, occurred_at
            ) VALUES (
                :subscriber_id, :product_type, :amount_cents, :property_id,
                :source_table, :source_id, :occurred_at
            )
            ON CONFLICT (source_table, source_id) DO NOTHING
        """), {
            "subscriber_id": subscriber_id,
            "product_type": product_type,
            "amount_cents": amount_cents,
            "property_id": property_id,
            "source_table": source_table,
            "source_id": source_id,
            "occurred_at": occurred_at or datetime.now(),
        })
    except Exception:
        logger.warning(
            "[RevenueLedger] record_revenue failed: product_type=%s source_table=%s source_id=%s",
            product_type, source_table, source_id, exc_info=True,
        )
        return

    # Task 4.1 frozen control holdout — retention_v1's conversion is "any
    # paid action" (no single dedicated event), so it hooks the one place
    # every purchase-confirmation path already funnels through (see module
    # docstring) rather than each individual webhook handler. No-op for any
    # subscriber without a retention_holdout AbAssignment, so this fires
    # safely for every product_type, not just retention-nudged subscribers.
    # holdout_verdict enforces the "within 7 days" window separately via
    # ab_assignments.outcome_at, not here.
    from src.services.ab_engine import record_holdout_conversion
    record_holdout_conversion(subscriber_id, "retention_holdout", db)


def mark_ledger_refunded(
    db: Session,
    source_table: str,
    source_id: int,
    refunded_at: Optional[datetime] = None,
    refunded_amount_cents: Optional[int] = None,
) -> None:
    """Propagate a refund/reversal to the matching ledger row so revenue
    reporting excludes it (WHERE refunded_at IS NULL), without any consumer
    needing to know which product-specific table the refund actually lives
    on. Best-effort — a failure here must never block the underlying refund
    from processing.

    `refunded_amount_cents`: pass the actual amount refunded (e.g. Stripe's
    own amount_refunded) when the caller knows it — a partial refund must
    only net out the refunded portion, not the row's full amount_cents.
    Callers that don't pass it (or don't know the actual amount) get the
    prior full-refund-assumed behavior via the COALESCE fallback.
    """
    try:
        db.execute(text("""
            UPDATE platform_revenue_ledger
            SET refunded_at = :refunded_at,
                refunded_amount_cents = COALESCE(:refunded_amount_cents, amount_cents)
            WHERE source_table = :source_table AND source_id = :source_id
              AND refunded_at IS NULL
        """), {
            "source_table": source_table,
            "source_id": source_id,
            "refunded_at": refunded_at or datetime.now(),
            "refunded_amount_cents": refunded_amount_cents,
        })
    except Exception:
        logger.warning(
            "[RevenueLedger] mark_ledger_refunded failed: source_table=%s source_id=%s",
            source_table, source_id, exc_info=True,
        )


def record_direct_cost_attribution(
    db: Session,
    enrichment_usage_log_id: int,
    subscriber_id: int,
    property_id: int,
    attributed_cost_cents: int,
) -> None:
    """Insert one platform_cost_attribution row for a direct-purchase
    attribution (attribution_method='direct_purchase'). Idempotent on
    (enrichment_usage_log_id, subscriber_id) for this method — see the
    partial unique index in the fa110 migration.
    """
    try:
        db.execute(text("""
            INSERT INTO platform_cost_attribution (
                enrichment_usage_log_id, subscriber_id, property_id,
                attribution_method, attributed_cost_cents, computed_for_date
            ) VALUES (
                :log_id, :subscriber_id, :property_id,
                'direct_purchase', :cost_cents, NULL
            )
            ON CONFLICT (enrichment_usage_log_id, subscriber_id)
                WHERE attribution_method = 'direct_purchase'
                DO NOTHING
        """), {
            "log_id": enrichment_usage_log_id,
            "subscriber_id": subscriber_id,
            "property_id": property_id,
            "cost_cents": attributed_cost_cents,
        })
    except Exception:
        logger.warning(
            "[RevenueLedger] record_direct_cost_attribution failed: log_id=%s subscriber_id=%s",
            enrichment_usage_log_id, subscriber_id, exc_info=True,
        )


def attribute_enrichment_cost_for_property(db: Session, property_id: int, subscriber_id: int) -> None:
    """Find every successful enrichment_usage_logs row for a property and
    write one direct-purchase cost-attribution row per log entry to this
    subscriber. One row per log (not a pre-summed total) so a retrace later
    just adds another row — summing attributed_cost_cents at read time
    always reflects the true total with no separate aggregation step.

    Call this from every per-lead purchase confirmation path (lead_unlock,
    lead_pack, premium report/brief) right after record_revenue().
    """
    try:
        rows = db.execute(text("""
            SELECT id, cost_cents FROM enrichment_usage_logs
            WHERE property_id = :property_id AND success = TRUE
        """), {"property_id": property_id}).fetchall()
    except Exception:
        logger.warning(
            "[RevenueLedger] enrichment_usage_logs lookup failed: property_id=%s",
            property_id, exc_info=True,
        )
        return

    for row in rows:
        record_direct_cost_attribution(
            db,
            enrichment_usage_log_id=row.id,
            subscriber_id=subscriber_id,
            property_id=property_id,
            attributed_cost_cents=row.cost_cents,
        )
