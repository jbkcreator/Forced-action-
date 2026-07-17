"""
Lead Pack fulfillment sweep — post-payment Hot-Enrichment + delivery (ADR 0018).

Lead Pack purchases are RESERVED at payment (stripe_webhooks._on_lead_pack_payment
sets status='enriching' and writes the cross-trade exclusivity rows). This sweep
runs the deferred half:

  1. Claim 'enriching' purchases (FOR UPDATE SKIP LOCKED; stamp
     enrichment_submitted_at so overlapping runs don't double-process).
  2. Lead Pack Hot-Enrichment — a fresh Tracerfy /trace/ batch over the 5
     reserved property IDs.
  3. 100% Quality Floor — every one of the 5 must return a phone OR email:
       - PASS  → status='delivered', SentLead rows (source='lead_pack'),
                 delivery email.
       - FAIL  → status='refunded', release the exclusivity reservation,
                 Stripe refund, refund email.

Stale claims (enrichment_submitted_at older than the retry window) are re-picked
so a crashed sweep self-heals on the next run.

Usage:
    python -m src.tasks.lead_pack_fulfillment_sweep
    python -m src.tasks.lead_pack_fulfillment_sweep --dry-run

Cron: every 2 minutes.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.core.database import get_db_context

logger = logging.getLogger(__name__)

# How long a claimed-but-unfinished purchase waits before another sweep retries
# it (covers a crash mid-Tracerfy-poll). Tracerfy worst case is ~5 min.
_RETRY_AFTER_MINUTES = 15
# Max purchases to fulfill per sweep run (each does a blocking Tracerfy poll).
_BATCH = 10


def _stale_delta():
    from datetime import timedelta
    return timedelta(minutes=_RETRY_AFTER_MINUTES)


def _candidate_ids(db, now: datetime) -> list[int]:
    """IDs of reserved packs eligible for fulfillment (unclaimed or stale)."""
    rows = db.execute(
        sa_text("""
            SELECT id FROM lead_pack_purchases
            WHERE status = 'enriching'
              AND (enrichment_submitted_at IS NULL
                   OR enrichment_submitted_at < :stale_before)
            ORDER BY purchased_at
            LIMIT :batch
        """),
        {"stale_before": now - _stale_delta(), "batch": _BATCH},
    ).fetchall()
    return [r[0] for r in rows]


def _claim(db, purchase_id: int, now: datetime) -> bool:
    """
    Atomically claim ONE purchase for fulfillment. Stamps enrichment_submitted_at
    only if it's still 'enriching' and unclaimed (or its claim went stale). This
    is the single guard that serializes the event-driven listener and the cron
    sweep — whichever calls first wins; the loser gets False and skips. Commits
    immediately so the row lock isn't held across the long Tracerfy poll.
    """
    claimed = db.execute(
        sa_text("""
            UPDATE lead_pack_purchases
            SET enrichment_submitted_at = :now
            WHERE id = :id
              AND status = 'enriching'
              AND (enrichment_submitted_at IS NULL
                   OR enrichment_submitted_at < :stale_before)
            RETURNING id
        """),
        {"id": purchase_id, "now": now, "stale_before": now - _stale_delta()},
    ).first()
    db.commit()
    return claimed is not None


def _fulfill_one(purchase_id: int) -> str:
    """
    Claim + fulfill one reserved purchase in its own transaction. The atomic
    _claim() makes this safe to call from BOTH the cron sweep and the
    event-driven listener concurrently — only one wins the claim.
    Returns 'delivered' | 'refunded' | 'skipped' | 'error'.
    """
    from src.core.models import LeadPackPurchase

    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        if not _claim(db, purchase_id, now):
            return "skipped"
        purchase = db.get(LeadPackPurchase, purchase_id)
        if purchase is None:
            return "skipped"
        outcome = fulfill_purchase(db, purchase)
        if outcome == "error":
            db.rollback()
        else:
            db.commit()
        return outcome


def _fulfill_when_visible(purchase_id: int) -> None:
    """
    Daemon-thread target for the event path. The webhook publishes the event
    BEFORE its own transaction commits, so the row may not yet be visible to a
    fresh session. Briefly wait for it to appear, then fulfill. The atomic
    _claim() inside _fulfill_one() still guards against the cron sweep racing us.
    """
    import time

    from src.core.models import LeadPackPurchase

    for _ in range(10):  # ~5s of visibility grace for the API commit
        with get_db_context() as db:
            if db.get(LeadPackPurchase, purchase_id) is not None:
                break
        time.sleep(0.5)

    _fulfill_one(purchase_id)


def handle_reserved_event(payload: dict) -> None:
    """
    Event-driven entry point (the "always-on listener" path). Invoked by the
    agents supervisor when a `lead_pack_reserved` event arrives, giving instant
    fulfillment instead of waiting for the next cron tick. Runs the (potentially
    multi-minute) Tracerfy poll on a daemon thread so the listener thread is
    never blocked. The cron sweep remains the durability backstop if the event
    is dropped (Redis down, agents process restarting).
    """
    import threading

    try:
        purchase_id = int(payload.get("purchase_id"))
    except (TypeError, ValueError):
        logger.warning("[LeadPackSweep] lead_pack_reserved missing purchase_id: %s", payload)
        return

    threading.Thread(
        target=_fulfill_when_visible,
        args=(purchase_id,),
        daemon=True,
        name=f"leadpack-fulfill-{purchase_id}",
    ).start()


def fulfill_purchase(db, purchase) -> str:
    """
    Core Hot-Enrichment + Quality-Floor logic for one reserved purchase, on the
    GIVEN session. Does NOT commit — the caller owns the transaction (so this is
    drivable under a test savepoint). Returns the terminal outcome.

    PASS  → status='delivered', SentLead rows, delivery email.
    FAIL  → status='refunded', exclusivity released, Stripe refund, refund email.
    """
    from src.core.models import Subscriber
    from src.services.lead_exclusivity import clear_exclusivity_for_purchase
    from src.services.stripe_webhooks import _send_lead_pack_refund_email
    from src.services.tracerfy_fallback import hot_enrich_properties

    settings = get_settings()
    now = datetime.now(timezone.utc)
    purchase_id = purchase.id
    lead_ids = list(purchase.lead_ids or [])

    if len(lead_ids) < 5:
        logger.warning(
            "[LeadPackSweep] purchase %s has %d reserved leads (<5)",
            purchase_id, len(lead_ids),
        )

    # ── Hot-Enrichment ────────────────────────────────────────────────────
    try:
        enriched = hot_enrich_properties(db, lead_ids)
    except Exception as exc:
        # Leave status='enriching'; the stale-claim retry picks it up next run.
        logger.error(
            "[LeadPackSweep] hot-enrich failed for purchase %s: %s",
            purchase_id, exc, exc_info=True,
        )
        return "error"

    # ── 100% Quality Floor ────────────────────────────────────────────────
    passed = len(lead_ids) >= 5 and all(
        enriched.get(pid, {}).get("match_success") for pid in lead_ids
    )
    subscriber = db.get(Subscriber, purchase.subscriber_id)

    if passed:
        purchase.status = "delivered"
        purchase.delivered_at = now

        # SentLead rows (source='lead_pack') — marks these leads delivered to
        # this buyer so they're excluded from his own blurred stack / $4 unlocks
        # and counted by the lead-quality monitor.
        #
        # Ledger revenue is split evenly across the delivered leads (not one
        # row for the full purchase) so every ledger row carries a real
        # property_id — this is what lets cost-joining stay uniform across
        # every product type, per src/services/revenue_ledger.py. Remainder
        # cents (if amount_cents doesn't divide evenly) go to the first N
        # rows so the sum always equals the real amount charged, never more.
        from src.services.revenue_ledger import (
            record_revenue, attribute_enrichment_cost_for_property,
        )
        total_cents = purchase.amount_cents or 0
        n_leads = len(lead_ids)
        base_share, remainder = divmod(total_cents, n_leads)
        for i, pid in enumerate(lead_ids):
            sent_row = db.execute(
                sa_text("""
                    INSERT INTO sent_leads (
                        subscriber_id, property_id, sent_at, source,
                        stripe_payment_intent_id
                    ) VALUES (
                        :sid, :pid, :now, 'lead_pack', :pi
                    )
                    ON CONFLICT (subscriber_id, property_id) DO UPDATE SET
                        sent_at = EXCLUDED.sent_at,
                        source = 'lead_pack',
                        stripe_payment_intent_id = EXCLUDED.stripe_payment_intent_id
                    RETURNING id
                """),
                {
                    "sid": purchase.subscriber_id,
                    "pid": pid,
                    "now": now,
                    "pi": purchase.stripe_payment_intent_id,
                },
            ).fetchone()

            if total_cents > 0 and sent_row:
                share = base_share + (1 if i < remainder else 0)
                record_revenue(
                    db, subscriber_id=purchase.subscriber_id, product_type="lead_pack",
                    amount_cents=share, source_table="sent_leads",
                    source_id=sent_row.id, property_id=pid, occurred_at=now,
                )
                attribute_enrichment_cost_for_property(db, pid, purchase.subscriber_id)
        db.flush()

        if subscriber and subscriber.email:
            try:
                _send_delivery_email(db, subscriber, purchase, lead_ids)
            except Exception:
                logger.error(
                    "[LeadPackSweep] delivery email failed for purchase %s",
                    purchase_id, exc_info=True,
                )

        try:
            from src.services.win_story_publisher import publish_win_story
            publish_win_story(
                "lead_pack",
                purchase.county_id,
                db,
                detail=purchase.vertical,
            )
        except Exception:
            logger.warning(
                "[LeadPackSweep] win-story publish failed for purchase %s",
                purchase_id, exc_info=True,
            )

        try:
            with db.begin_nested():
                from src.services.referral_prompt_service import maybe_send_referral_prompt
                maybe_send_referral_prompt(
                    subscriber, db,
                    trigger_type="lead_pack_delivery",
                    trigger_source_table="lead_pack_purchases",
                    trigger_source_id=purchase.id,
                )
        except Exception:
            logger.warning(
                "[LeadPackSweep] referral prompt failed for purchase %s",
                purchase_id, exc_info=True,
            )

        logger.info(
            "[LeadPackSweep] DELIVERED purchase %s (%d leads) to subscriber %s",
            purchase_id, len(lead_ids), purchase.subscriber_id,
        )
        return "delivered"

    # ── Floor miss → refund + release reservation ──────────────────────────
    hits = sum(1 for pid in lead_ids if enriched.get(pid, {}).get("match_success"))
    purchase.status = "refunded"
    purchase.refunded_at = now
    purchase.refund_reason = f"quality_floor_{hits}_of_{len(lead_ids)}"
    db.flush()

    clear_exclusivity_for_purchase(db, purchase.id, "lead_pack")

    try:
        import stripe
        stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
        refund = stripe.Refund.create(
            payment_intent=purchase.stripe_payment_intent_id,
            idempotency_key=f"leadpack-refund-{purchase.stripe_payment_intent_id}",
        )
        purchase.stripe_refund_id = refund.get("id")
    except Exception as exc:
        logger.error(
            "[LeadPackSweep] Stripe refund failed for purchase %s: %s",
            purchase_id, exc,
        )

    if subscriber:
        try:
            _send_lead_pack_refund_email(subscriber, purchase)
        except Exception:
            logger.error(
                "[LeadPackSweep] refund email failed for purchase %s",
                purchase_id, exc_info=True,
            )

    logger.info(
        "[LeadPackSweep] REFUNDED purchase %s — Quality Floor %d/%d",
        purchase_id, hits, len(lead_ids),
    )
    return "refunded"


def _send_delivery_email(db, subscriber, purchase, lead_ids: list[int]) -> None:
    """Load the delivered leads and hand off to the existing delivery template."""
    from src.core.models import DistressScore, Property
    from src.services.stripe_webhooks import _send_lead_pack_email

    rows = (
        db.query(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .filter(Property.id.in_(lead_ids))
        .all()
    )
    _send_lead_pack_email(subscriber, purchase, rows)


def run(dry_run: bool = False) -> dict:
    now = datetime.now(timezone.utc)
    summary = {"dry_run": dry_run, "claimed": 0, "delivered": 0, "refunded": 0,
               "skipped": 0, "error": 0}

    if dry_run:
        with get_db_context() as db:
            row = db.execute(sa_text("""
                SELECT COUNT(*) AS c FROM lead_pack_purchases
                WHERE status = 'enriching'
                  AND (enrichment_submitted_at IS NULL
                       OR enrichment_submitted_at < :stale_before)
            """), {"stale_before": now - _stale_delta()}).first()
        summary["claimed"] = int(row.c) if row else 0
        logger.info("[LeadPackSweep] %s", json.dumps(summary, default=str))
        return summary

    with get_db_context() as db:
        ids = _candidate_ids(db, now)
    summary["claimed"] = len(ids)

    # _fulfill_one re-claims each id atomically, so a pack already grabbed by the
    # event-driven listener simply returns 'skipped' here — no double-processing.
    for pid in ids:
        outcome = _fulfill_one(pid)
        summary[outcome] = summary.get(outcome, 0) + 1

    logger.info("[LeadPackSweep] %s", json.dumps(summary, default=str))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    dry_run = "--dry-run" in set(argv or sys.argv[1:])
    summary = run(dry_run=dry_run)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
