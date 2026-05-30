"""
Proactive Save — Item 7 (Data-Only Save Tier).

Identifies at-risk subscribers and offers the Data-Only plan ($97/mo) to
prevent churn.

Triggers (either fires the save offer):
  - churn_risk:          band ∈ {high, very_high} AND predicted_inactivity_at
                         ≤ HORIZON_DAYS out (fa051: reads churn_scoring output)
  - payment_failure_day5: subscriber has been in grace for 5+ days (unchanged)

Gates (suppress the churn_risk trigger; payment_failure_day5 bypasses them):
  - Save Offer Holdout:  latest churn_predictions.in_holdout is True (ADR 0007)
  - Cooldown:            save_offer_sent_at within COOLDOWN_DAYS (ADR 0008)

Cron: 0 15 * * * (15:00 UTC daily; churn_scoring at 13:00 must run first)
"""
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.churn import COOLDOWN_DAYS, FIRE_BANDS, HORIZON_DAYS
from config.revenue_ladder import DATA_ONLY_TIER
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import ChurnPrediction, Subscriber
from src.services.claude_router import call_claude_with_usage
from src.utils.prompt_loader import get_prompt

logger = logging.getLogger(__name__)

_INACTIVE_MIN = 5  # kept for reference; Trigger 1 now reads predicted_inactivity_at


def run_proactive_save(dry_run: bool = False) -> dict:
    """Identify at-risk subscribers and send Data-Only save offers."""
    results = {"checked": 0, "at_risk": 0, "offers_sent": 0, "errors": 0}

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.status.in_(["active", "grace"]))
        ).scalars().all()

        for sub in subs:
            results["checked"] += 1
            try:
                trigger = _identify_risk(sub, db)
                if trigger:
                    results["at_risk"] += 1
                    if not dry_run:
                        if _send_save_offer(sub, trigger, db):
                            results["offers_sent"] += 1
            except Exception as exc:
                logger.error("Proactive save failed for subscriber %d: %s", sub.id, exc)
                results["errors"] += 1

    logger.info(
        "[ProactiveSave] checked=%d at_risk=%d offers_sent=%d errors=%d dry_run=%s",
        results["checked"], results["at_risk"], results["offers_sent"], results["errors"], dry_run,
    )
    return results


def _latest_churn_prediction(sub_id: int, db: Session) -> Optional[ChurnPrediction]:
    """Return the most recent churn_predictions row for this subscriber, or None.

    churn_predictions is the source of truth for the churn_risk trigger: the
    nightly job appends one row per scored subscriber, so this is populated for
    every subscriber it scores. (The user_segments churn columns are only a
    display mirror and are UPDATE-only — absent for subscribers with no segment
    row — so they must NOT be used for firing decisions.)
    """
    return db.execute(
        select(ChurnPrediction)
        .where(ChurnPrediction.subscriber_id == sub_id)
        .order_by(ChurnPrediction.predicted_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _last_offer_sent_at(sub_id: int, db: Session) -> Optional[datetime]:
    """Most recent save_offer_sent_at across ALL of this subscriber's predictions.

    The cooldown must look across every prediction row, not just the latest one:
    churn_scoring appends a fresh row nightly with save_offer_sent_at=NULL, so a
    "latest row only" check would forget that an offer was sent days ago and let
    the offer re-fire every night (defeating COOLDOWN_DAYS).
    """
    return db.execute(
        select(func.max(ChurnPrediction.save_offer_sent_at))
        .where(ChurnPrediction.subscriber_id == sub_id)
    ).scalar_one_or_none()


def _identify_risk(sub: Subscriber, db: Session) -> Optional[str]:
    """Return trigger string if subscriber is at risk, else None.

    Trigger 1 (churn_risk): read churn_scoring output from the latest
    churn_predictions row. Subject to holdout + cooldown gates.

    Trigger 2 (payment_failure_day5): grace period ≥ 5 days. Not gated by
    holdout or cooldown — payment recovery always fires.
    """
    if sub.tier in ("data_only", "free"):
        return None

    now = datetime.now(timezone.utc)

    # ── Trigger 1: churn risk prediction ──────────────────────────────────
    # Read band + onset + holdout from churn_predictions (written for every
    # scored subscriber), NOT user_segments (UPDATE-only display mirror).
    latest = _latest_churn_prediction(sub.id, db)

    if latest and latest.churn_risk_band in FIRE_BANDS and latest.predicted_inactivity_at:
        predicted = latest.predicted_inactivity_at
        if predicted.tzinfo is None:
            predicted = predicted.replace(tzinfo=timezone.utc)
        days_out = (predicted - now).total_seconds() / 86400

        if days_out <= HORIZON_DAYS:
            # Check holdout gate (ADR 0007)
            if latest.in_holdout:
                logger.debug(
                    "[ProactiveSave] sub=%d in Save Offer Holdout — skipping", sub.id
                )
                return None

            # Check cooldown gate — MAX across all predictions (see _last_offer_sent_at)
            last_sent = _last_offer_sent_at(sub.id, db)
            if last_sent:
                if last_sent.tzinfo is None:
                    last_sent = last_sent.replace(tzinfo=timezone.utc)
                if (now - last_sent).days < COOLDOWN_DAYS:
                    logger.debug(
                        "[ProactiveSave] sub=%d on cooldown (%dd since last offer)",
                        sub.id,
                        (now - last_sent).days,
                    )
                    return None

            return "churn_risk"

    # ── Trigger 2: Day 5+ of grace period (payment failure) ───────────────
    if sub.status == "grace" and sub.grace_expires_at:
        expires = sub.grace_expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        grace_entered = expires - timedelta(hours=settings.grace_period_hours)
        days_in_grace = (now - grace_entered).days
        if days_in_grace >= 5:
            return "payment_failure_day5"

    return None


def _stamp_save_offer_sent_at(sub_id: int, db: Session) -> None:
    """Set save_offer_sent_at on the latest churn_predictions row (cooldown record)."""
    latest = _latest_churn_prediction(sub_id, db)
    if latest:
        latest.save_offer_sent_at = datetime.now(timezone.utc)
    else:
        logger.debug(
            "[ProactiveSave] No churn_predictions row for sub=%d — cooldown not stamped", sub_id
        )


def _parse_email(text: str) -> tuple[str, str]:
    lines = text.strip().splitlines()
    subject = next((l.replace("SUBJECT:", "").strip() for l in lines if l.startswith("SUBJECT:")), "")
    body_start = next((i for i, l in enumerate(lines) if l.startswith("BODY:")), None)
    body = "\n".join(lines[body_start + 1:]).strip() if body_start is not None else ""
    if not subject or len(subject) > 60 or not body:
        return "", ""
    return subject, body


def _send_save_offer(sub: Subscriber, trigger: str, db) -> bool:
    """Send Data-Only save offer email. Returns True if sent."""
    if not sub.email:
        return False

    price = DATA_ONLY_TIER["price_cents"] // 100
    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}?save_offer=accept"
        if sub.event_feed_uuid
        else settings.app_base_url
    )
    name = sub.name or "there"
    founding_member = getattr(sub, "founding_member", False)

    subject = ""
    body_text = ""
    try:
        system_prompt = get_prompt("emails/proactive_save.yaml", "system")
        user_prompt = get_prompt(
            "emails/proactive_save.yaml", "user",
            name=name, trigger=trigger, price=price,
            feed_url=feed_url, founding_member=founding_member,
        )
        result = call_claude_with_usage(
            task_type="email_copy",
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=800,
            subscriber_id=sub.id,
            db=db,
        )
        subject, body_text = _parse_email(result["text"])
    except Exception as exc:
        logger.warning("[ProactiveSave] Cora composition failed for sub=%d, using fallback: %s", sub.id, exc)

    if not subject or not body_text:
        trigger_line = (
            "We noticed you haven't been active recently — life gets busy."
            if trigger in ("inactivity", "churn_risk")
            else "We noticed your payment hasn't gone through yet."
        )
        subject = f"Keep your leads for ${price}/mo — Data-Only access"
        body_text = (
            f"Hi {name},\n\n"
            f"{trigger_line}\n\n"
            f"We don't want you to lose your territory. Switch to our Data-Only plan at "
            f"just ${price}/mo — full property data feed, no enrichment fees, cancel anytime.\n\n"
            f"Switch now:\n{feed_url}\n\n"
            f"Questions? Reply to this email.\n\n"
            f"— Forced Action Team"
        )

    try:
        from src.services.email import send_email
        send_email(to=sub.email, subject=subject, body_text=body_text)
        logger.info("[ProactiveSave] Offer sent: subscriber=%d trigger=%s", sub.id, trigger)
        # Stamp cooldown record (churn_risk trigger only; payment_failure_day5 is not rate-limited)
        if trigger == "churn_risk":
            _stamp_save_offer_sent_at(sub.id, db)
        return True
    except Exception as exc:
        logger.error("Save offer email failed for subscriber %d: %s", sub.id, exc)
        return False


def downgrade_to_data_only(subscriber_id: int, db: Session) -> bool:
    """
    Downgrade subscriber to Data-Only plan via Stripe.
    Called when subscriber accepts the save offer.
    Returns True on success.
    """
    from src.services.stripe_service import switch_subscription_plan

    sub = db.get(Subscriber, subscriber_id)
    if not sub or not sub.stripe_subscription_id:
        logger.error(
            "downgrade_to_data_only: subscriber %d has no active subscription", subscriber_id
        )
        return False

    price_id = settings.active_stripe_price("data_only")
    if not price_id:
        logger.error("downgrade_to_data_only: STRIPE_PRICE_DATA_ONLY not configured")
        return False

    try:
        switch_subscription_plan(sub.stripe_subscription_id, price_id)
        sub.tier = "data_only"
        logger.info("Subscriber %d downgraded to data_only", subscriber_id)
    except Exception as exc:
        logger.error("downgrade_to_data_only failed for subscriber %d: %s", subscriber_id, exc)
        return False

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="data_only_save",
            source_table="stripe_subscriptions",
            source_event_id=sub.stripe_subscription_id,
            subscriber_id=sub.id,
            occurred_at=datetime.now(timezone.utc),
            db=db,
        )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", sub.id, exc_info=True)

    return True


def compute_save_offer_active(subscriber, db) -> bool:
    """Return True if subscriber is eligible for the Data-Only save offer. Safe to call from API layer."""
    try:
        return bool(_identify_risk(subscriber, db))
    except Exception:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_proactive_save(dry_run=dry))
