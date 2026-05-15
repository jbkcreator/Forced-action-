"""
Annual Push — Item 6.

Checks ANNUAL_PUSH_TRIGGERS daily for every active subscriber and sends
the annual lock offer via email (SMS pending Subscriber.phone in 2B-2).

Triggers (ANY fires the push):
  - charter_day_7:      Day 7 for founding members (first 50 charter users)
  - day_10_14:          Day 10–14 for all users
  - two_deals:          2+ confirmed deal outcomes
  - spend_250:          $250+ cumulative wallet spend (debit transactions)
  - deal_win_10k:       Single deal reported at $10K+
  - auto_switch_day_60: Day 60 — automated annual offer

Stage 5 changes:
  - 30-day duplicate-offer suppression via MessageOutcome lookback.
  - Each push now writes a MessageOutcome(template_id='annual_offer_<trigger>')
    so the suppression check + Cora attribution have ground truth.
  - The deal-capture path imports `_push_annual_offer` directly to fire
    the deal_win_10k trigger inline (no daily-cron lag for big deals).
  - The 60-day auto-switch trigger runs the same `run_annual_push` loop;
    the cron just calls it daily.

Cron: 0 14 * * * (14:00 UTC == 9-10am ET — friendly outbound window)
"""
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.revenue_ladder import ANNUAL_PLAN
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import DealOutcome, MessageOutcome, Subscriber, WalletTransaction


_OFFER_SUPPRESSION_DAYS = 30   # don't re-offer annual within 30 days (other triggers)

# Day-60 auto-switch sequence (Phase 2B v9). Three touches with progressively
# longer gaps; sequence stops when the user accepts (tier=annual_lock) or
# after the final reminder (subscriber stays in 30-day quiet period).
_AUTO_SWITCH_BASE_TEMPLATE       = "annual_offer_auto_switch_day_60"
_AUTO_SWITCH_R1_TEMPLATE         = "annual_offer_auto_switch_day_60_r1"
_AUTO_SWITCH_R2_TEMPLATE         = "annual_offer_auto_switch_day_60_r2"
_AUTO_SWITCH_TEMPLATES           = (
    _AUTO_SWITCH_BASE_TEMPLATE,
    _AUTO_SWITCH_R1_TEMPLATE,
    _AUTO_SWITCH_R2_TEMPLATE,
)
_AUTO_SWITCH_MIN_AGE_DAYS        = 60   # account_age must be >= this; was == 60 (single-day bug)
_AUTO_SWITCH_R1_GAP_DAYS         = 3    # reminder 1 fires 3 days after first offer
_AUTO_SWITCH_R2_GAP_DAYS         = 7    # reminder 2 fires 7 days after reminder 1

# Trigger short-names returned by _check_triggers (turned into template_ids
# by _push_annual_offer as `f"annual_offer_{trigger}"`).
_AUTO_SWITCH_TRIGGER_NAMES = {
    _AUTO_SWITCH_BASE_TEMPLATE: "auto_switch_day_60",
    _AUTO_SWITCH_R1_TEMPLATE:   "auto_switch_day_60_r1",
    _AUTO_SWITCH_R2_TEMPLATE:   "auto_switch_day_60_r2",
}

logger = logging.getLogger(__name__)


def run_annual_push(dry_run: bool = False) -> dict:
    """Check all active subscribers and push annual offer to qualifiers."""
    results = {"checked": 0, "triggered": 0, "pushed": 0, "errors": 0}

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.status == "active")
        ).scalars().all()

        for sub in subs:
            results["checked"] += 1
            try:
                triggers = _check_triggers(sub, db)
                if triggers:
                    results["triggered"] += 1
                    if not dry_run:
                        if _push_annual_offer(sub, triggers[0], db):
                            results["pushed"] += 1
            except Exception as exc:
                logger.error("Annual push failed for subscriber %d: %s", sub.id, exc)
                results["errors"] += 1

    logger.info(
        "[AnnualPush] checked=%d triggered=%d pushed=%d errors=%d dry_run=%s",
        results["checked"], results["triggered"], results["pushed"], results["errors"], dry_run,
    )
    return results


def _check_triggers(sub: Subscriber, db: Session) -> list[str]:
    """Return list of trigger names that apply to this subscriber (may be empty).

    The Day-60 auto-switch sequence runs an independent state machine — three
    touches at Day 60+, +3 days, +7 days. It is NOT gated by the 30-day
    suppression that governs the other triggers, because those reminders are
    part of one logical campaign. After the final reminder the global 30-day
    suppression resumes via the most-recent annual_offer_* row.
    """
    if sub.tier == "annual_lock":
        return []   # already annual

    now = datetime.now(timezone.utc)
    created = sub.created_at
    if created and created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    account_age = (now - created).days if created else 0

    triggered: list[str] = []

    # ── Day-60 auto-switch (own multi-step state machine) ─────────────────
    # Evaluated first so it takes priority when other triggers also fire.
    auto_switch_trigger = _next_auto_switch_step(sub, account_age, now, db)
    if auto_switch_trigger:
        triggered.append(auto_switch_trigger)

    # ── Other triggers (charter_day_7, day_10_14, two_deals, spend_250,
    #    deal_win_10k) — guarded by the 30-day "any annual_offer_* recently"
    #    suppression. The auto_switch row from above (if just queued) doesn't
    #    yet exist in the DB this iteration, so it doesn't suppress itself.
    cutoff = now - timedelta(days=_OFFER_SUPPRESSION_DAYS)
    recent_offer = db.execute(
        select(MessageOutcome.id).where(
            MessageOutcome.subscriber_id == sub.id,
            MessageOutcome.template_id.like("annual_offer_%"),
            MessageOutcome.sent_at >= cutoff,
        ).limit(1)
    ).scalar_one_or_none()
    if recent_offer:
        return triggered  # auto_switch may already be queued; suppress the rest

    if sub.founding_member and account_age == 7:
        triggered.append("charter_day_7")

    if 10 <= account_age <= 14:
        triggered.append("day_10_14")

    deal_count = db.execute(
        select(func.count(DealOutcome.id)).where(
            DealOutcome.subscriber_id == sub.id,
            DealOutcome.deal_size_bucket != "skip",
        )
    ).scalar_one_or_none() or 0
    if deal_count >= 2:
        triggered.append("two_deals")

    total_debits = db.execute(
        select(func.sum(func.abs(WalletTransaction.amount))).where(
            WalletTransaction.subscriber_id == sub.id,
            WalletTransaction.txn_type == "debit",
        )
    ).scalar_one_or_none() or 0
    if float(total_debits) * 2.5 >= 250:
        triggered.append("spend_250")

    big_deal = db.execute(
        select(DealOutcome.id).where(
            DealOutcome.subscriber_id == sub.id,
            DealOutcome.deal_amount >= 10000,
        ).limit(1)
    ).scalar_one_or_none()
    if big_deal:
        triggered.append("deal_win_10k")

    return triggered


def _next_auto_switch_step(
    sub: Subscriber,
    account_age: int,
    now: datetime,
    db: Session,
) -> Optional[str]:
    """Determine which step (if any) of the Day-60 auto-switch sequence to
    fire today. Returns the short trigger name (e.g. 'auto_switch_day_60_r1')
    or None.

    Sequence:
        Day 60+ : first offer (template 'annual_offer_auto_switch_day_60')
        +3 days : reminder 1   (template '..._r1')
        +7 days : reminder 2   (template '..._r2') — final touch
    The user accepting (tier=annual_lock) halts the sequence at the top of
    _check_triggers. After r2 fires the sequence is permanently complete for
    this subscriber.
    """
    if account_age < _AUTO_SWITCH_MIN_AGE_DAYS:
        return None

    # Most recent row in the auto-switch sequence, if any.
    last_row = db.execute(
        select(MessageOutcome.template_id, MessageOutcome.sent_at).where(
            MessageOutcome.subscriber_id == sub.id,
            MessageOutcome.template_id.in_(_AUTO_SWITCH_TEMPLATES),
        ).order_by(MessageOutcome.sent_at.desc()).limit(1)
    ).first()

    if last_row is None:
        return _AUTO_SWITCH_TRIGGER_NAMES[_AUTO_SWITCH_BASE_TEMPLATE]

    last_template, last_sent = last_row
    if last_sent and last_sent.tzinfo is None:
        last_sent = last_sent.replace(tzinfo=timezone.utc)
    days_since_last = (now - last_sent).days if last_sent else 0

    if last_template == _AUTO_SWITCH_BASE_TEMPLATE:
        if days_since_last >= _AUTO_SWITCH_R1_GAP_DAYS:
            return _AUTO_SWITCH_TRIGGER_NAMES[_AUTO_SWITCH_R1_TEMPLATE]
        return None
    if last_template == _AUTO_SWITCH_R1_TEMPLATE:
        if days_since_last >= _AUTO_SWITCH_R2_GAP_DAYS:
            return _AUTO_SWITCH_TRIGGER_NAMES[_AUTO_SWITCH_R2_TEMPLATE]
        return None
    # last_template is r2 → final reminder already sent; sequence complete.
    return None


def _push_annual_offer(sub: Subscriber, trigger: str, db: Session) -> bool:
    """Send annual offer. Returns True if dispatched successfully.

    Stage 5: also writes a MessageOutcome row (template_id='annual_offer_<trigger>')
    so the 30-day suppression guard + Cora attribution have ground truth.
    """
    if not sub.email:
        logger.debug("No email for subscriber %d - annual push skipped", sub.id)
        return False

    annual_cents = ANNUAL_PLAN["price_cents"]
    monthly_cents = ANNUAL_PLAN["effective_monthly_cents"]
    annual_str = f"${annual_cents // 100:,}"
    monthly_str = f"${monthly_cents // 100}"

    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid
        else settings.app_base_url
    )
    accept_url = f"{settings.app_base_url}/api/annual/accept?feed_uuid={sub.event_feed_uuid}" if sub.event_feed_uuid else feed_url

    subject_by_trigger = {
        "deal_win_10k":               "You just closed a deal - lock the year, save 2 months",
        "auto_switch_day_60":         "Your 60-day mark - lock the year, save 2 months",
        "auto_switch_day_60_r1":      "Reminder: 2 months free if you lock in by this week",
        "auto_switch_day_60_r2":      "Last reminder: your annual offer expires soon",
    }
    subject = subject_by_trigger.get(trigger, "Save 2 months - lock your territory for a full year")

    try:
        from src.services.email import send_email
        send_email(
            to=sub.email,
            subject=subject,
            body_text=(
                f"Hi {sub.name or 'there'},\n\n"
                f"Lock in your Forced Action territory for a full year at just "
                f"{annual_str}/yr ({monthly_str}/mo effective - 2 months free).\n\n"
                f"This rate is available now. Visit your dashboard to upgrade:\n{feed_url}\n\n"
                f"One-tap accept: {accept_url}\n\n"
                f"Or reply YEARLY to your Forced Action number to lock it in.\n\n"
                f"- Forced Action Team"
            ),
        )
        # Stage 5: log MessageOutcome so the 30-day suppression guard works
        try:
            db.add(MessageOutcome(
                subscriber_id=sub.id,
                message_type="email",
                template_id=f"annual_offer_{trigger}",
                channel="ses",
                sent_at=datetime.now(timezone.utc),
            ))
            db.flush()
        except Exception as exc:
            logger.warning("[AnnualPush] MessageOutcome log failed for sub=%d: %s", sub.id, exc)

        # Best-effort GHL handoff: tag the subscriber's GHL contact so any
        # workflow listening for `annual_60_day_offer*` can take over (SMS
        # cadence, in-app banner, etc.). Failure is logged but never blocks.
        _apply_ghl_annual_tag(sub, trigger, db)

        logger.info(
            "[AnnualPush] Offer sent: subscriber=%d trigger=%s", sub.id, trigger
        )
        return True
    except Exception as exc:
        logger.error("Annual push email failed for subscriber %d: %s", sub.id, exc)
        return False


def _apply_ghl_annual_tag(sub: Subscriber, trigger: str, db: Session) -> None:
    """Push a tag like `annual_60_day_offer` / `_r1` / `_r2` onto the
    subscriber's GHL contact. Pure side-effect; never raises. If GHL isn't
    configured, `push_subscriber_to_ghl` silently returns False and we just
    log debug.
    """
    try:
        from src.services.ghl_webhook import push_subscriber_to_ghl
        tag = f"annual_60_day_offer_{trigger}" if trigger.startswith("auto_switch_day_60") \
            else f"annual_offer_{trigger}"
        ok = push_subscriber_to_ghl(
            sub, stage=None, tags=[tag], db=db,
        )
        if ok:
            logger.info(
                "[AnnualPush] GHL tag applied: subscriber=%d tag=%s",
                sub.id, tag,
            )
        else:
            logger.debug(
                "[AnnualPush] GHL not configured or push failed (non-fatal): "
                "subscriber=%d tag=%s", sub.id, tag,
            )
    except Exception as exc:
        logger.warning(
            "[AnnualPush] GHL tag push raised (non-fatal): subscriber=%d: %s",
            sub.id, exc,
        )


def switch_to_annual(subscriber_id: int, db: Session) -> bool:
    """
    Switch a subscriber's Stripe subscription from monthly to annual.
    Called after subscriber accepts the annual offer.
    Returns True on success.

    Refuses to call Stripe when the subscriber is in a billing-broken state
    (grace / churned / cancelled / paused / disputed). The /api/annual/accept
    endpoint maps the False return to a 409 with billing-portal url so the
    user fixes their card before retrying.
    """
    from src.services.stripe_service import can_switch_subscription, switch_subscription_plan

    sub = db.get(Subscriber, subscriber_id)
    if not sub or not sub.stripe_subscription_id:
        logger.error(
            "switch_to_annual: subscriber %d has no active subscription", subscriber_id
        )
        return False

    ok, reason = can_switch_subscription(sub)
    if not ok:
        logger.warning(
            "switch_to_annual: subscriber=%d blocked by status=%s",
            subscriber_id, reason,
        )
        return False

    price_id = settings.active_stripe_price("annual_lock")
    if not price_id:
        logger.error("switch_to_annual: STRIPE_PRICE_ANNUAL_LOCK not configured")
        return False

    try:
        switch_subscription_plan(sub.stripe_subscription_id, price_id)
        sub.tier = "annual_lock"
        logger.info("Subscriber %d switched to annual_lock", subscriber_id)
        return True
    except Exception as exc:
        logger.error("switch_to_annual failed for subscriber %d: %s", subscriber_id, exc)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_annual_push(dry_run=dry))
