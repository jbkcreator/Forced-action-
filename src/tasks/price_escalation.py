"""
6-month founding rate escalation task.

Finds founding members whose rate lock has been in place for ≥ 6 months
and whose subscription has NOT yet been escalated, then:
  1. Updates their Stripe subscription to the current regular price.
  2. Sets subscriber.escalated_at = now().
  3. Sends a notification email.

Run weekly via cron (Monday at 8 AM so it never runs on a billing weekend):
    0 8 * * 1 $PROJECT/scripts/cron/run.sh src.tasks.price_escalation

Supports --dry-run to preview eligible subscribers without making changes.
"""

import logging
from datetime import datetime, timezone, timedelta

import stripe
from sqlalchemy import select

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import Subscriber
from src.services.email import send_email

logger = logging.getLogger(__name__)

# Regular price map: (tier, vertical) → settings attribute name
# The vertical dimension is currently the same price per tier, but kept
# extensible in case verticals diverge later.
_REGULAR_PRICE_ATTR = {
    "starter":   "stripe_price_starter_regular",
    "pro":       "stripe_price_pro_regular",
    "dominator": "stripe_price_dominator_regular",
}

_SIX_MONTHS = timedelta(days=183)


def run_price_escalation(dry_run: bool = False) -> dict:
    """
    Escalate founding members who have passed the 6-month rate lock window.

    Returns:
        dict with keys: checked, eligible, escalated, failed, dry_run
    """
    settings = get_settings()
    stats = {"checked": 0, "eligible": 0, "escalated": 0, "failed": 0, "dry_run": dry_run}

    if not settings.stripe_secret_key:
        logger.warning("[PriceEscalation] STRIPE_SECRET_KEY not set — cannot escalate")
        return stats

    stripe.api_key = settings.stripe_secret_key.get_secret_value()
    cutoff = datetime.now(timezone.utc) - _SIX_MONTHS

    with get_db_context() as db:
        eligible = db.execute(
            select(Subscriber).where(
                Subscriber.founding_member == True,  # noqa: E712
                Subscriber.status == "active",
                Subscriber.rate_locked_at <= cutoff,
                Subscriber.escalated_at == None,  # noqa: E711
                Subscriber.stripe_subscription_id != None,  # noqa: E711
            )
        ).scalars().all()

        stats["checked"] = len(eligible)
        logger.info(
            "[PriceEscalation] Found %d founding member(s) eligible for rate escalation%s",
            len(eligible), " (DRY RUN)" if dry_run else "",
        )

        for subscriber in eligible:
            stats["eligible"] += 1
            regular_price_id = getattr(settings, _REGULAR_PRICE_ATTR.get(subscriber.tier, ""), None)

            if not regular_price_id:
                logger.error(
                    "[PriceEscalation] No regular price configured for tier '%s' — skipping %s",
                    subscriber.tier, subscriber.id,
                )
                stats["failed"] += 1
                continue

            if dry_run:
                logger.info(
                    "[PriceEscalation] DRY RUN — would escalate subscriber %s (%s) "
                    "from founding price to %s",
                    subscriber.id, subscriber.email, regular_price_id,
                )
                continue

            try:
                # Retrieve current subscription items to get the item ID
                sub = stripe.Subscription.retrieve(subscriber.stripe_subscription_id)
                item_id = sub["items"]["data"][0]["id"]

                # Update to regular price (prorated at next billing cycle)
                stripe.Subscription.modify(
                    subscriber.stripe_subscription_id,
                    items=[{"id": item_id, "price": regular_price_id}],
                    proration_behavior="none",  # switch at next renewal, no charge now
                )

                # Mark escalated in DB
                subscriber.escalated_at = datetime.now(timezone.utc)
                db.flush()

                stats["escalated"] += 1
                logger.info(
                    "[PriceEscalation] Escalated subscriber %s (%s) to regular price %s",
                    subscriber.id, subscriber.email, regular_price_id,
                )

                # Send notification email
                if subscriber.email:
                    dashboard_url = (
                        f"{settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
                        if subscriber.event_feed_uuid else settings.app_base_url
                    )
                    name = subscriber.name or "there"
                    tier = (subscriber.tier or "starter").title()

                    # Extract human-readable prices from Stripe subscription
                    try:
                        old_amount = sub["items"]["data"][0]["price"]["unit_amount"]
                        founding_rate_str = f"${old_amount / 100:,.0f}/mo"
                    except (KeyError, IndexError, TypeError):
                        founding_rate_str = "founding rate"

                    try:
                        new_price = stripe.Price.retrieve(regular_price_id)
                        regular_rate_str = f"${new_price['unit_amount'] / 100:,.0f}/mo"
                    except Exception:
                        regular_rate_str = "regular rate"

                    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">

        <!-- Header -->
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>

        <!-- Info banner -->
        <tr>
          <td style="padding:0;">
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(251,191,36,0.12);border-bottom:1px solid rgba(251,191,36,0.25);">
              <tr>
                <td style="padding:14px 40px;font-size:14px;font-weight:700;color:#fbbf24;text-align:center;">
                  Pricing update effective next billing cycle
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:26px;font-weight:800;color:#ffffff;">
              Update to your subscription
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Hi {name}, thank you for being a founding member. Your 6-month founding
              rate lock period has now ended, and your <strong style="color:#ffffff;">{tier}</strong>
              plan will transition to its regular pricing starting with your next billing cycle.
            </p>

            <!-- What's changing card -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
                          border-radius:12px;padding:20px 24px;margin-bottom:24px;">
              <tr>
                <td>
                  <p style="margin:0 0 14px;font-size:14px;font-weight:700;color:#ffffff;">
                    What&rsquo;s changing
                  </p>
                  <table width="100%" cellpadding="0" cellspacing="0">
                    <tr>
                      <td style="font-size:13px;color:#94a3b8;padding:0 0 8px;">Founding rate</td>
                      <td style="font-size:13px;color:#94a3b8;padding:0 0 8px;text-align:right;">
                        <span style="text-decoration:line-through;">{founding_rate_str}</span>
                      </td>
                    </tr>
                    <tr>
                      <td style="font-size:14px;font-weight:700;color:#ffffff;padding:0;">Regular rate</td>
                      <td style="font-size:14px;font-weight:700;color:#fbbf24;padding:0;text-align:right;">
                        {regular_rate_str}
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>

            <p style="margin:0 0 24px;color:#94a3b8;font-size:14px;">
              Your ZIP territories and lead access remain completely unchanged &mdash;
              only the subscription price is updating.
            </p>

            <!-- CTA -->
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{dashboard_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Access Your Lead Feed &rarr;
                  </a>
                </td>
              </tr>
            </table>

            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? Reply to this email or reach us at
              <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">
                support@forcedaction.io
              </a>
            </p>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

                    send_email(
                        to=subscriber.email,
                        subject="Update to your Forced Action subscription",
                        body_text=(
                            f"Hi {name},\n\n"
                            f"Your 6-month founding rate lock has now expired. "
                            f"Your subscription will automatically renew at the current "
                            f"{tier} plan rate starting with your next "
                            f"billing cycle.\n\n"
                            f"What's changing:\n"
                            f"  Founding rate: {founding_rate_str} (ended)\n"
                            f"  Regular rate:  {regular_rate_str}\n\n"
                            f"Your ZIP territories and lead access remain unchanged — "
                            f"only the price updates.\n\n"
                            f"Access your lead feed:\n{dashboard_url}\n\n"
                            f"Questions? support@forcedaction.io\n\n"
                            f"— Forced Action Team"
                        ),
                        body_html=body_html,
                    )

            except stripe.StripeError as exc:
                logger.error(
                    "[PriceEscalation] Stripe error escalating subscriber %s: %s",
                    subscriber.id, exc,
                )
                stats["failed"] += 1
            except Exception as exc:
                logger.error(
                    "[PriceEscalation] Unexpected error for subscriber %s: %s",
                    subscriber.id, exc, exc_info=True,
                )
                stats["failed"] += 1

    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Escalate founding member prices after 6 months")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    args = parser.parse_args()

    result = run_price_escalation(dry_run=args.dry_run)
    print(result)
