"""
Activation stall monitor (E5 / client Section 4.10).

Alerts on exactly one case: a subscriber who paid, was sent a welcome email,
and still has not redeemed their magic link 48 hours later. That combination
means we told them and they never got in — a known-cause churn risk with a
phone-call fix.

Deliberately narrow. Stalls further down the funnel (logged in but hasn't
finished onboarding, or hasn't unlocked a lead yet) are ordinary behaviour,
not breakage, and are reporting-only via the weekly client report — alerting
on those would train the recipient to ignore these pages.

Distinct from the retry-then-alert in subscriber_auth.issue_magic_link_url_with_retry,
which covers a link we failed to *issue*. This covers a link that was issued
and delivered but never *used*.

Usage:
    PYTHONPATH=. python -m src.tasks.activation_stall_monitor
    PYTHONPATH=. python -m src.tasks.activation_stall_monitor --dry-run
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.email import send_alert
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

STALL_THRESHOLD_HOURS = 48
ALERT_TYPE = "activation_stall_magic_link_unredeemed"


def _required_columns_present(db) -> bool:
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'activation_events'
              AND column_name IN ('welcome_email_sent_time', 'magic_link_redeemed_time')
            """
        )
    ).fetchall()
    return {row[0] for row in rows} == {"welcome_email_sent_time", "magic_link_redeemed_time"}


def find_stalled_subscribers(db, *, threshold_hours: int = STALL_THRESHOLD_HOURS) -> list[dict]:
    """Paid subscribers sent a welcome email >= threshold_hours ago who never redeemed it.

    Excludes anyone already alerted for this rule (one page per subscriber,
    ever — a stall does not become newsworthy again on the next run).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)
    rows = db.execute(
        text(
            """
            SELECT s.id, s.email, s.tier, ae.welcome_email_sent_time
            FROM activation_events ae
            JOIN subscribers s ON s.id = ae.subscriber_id
            WHERE ae.welcome_email_sent_time IS NOT NULL
              AND ae.welcome_email_sent_time <= :cutoff
              AND ae.magic_link_redeemed_time IS NULL
              AND s.tier <> 'free'
              AND s.status IN ('active', 'grace')
              AND NOT EXISTS (
                  SELECT 1 FROM scraper_alert_log sal
                  WHERE sal.source_type = :alert_type
                    AND sal.alert_type = :alert_type || ':' || s.id::text
              )
            ORDER BY ae.welcome_email_sent_time ASC
            """
        ),
        {"cutoff": cutoff, "alert_type": ALERT_TYPE},
    ).fetchall()
    return [
        {
            "subscriber_id": r[0],
            "email": r[1],
            "tier": r[2],
            "welcome_email_sent_time": r[3],
        }
        for r in rows
    ]


def run(dry_run: bool = False, threshold_hours: int = STALL_THRESHOLD_HOURS) -> dict:
    stats = {"checked": 0, "alerted": 0, "skipped": 0}
    with get_db_context() as db:
        if not _required_columns_present(db):
            logger.warning(
                "activation_stall_monitor: activation_events checkpoint columns missing - "
                "run migrations/apply_activation_event_checkpoints.py first; skipping"
            )
            return stats

        stalled = find_stalled_subscribers(db, threshold_hours=threshold_hours)
        stats["checked"] = len(stalled)

        for row in stalled:
            if dry_run:
                logger.info(
                    "[DRY RUN] would alert: subscriber=%s tier=%s welcomed_at=%s",
                    row["subscriber_id"], row["tier"], row["welcome_email_sent_time"],
                )
                stats["skipped"] += 1
                continue

            sent = send_alert(
                subject="[FA] Paid subscriber never logged in",
                body=(
                    f"Subscriber id={row['subscriber_id']} ({row['tier']}) was sent a "
                    f"welcome email at {row['welcome_email_sent_time']} and has still not "
                    f"redeemed their magic link {threshold_hours}h later.\n\n"
                    f"Email: {row['email']}\n\n"
                    "They have paid and cannot get in. Call them and send a fresh link."
                ),
            )
            if not sent:
                # Leave it unlogged so the next run retries rather than
                # silently dropping the page.
                logger.error(
                    "activation_stall_monitor: alert send failed for subscriber=%s - will retry next run",
                    row["subscriber_id"],
                )
                stats["skipped"] += 1
                continue

            # scraper_alert_log has no per-entity key column, so the subscriber
            # id rides in alert_type — this is what makes the page fire once
            # per subscriber rather than once per rule.
            db.execute(
                text(
                    """
                    INSERT INTO scraper_alert_log (source_type, county_id, alert_type, alerted_at)
                    VALUES (:t, :county, :scoped, now())
                    """
                ),
                {
                    "t": ALERT_TYPE,
                    "county": "platform",
                    "scoped": f"{ALERT_TYPE}:{row['subscriber_id']}",
                },
            )
            stats["alerted"] += 1

        db.commit()

    logger.info("activation_stall_monitor complete: %s", stats)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Alert on paid subscribers who never redeemed a magic link")
    parser.add_argument("--dry-run", action="store_true", help="Report without alerting or recording")
    parser.add_argument("--hours", type=int, default=STALL_THRESHOLD_HOURS)
    args = parser.parse_args()
    run(dry_run=args.dry_run, threshold_hours=args.hours)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
