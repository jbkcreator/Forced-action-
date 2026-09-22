"""
FA Max send-infrastructure health monitor (WP-T2-1).

WP-T2-1's own Done-When line is "sustained inbox placement above 95% at
target volume" — this monitor does NOT measure that; it is a proxy built
from what the existing stack already observes:

  - Instantly warmup health score for the FA Max venture's own sending
    mailbox specifically (see _warmup_trip -- scoped by
    venture.relay_instantly_sender_email since 2026-09; previously checked
    every connected Instantly account fleet-wide, a real bug an unrelated
    venture's mailbox could have tripped or masked).
  - Relay's own observed bounce/skip rate for 'sent' vs 'failed'/'skipped'
    FA Max items over the trailing window (src.services.relay.queue).

Real placement measurement (correction, 2026-09 go-live review: an earlier
version of this docstring said it needs a separate seed-list vendor and
cannot be built from this repo at all -- that was wrong) is buildable via
Instantly's own documented Inbox Placement Analytics API
(developer.instantly.ai/api-reference/groups/inbox-placement-analytics),
pending confirmation of (1) account entitlement to Instantly's separate
"Inbox Placement" plan and (2) whether seed-list-style sampling (this
feature's actual methodology, same as any third-party alternative) is what
the client means by "at target volume." See docs/fa-max-go-live.md Step 6
for both open items. Do not wire this monitor's proxy output into a "Done"
claim for the 95% placement criterion either way.

Alerts route through src.services.relay.exceptions_alert_queue (WP-T2-1
go-live review, 2026-09): a row is durably committed BEFORE Slack is ever
called, so a Slack outage OR a process crash mid-attempt leaves the alert
recoverable by the drain worker (src/tasks/fa_max_exceptions_alert_drain.py)
rather than silently dropped. That module also owns the "don't re-page the
same condition within N hours" dedup — this monitor no longer keeps its own
separate ScraperAlertLog-based dedup for that purpose (removed here to avoid
two divergent sources of truth for "was this recently alerted").
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services import instantly_service
from src.services.relay import exceptions_alert_queue
from src.utils.venture_config import get_venture_config

logger = logging.getLogger(__name__)

FA_MAX_VENTURE = "fa_max_lending"
WARMUP_SCORE_FLOOR = 70
ROLLING_HOURS = 24
MIN_SENT_FLOOR = 10
FAILURE_RATE_CONCERN = 0.10


@dataclass
class Trip:
    rule: str
    detail: str


def _warmup_trip(campaign_id: Optional[str], sender_email: Optional[str]) -> Optional[Trip]:
    """WP-T2-1 go-live review (2026-09) fix: instantly_service.list_accounts()
    is genuinely fleet-wide (GET /api/v2/accounts, no campaign filter) --
    this function used to call it unconditionally and check warmup health on
    EVERY connected mailbox in the whole Instantly workspace, only using
    campaign_id to decide whether to run at all. That meant an unrelated
    venture's mailbox warming up could trip an FA Max EXCEPTIONS alert, and
    a genuinely unhealthy FA Max mailbox could be masked by an unrelated
    healthy one averaging into "not unhealthy" — the result proved nothing
    about FA Max specifically.

    Scoped instead to venture.relay_instantly_sender_email -- the one
    sending mailbox this repo's own data model actually tracks per venture
    (see src/utils/venture_config.py; it's a single str field, not a list,
    so this repo has no multi-mailbox model to scope against yet regardless
    of what Instantly's account-campaign-mapping API might otherwise allow).
    """
    if not campaign_id or not sender_email:
        return None
    accounts = instantly_service.list_accounts()
    all_emails = {a.get("email") for a in accounts if a.get("email")}
    sender_email_lower = sender_email.strip().lower()
    matched = next((e for e in all_emails if e.strip().lower() == sender_email_lower), None)
    if matched is None:
        return Trip(
            rule="fa_max_sender_mailbox_not_found",
            detail=f"Configured sender {sender_email!r} not found among "
                   f"{len(all_emails)} connected Instantly account(s) -- warmup "
                   f"cannot be checked for a mailbox Instantly doesn't report.",
        )
    warmup = {
        item.get("email"): item
        for item in instantly_service.get_warmup_analytics([matched])
        if item.get("email")
    }
    score = int((warmup.get(matched) or {}).get("health_score") or 0)
    if score >= WARMUP_SCORE_FLOOR:
        return None
    return Trip(
        rule="fa_max_warmup_score_low",
        detail=f"FA Max sending mailbox {matched!r} warmup score {score} is below "
               f"floor {WARMUP_SCORE_FLOOR} "
               f"(proxy only — not a placement measurement, see module docstring)",
    )


def _backflip_feed_stale_trip(session, now: datetime) -> Optional[Trip]:
    """WP-T2-3: proactive EXCEPTIONS alert when the Backflip campaign snapshot
    is missing or stale. backflip_campaign_reason() already blocks sends when
    the feed is stale (fail-closed), but that is reactive — an operator only
    learns of the staleness when a send attempt is refused. This check surfaces
    the condition proactively so the feed can be refreshed before live sends begin.

    Uses the same freshness window as backflip_campaign_reason() itself
    (fa_max_backflip_feed_max_age_hours), sourced from settings to keep the two
    in sync. If the singleton row doesn't exist at all (feed never imported),
    treats it as stale."""
    from config.settings import get_settings
    max_age = get_settings().fa_max_backflip_feed_max_age_hours
    fresh = session.execute(
        text(
            "SELECT last_success_at >= now() - make_interval(hours => :max_age) "
            "FROM fa_max_backflip_campaign_feed WHERE id = 1"
        ),
        {"max_age": max_age},
    ).scalar_one_or_none()
    if fresh is None:
        return Trip(
            rule="fa_max_backflip_feed_never_imported",
            detail="fa_max_backflip_campaign_feed has no row (id=1 missing) — "
                   "the Backflip suppression snapshot has never been imported. "
                   "Run: python scripts/import_backflip_suppression_csv.py --file <path>",
        )
    if not fresh:
        return Trip(
            rule="fa_max_backflip_feed_stale",
            detail=f"Backflip campaign snapshot is older than {max_age}h — "
                   "backflip_campaign_reason() is currently blocking ALL FA Max sends "
                   "on this basis (fail-closed). Re-import to unblock.",
        )
    return None


def _relay_failure_trip(session, now: datetime) -> Optional[Trip]:
    since = now - timedelta(hours=ROLLING_HOURS)
    row = session.execute(
        text(
            "SELECT "
            "  count(*) FILTER (WHERE status = 'sent') AS sent, "
            "  count(*) FILTER (WHERE status IN ('failed', 'skipped')) AS failed "
            "FROM relay_approval_queue "
            "WHERE venture_key = :venture AND updated_at >= :since"
        ),
        {"venture": FA_MAX_VENTURE, "since": since},
    ).mappings().one()
    sent, failed = int(row["sent"] or 0), int(row["failed"] or 0)
    total = sent + failed
    if total < MIN_SENT_FLOOR:
        return None
    rate = failed / total
    if rate <= FAILURE_RATE_CONCERN:
        return None
    return Trip(
        rule="fa_max_relay_failure_rate_high",
        detail=f"{failed}/{total} FA Max relay items failed or were blocked in the "
               f"trailing {ROLLING_HOURS}h ({rate:.1%})",
    )


def evaluate(*, now: Optional[datetime] = None) -> list[Trip]:
    now = now or datetime.now(timezone.utc)
    trips: list[Trip] = []
    venture = get_venture_config(FA_MAX_VENTURE)
    trips.append(_warmup_trip(venture.relay_instantly_campaign_id, venture.relay_instantly_sender_email))
    with get_db_context() as session:
        trips.append(_relay_failure_trip(session, now))
        trips.append(_backflip_feed_stale_trip(session, now))
    return [t for t in trips if t is not None]


def run_and_page(*, dry_run: bool = False) -> list[Trip]:
    trips = evaluate()
    if not trips:
        logger.info("[fa_max_send_health] no trips")
        return []

    for trip in trips:
        if dry_run:
            logger.info("[fa_max_send_health][DRY] would page %s: %s", trip.rule, trip.detail)
            continue
        # Dedup (skip if already pending/recently sent) and durability are
        # both exceptions_alert_queue's responsibility now — see module
        # docstring. A False return here means either "deduped, nothing new
        # queued" or "queued but this immediate attempt failed"; either way
        # the drain worker's next tick is the correct place to look, not a
        # retry here.
        exceptions_alert_queue.enqueue_and_attempt(
            venture_key=FA_MAX_VENTURE, rule=trip.rule, message=trip.detail,
        )
    return trips


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FA Max send-infrastructure health monitor")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    fired = run_and_page(dry_run=args.dry_run)
    logger.info("fa_max_send_health_monitor: %d rule(s) tripped", len(fired))
