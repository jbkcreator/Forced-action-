"""
Email deliverability monitor for the shared sending domain.

V1 scope:
  - cold-outbound bounce rate from campaign_daily_analytics
  - transactional bounce + complaint rate from MessageOutcome / Mandrill events
  - Instantly warm-up health score for connected mailboxes

Alerts reuse ScraperAlertLog dedup semantics and default to soft-launch
log-only mode until SHIP_DELIVERABILITY_ALERTS=1.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional

from sqlalchemy import func, select, text

from src.core.database import get_db_context
from src.core.models import CampaignDailyAnalytics, EmailCampaign, MessageOutcome, ScraperAlertLog
from src.services.email import send_alert
from src.services import instantly_service
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

ROLLING_DAYS = 7
MIN_SEND_FLOOR = 20
COLD_BOUNCE_CONCERN = 0.02
COLD_BOUNCE_DANGER = 0.05
TRANSACTIONAL_COMPLAINT_DANGER = 0.001
WARMUP_SCORE_FLOOR = 70
ALERT_DEDUP_WINDOW_HOURS = 12
SOFT_LAUNCH_ENV = "SHIP_DELIVERABILITY_ALERTS"


@dataclass
class Trip:
    rule: str
    observed: str
    baseline: str
    threshold: str
    context: dict
    tripped_at: datetime

    def email_subject(self) -> str:
        return f"[FA][DELIVERABILITY] {self.rule}"

    def email_body(self) -> str:
        lines = [
            f"Rule:        {self.rule}",
            f"Tripped at:  {self.tripped_at.isoformat(timespec='seconds')}",
            f"Observed:    {self.observed}",
            f"Baseline:    {self.baseline}",
            f"Threshold:   {self.threshold}",
            "",
            "Context:",
        ]
        for k, v in self.context.items():
            lines.append(f"  {k}: {v}")
        return "\n".join(lines)


def _window_bounds(today: date) -> tuple[date, datetime]:
    start_date = today - timedelta(days=ROLLING_DAYS - 1)
    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    return start_date, start_dt


def _email_tracking_columns_ready(session) -> bool:
    required = {"recipient_email", "provider_message_id", "failure_reason"}
    rows = session.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'message_outcomes'
              AND column_name IN ('recipient_email', 'provider_message_id', 'failure_reason')
            """
        )
    ).fetchall()
    return {row[0] for row in rows} == required


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _recently_paged(session, rule: str, county_id: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ALERT_DEDUP_WINDOW_HOURS)
    existing = session.execute(
        select(ScraperAlertLog.id).where(
            ScraperAlertLog.alert_type == rule,
            ScraperAlertLog.county_id == county_id,
            ScraperAlertLog.alerted_at >= cutoff,
        ).limit(1)
    ).scalar_one_or_none()
    return existing is not None


def _record_paged(session, rule: str, county_id: str) -> None:
    session.add(ScraperAlertLog(source_type=rule, county_id=county_id, alert_type=rule))


def _is_soft_launch() -> bool:
    return os.getenv(SOFT_LAUNCH_ENV, "0").strip() not in ("1", "true", "True")


def _cold_outbound_trips(session, today: date, county_id: str) -> list[Trip]:
    start_date, _ = _window_bounds(today)
    totals = session.execute(
        select(
            func.coalesce(func.sum(CampaignDailyAnalytics.emails_sent), 0),
            func.coalesce(func.sum(CampaignDailyAnalytics.bounces), 0),
            func.count(func.distinct(CampaignDailyAnalytics.campaign_id)),
        )
        .select_from(CampaignDailyAnalytics)
        .join(EmailCampaign, EmailCampaign.id == CampaignDailyAnalytics.campaign_id)
        .where(
            CampaignDailyAnalytics.snapshot_date >= start_date,
            CampaignDailyAnalytics.snapshot_date <= today,
            EmailCampaign.county_id == county_id,
        )
    ).one()
    emails_sent, bounces, campaigns = (int(totals[0] or 0), int(totals[1] or 0), int(totals[2] or 0))
    if emails_sent < MIN_SEND_FLOOR:
        return []

    bounce_rate = _rate(bounces, emails_sent)
    if bounce_rate <= COLD_BOUNCE_CONCERN:
        return []

    severity = "danger" if bounce_rate > COLD_BOUNCE_DANGER else "concern"
    return [
        Trip(
            rule="deliverability_cold_bounce_rate_high",
            observed=f"{_fmt_pct(bounce_rate)} ({bounces}/{emails_sent})",
            baseline=f"rolling_{ROLLING_DAYS}d",
            threshold=(
                f">{_fmt_pct(COLD_BOUNCE_DANGER)} danger"
                if severity == "danger"
                else f">{_fmt_pct(COLD_BOUNCE_CONCERN)} concern"
            ),
            context={
                "county_id": county_id,
                "severity": severity,
                "campaigns_seen": campaigns,
                "window_start": start_date.isoformat(),
                "window_end": today.isoformat(),
            },
            tripped_at=datetime.now(timezone.utc),
        )
    ]


def _transactional_trips(session, today: date) -> list[Trip]:
    if not _email_tracking_columns_ready(session):
        logger.info("[deliverability_monitor] message_outcomes email-tracking columns missing; skipping transactional checks")
        return []

    _, start_dt = _window_bounds(today)
    sent_count = int(
        session.execute(
            select(func.count()).select_from(MessageOutcome).where(
                MessageOutcome.message_type == "email",
                MessageOutcome.channel == "mandrill",
                MessageOutcome.sent_at >= start_dt,
            )
        ).scalar_one() or 0
    )
    if sent_count < MIN_SEND_FLOOR:
        return []

    bounce_count = int(
        session.execute(
            select(func.count()).select_from(MessageOutcome).where(
                MessageOutcome.message_type == "email",
                MessageOutcome.channel == "mandrill",
                MessageOutcome.sent_at >= start_dt,
                MessageOutcome.failure_reason.in_(("hard_bounce", "soft_bounce", "reject")),
            )
        ).scalar_one() or 0
    )
    complaint_count = int(
        session.execute(
            select(func.count()).select_from(MessageOutcome).where(
                MessageOutcome.message_type == "email",
                MessageOutcome.channel == "mandrill",
                MessageOutcome.sent_at >= start_dt,
                MessageOutcome.failure_reason == "spam",
            )
        ).scalar_one() or 0
    )

    trips: list[Trip] = []
    bounce_rate = _rate(bounce_count, sent_count)
    if bounce_rate > COLD_BOUNCE_CONCERN:
        severity = "danger" if bounce_rate > COLD_BOUNCE_DANGER else "concern"
        trips.append(
            Trip(
                rule="deliverability_transactional_bounce_rate_high",
                observed=f"{_fmt_pct(bounce_rate)} ({bounce_count}/{sent_count})",
                baseline=f"rolling_{ROLLING_DAYS}d",
                threshold=(
                    f">{_fmt_pct(COLD_BOUNCE_DANGER)} danger"
                    if severity == "danger"
                    else f">{_fmt_pct(COLD_BOUNCE_CONCERN)} concern"
                ),
                context={
                    "severity": severity,
                    "window_start": start_dt.date().isoformat(),
                    "window_end": today.isoformat(),
                },
                tripped_at=datetime.now(timezone.utc),
            )
        )

    complaint_rate = _rate(complaint_count, sent_count)
    if complaint_rate > TRANSACTIONAL_COMPLAINT_DANGER:
        trips.append(
            Trip(
                rule="deliverability_transactional_complaint_rate_high",
                observed=f"{_fmt_pct(complaint_rate)} ({complaint_count}/{sent_count})",
                baseline=f"rolling_{ROLLING_DAYS}d",
                threshold=f">{_fmt_pct(TRANSACTIONAL_COMPLAINT_DANGER)} danger",
                context={
                    "window_start": start_dt.date().isoformat(),
                    "window_end": today.isoformat(),
                },
                tripped_at=datetime.now(timezone.utc),
            )
        )

    return trips


def _warmup_trips(
    warmup_fetcher: Optional[Callable[[], list[dict]]] = None,
) -> list[Trip]:
    fetcher = warmup_fetcher or _load_warmup_accounts
    accounts = fetcher()
    unhealthy = [a for a in accounts if int(a.get("health_score") or 0) < WARMUP_SCORE_FLOOR]
    if not unhealthy:
        return []

    affected = ", ".join(f"{a.get('email')}={a.get('health_score')}" for a in unhealthy[:10])
    return [
        Trip(
            rule="deliverability_warmup_score_low",
            observed=affected,
            baseline="connected_mailboxes",
            threshold=f"any health_score < {WARMUP_SCORE_FLOOR}",
            context={
                "mailbox_count": len(accounts),
                "affected_count": len(unhealthy),
            },
            tripped_at=datetime.now(timezone.utc),
        )
    ]


def _load_warmup_accounts() -> list[dict]:
    accounts = instantly_service.list_accounts()
    emails = [a.get("email") for a in accounts if a.get("email")]
    if not emails:
        return []

    warmup_by_email = {
        item.get("email"): item
        for item in instantly_service.get_warmup_analytics(emails)
        if item.get("email")
    }
    result = []
    for account in accounts:
        email = account.get("email")
        if not email:
            continue
        warmup = warmup_by_email.get(email, {})
        result.append(
            {
                "email": email,
                "health_score": warmup.get("health_score") or warmup.get("warmup_score") or 0,
                "warmup_enabled": account.get("warmup_enabled", False),
            }
        )
    return result


def evaluate(
    session,
    *,
    today: Optional[date] = None,
    county_id: str = "hillsborough",
    warmup_fetcher: Optional[Callable[[], list[dict]]] = None,
) -> list[Trip]:
    today = today or date.today()
    trips: list[Trip] = []
    trips.extend(_cold_outbound_trips(session, today, county_id))
    trips.extend(_transactional_trips(session, today))
    trips.extend(_warmup_trips(warmup_fetcher))
    return trips


def run_and_page(
    *,
    today: Optional[date] = None,
    county_id: str = "hillsborough",
    dry_run: bool = False,
    warmup_fetcher: Optional[Callable[[], list[dict]]] = None,
) -> list[Trip]:
    with get_db_context() as session:
        trips = evaluate(session, today=today, county_id=county_id, warmup_fetcher=warmup_fetcher)
        if not trips:
            logger.info("[deliverability_monitor] no deliverability anomalies tripped")
            return []

        for trip in trips:
            if _is_soft_launch():
                logger.info("[deliverability_monitor][SOFT-LAUNCH] %s would have fired: %s", trip.rule, trip.context)
                continue
            if _recently_paged(session, trip.rule, county_id):
                logger.info("[deliverability_monitor] %s already paged in last %dh", trip.rule, ALERT_DEDUP_WINDOW_HOURS)
                continue
            if dry_run:
                logger.info("[deliverability_monitor][DRY] would send subject=%s\n%s", trip.email_subject(), trip.email_body())
                continue
            try:
                send_alert(trip.email_subject(), trip.email_body())
                _record_paged(session, trip.rule, county_id)
            except Exception as exc:
                logger.error("[deliverability_monitor] failed to send alert for %s: %s", trip.rule, exc)
        session.commit()
        return trips


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deliverability monitor")
    parser.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else date.today()
    fired = run_and_page(today=target, county_id=args.county_id, dry_run=args.dry_run)
    logger.info(
        "deliverability monitor: %d rule(s) tripped on %s", len(fired), target.isoformat()
    )
