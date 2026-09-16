"""
WP-6 Repeat & Maturity Engine.

Runs nightly, queries borrower_ledger_events + buyer_entities for four trigger
conditions, and posts structured alerts to the RELATIONSHIPS Slack channel.

Monitors:
  loan_maturity       — deed acquired ~12 months ago, property not yet sold,
                        financed buyer: loan likely maturing in ~45 days
  next_project        — deed_sale or permit_closed yesterday: capital freed,
                        borrower likely shopping for next deal
  dscr_day120         — deed acquired ~120 days ago, not yet sold, financed:
                        DSCR loan seasoning window reached, refi-eligible
  portfolio_expansion — borrower just crossed a milestone (3, 5, or 10 deals)
                        via a deed_acquisition in the last 7 days

Idempotency: each fired alert is recorded in borrower_monitor_log with a UNIQUE
constraint on (buyer_entity_id, monitor_type, source_event_id). Re-running the
engine the same day produces zero new alerts.

Slack is optional — if RELATIONSHIPS_SLACK_CHANNEL or slack_bot_token is unset,
alerts are logged at INFO level instead (never a silent failure).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings

logger = logging.getLogger(__name__)

_LOAN_MATURITY_TERM_DAYS = 365        # assumed hard-money term
_LOAN_MATURITY_WINDOW_DAYS = 45       # alert window before maturity
_DSCR_SEASONING_DAYS = 120
_DSCR_WINDOW_DAYS = 7                 # fire within 7-day window around day 120
_EXPANSION_MILESTONES = {3, 5, 10}
_EXPANSION_LOOKBACK_DAYS = 7
_NEXT_PROJECT_RETRY_DAYS = 30

MONITOR_TYPES = ("loan_maturity", "next_project", "dscr_day120", "portfolio_expansion")


@dataclass
class MonitorAlert:
    monitor_type: str
    buyer_entity_id: int
    canonical_name: str
    source_event_id: Optional[int]
    property_address: Optional[str]
    event_date: Optional[date]
    days_since: Optional[int]
    total_purchase_count: int
    total_cash_volume: float
    buyer_type: Optional[str]
    financing_signal: Optional[str]
    extra: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Monitor queries
# --------------------------------------------------------------------------- #

def _check_loan_maturity(session: Session, today: date) -> list[MonitorAlert]:
    """
    Deed acquisitions where event_date is between (term - window) and term days
    ago, property not yet sold, buyer is financed or unknown.
    """
    window_start = today - timedelta(days=_LOAN_MATURITY_TERM_DAYS)
    window_end = today - timedelta(days=_LOAN_MATURITY_TERM_DAYS - _LOAN_MATURITY_WINDOW_DAYS)

    rows = session.execute(text("""
        SELECT
            ble.id              AS event_id,
            ble.buyer_entity_id,
            be.canonical_name,
            ble.property_id,
            p.address           AS property_address,
            ble.event_date,
            be.total_purchase_count,
            be.total_cash_volume,
            be.buyer_type,
            be.financing_signal,
            (CURRENT_DATE - ble.event_date) AS days_since
        FROM borrower_ledger_events ble
        JOIN buyer_entities be ON be.id = ble.buyer_entity_id
        LEFT JOIN properties p ON p.id = ble.property_id
        WHERE ble.event_type = 'deed_acquisition'
          AND ble.event_date BETWEEN :window_start AND :window_end
          AND be.financing_signal IN ('financed', 'unknown')
          AND NOT EXISTS (
              SELECT 1 FROM borrower_ledger_events sale
              WHERE sale.buyer_entity_id = ble.buyer_entity_id
                AND sale.property_id = ble.property_id
                AND sale.event_type = 'deed_sale'
                AND sale.event_date > ble.event_date
          )
          AND NOT EXISTS (
              SELECT 1 FROM borrower_monitor_log bml
              WHERE bml.buyer_entity_id = ble.buyer_entity_id
                AND bml.monitor_type = 'loan_maturity'
                AND bml.source_event_id = ble.id
          )
    """), {"window_start": window_start, "window_end": window_end}).mappings().all()

    alerts = []
    for r in rows:
        days_to_maturity = _LOAN_MATURITY_TERM_DAYS - r["days_since"]
        alerts.append(MonitorAlert(
            monitor_type="loan_maturity",
            buyer_entity_id=r["buyer_entity_id"],
            canonical_name=r["canonical_name"],
            source_event_id=r["event_id"],
            property_address=r["property_address"],
            event_date=r["event_date"],
            days_since=r["days_since"],
            total_purchase_count=r["total_purchase_count"],
            total_cash_volume=float(r["total_cash_volume"] or 0),
            buyer_type=r["buyer_type"],
            financing_signal=r["financing_signal"],
            extra={"days_to_maturity": days_to_maturity},
        ))
    return alerts


def _check_next_project(session: Session, today: date) -> list[MonitorAlert]:
    """
    deed_sale or permit_closed events from yesterday, buyer has prior acquisitions.
    """
    yesterday = today - timedelta(days=1)
    retry_cutoff = today - timedelta(days=_NEXT_PROJECT_RETRY_DAYS)

    rows = session.execute(text("""
        SELECT
            ble.id              AS event_id,
            ble.buyer_entity_id,
            be.canonical_name,
            ble.property_id,
            p.address           AS property_address,
            ble.event_date,
            ble.event_type,
            be.total_purchase_count,
            be.total_cash_volume,
            be.buyer_type,
            be.financing_signal
        FROM borrower_ledger_events ble
        JOIN buyer_entities be ON be.id = ble.buyer_entity_id
        LEFT JOIN properties p ON p.id = ble.property_id
        WHERE ble.event_type IN ('deed_sale', 'permit_closed')
          AND ble.event_date BETWEEN :retry_cutoff AND :yesterday
          AND be.total_purchase_count >= 1
          AND NOT EXISTS (
              SELECT 1 FROM borrower_monitor_log bml
              WHERE bml.buyer_entity_id = ble.buyer_entity_id
                AND bml.monitor_type = 'next_project'
                AND bml.source_event_id = ble.id
          )
    """), {"retry_cutoff": retry_cutoff, "yesterday": yesterday}).mappings().all()

    alerts = []
    for r in rows:
        alerts.append(MonitorAlert(
            monitor_type="next_project",
            buyer_entity_id=r["buyer_entity_id"],
            canonical_name=r["canonical_name"],
            source_event_id=r["event_id"],
            property_address=r["property_address"],
            event_date=r["event_date"],
            days_since=1,
            total_purchase_count=r["total_purchase_count"],
            total_cash_volume=float(r["total_cash_volume"] or 0),
            buyer_type=r["buyer_type"],
            financing_signal=r["financing_signal"],
            extra={"trigger_event": r["event_type"]},
        ))
    return alerts


def _check_dscr_day120(session: Session, today: date) -> list[MonitorAlert]:
    """
    Deed acquisitions from ~120 days ago (within a 7-day window), property not
    yet sold, financed buyer: DSCR seasoning window reached.
    """
    window_start = today - timedelta(days=_DSCR_SEASONING_DAYS + _DSCR_WINDOW_DAYS)
    window_end = today - timedelta(days=_DSCR_SEASONING_DAYS)

    rows = session.execute(text("""
        SELECT
            ble.id              AS event_id,
            ble.buyer_entity_id,
            be.canonical_name,
            ble.property_id,
            p.address           AS property_address,
            ble.event_date,
            be.total_purchase_count,
            be.total_cash_volume,
            be.buyer_type,
            be.financing_signal,
            (CURRENT_DATE - ble.event_date) AS days_since
        FROM borrower_ledger_events ble
        JOIN buyer_entities be ON be.id = ble.buyer_entity_id
        LEFT JOIN properties p ON p.id = ble.property_id
        WHERE ble.event_type = 'deed_acquisition'
          AND ble.event_date BETWEEN :window_start AND :window_end
          AND be.financing_signal IN ('financed', 'unknown')
          AND NOT EXISTS (
              SELECT 1 FROM borrower_ledger_events sale
              WHERE sale.buyer_entity_id = ble.buyer_entity_id
                AND sale.property_id = ble.property_id
                AND sale.event_type = 'deed_sale'
                AND sale.event_date > ble.event_date
          )
          AND NOT EXISTS (
              SELECT 1 FROM borrower_monitor_log bml
              WHERE bml.buyer_entity_id = ble.buyer_entity_id
                AND bml.monitor_type = 'dscr_day120'
                AND bml.source_event_id = ble.id
          )
    """), {"window_start": window_start, "window_end": window_end}).mappings().all()

    alerts = []
    for r in rows:
        alerts.append(MonitorAlert(
            monitor_type="dscr_day120",
            buyer_entity_id=r["buyer_entity_id"],
            canonical_name=r["canonical_name"],
            source_event_id=r["event_id"],
            property_address=r["property_address"],
            event_date=r["event_date"],
            days_since=r["days_since"],
            total_purchase_count=r["total_purchase_count"],
            total_cash_volume=float(r["total_cash_volume"] or 0),
            buyer_type=r["buyer_type"],
            financing_signal=r["financing_signal"],
            extra={},
        ))
    return alerts


def _check_portfolio_expansion(session: Session, today: date) -> list[MonitorAlert]:
    """
    Buyers who have CROSSED a milestone (3, 5, 10) via a deed_acquisition in the
    last 7 days.

    total_purchase_count is a batch recompute (refresh_portfolio_aggregates), so
    a single sweep can jump an active buyer past a milestone (2→4, 4→6) without
    ever landing exactly on it. Matching on `>=` the highest crossed milestone —
    and deduping on the highest milestone already alerted (stored monitor_value)
    — means a skipped milestone still fires, and each milestone fires at most
    once per entity.
    """
    lookback = today - timedelta(days=_EXPANSION_LOOKBACK_DAYS)
    milestones = list(_EXPANSION_MILESTONES)
    min_milestone = min(milestones)

    # DISTINCT ON (buyer_entity_id) — one alert per entity per run. `milestone`
    # is the highest configured milestone the current count has reached; the
    # NOT EXISTS suppresses it only if an equal-or-higher milestone already fired.
    rows = session.execute(text("""
        SELECT DISTINCT ON (ble.buyer_entity_id)
            ble.id              AS event_id,
            ble.buyer_entity_id,
            be.canonical_name,
            ble.event_date,
            ble.property_id,
            p.address           AS property_address,
            be.total_purchase_count,
            be.total_cash_volume,
            be.buyer_type,
            be.financing_signal,
            (SELECT max(m) FROM unnest(:milestones) AS m
                 WHERE m <= be.total_purchase_count) AS milestone
        FROM borrower_ledger_events ble
        JOIN buyer_entities be ON be.id = ble.buyer_entity_id
        LEFT JOIN properties p ON p.id = ble.property_id
        WHERE ble.event_type = 'deed_acquisition'
          AND ble.event_date >= :lookback
          AND be.total_purchase_count >= :min_milestone
          AND NOT EXISTS (
              SELECT 1 FROM borrower_monitor_log bml
              WHERE bml.buyer_entity_id = ble.buyer_entity_id
                AND bml.monitor_type = 'portfolio_expansion'
                AND bml.monitor_value >= (
                    SELECT max(m) FROM unnest(:milestones) AS m
                        WHERE m <= be.total_purchase_count
                )
          )
        ORDER BY ble.buyer_entity_id, ble.id DESC
    """), {
        "lookback": lookback,
        "milestones": milestones,
        "min_milestone": min_milestone,
    }).mappings().all()

    alerts = []
    for r in rows:
        alerts.append(MonitorAlert(
            monitor_type="portfolio_expansion",
            buyer_entity_id=r["buyer_entity_id"],
            canonical_name=r["canonical_name"],
            source_event_id=r["event_id"],
            property_address=r["property_address"],
            event_date=r["event_date"],
            days_since=None,
            total_purchase_count=r["total_purchase_count"],
            total_cash_volume=float(r["total_cash_volume"] or 0),
            buyer_type=r["buyer_type"],
            financing_signal=r["financing_signal"],
            extra={"milestone": r["milestone"]},
        ))
    return alerts


# --------------------------------------------------------------------------- #
# Slack formatting
# --------------------------------------------------------------------------- #

_MONITOR_EMOJI = {
    "loan_maturity":       ":alarm_clock:",
    "next_project":        ":rocket:",
    "dscr_day120":         ":bank:",
    "portfolio_expansion": ":chart_with_upwards_trend:",
}

_MONITOR_TITLE = {
    "loan_maturity":       "Loan Maturity Alert",
    "next_project":        "Next Project Signal",
    "dscr_day120":         "DSCR Day-120 — Refi Eligible",
    "portfolio_expansion": "Portfolio Expansion Milestone",
}


def _format_alert_blocks(alert: MonitorAlert) -> tuple[str, list]:
    emoji = _MONITOR_EMOJI[alert.monitor_type]
    title = _MONITOR_TITLE[alert.monitor_type]
    volume_str = f"${alert.total_cash_volume:,.0f}" if alert.total_cash_volume else "n/a"

    summary_text = f"{emoji} {title} — {alert.canonical_name}"

    detail_lines = []
    if alert.property_address:
        detail_lines.append(f"*Property:* {alert.property_address}")
    if alert.event_date:
        detail_lines.append(f"*Acquired:* {alert.event_date} ({alert.days_since} days ago)")
    if alert.monitor_type == "loan_maturity":
        dtm = alert.extra.get("days_to_maturity")
        detail_lines.append(f"*Est. maturity in:* ~{dtm} days")
    if alert.monitor_type == "next_project":
        detail_lines.append(f"*Trigger:* {alert.extra.get('trigger_event', '?')} yesterday — capital freed")
    if alert.monitor_type == "dscr_day120":
        detail_lines.append(f"*DSCR seasoning:* {alert.days_since} days — refi window open")
    if alert.monitor_type == "portfolio_expansion":
        detail_lines.append(f"*Milestone:* deal #{alert.extra.get('milestone')} just crossed")

    portfolio_line = (
        f"*Portfolio:* {alert.total_purchase_count} deals · "
        f"{volume_str} volume · "
        f"{alert.buyer_type or 'type unknown'} · "
        f"{alert.financing_signal or 'financing unknown'}"
    )
    detail_lines.append(portfolio_line)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{emoji} {title}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(detail_lines)},
        },
    ]
    return summary_text, blocks


# --------------------------------------------------------------------------- #
# Slack dispatch
# --------------------------------------------------------------------------- #

def _post_alert(alert: MonitorAlert) -> Optional[str]:
    """Post one alert to Slack. Returns ts on success, None if unconfigured."""
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.relationships_slack_channel

    summary_text, blocks = _format_alert_blocks(alert)

    if not token or not channel:
        logger.info(
            "[repeat_maturity] Slack not configured — alert logged only: %s | %s",
            alert.monitor_type, alert.canonical_name,
        )
        return None

    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError:
        logger.warning("[repeat_maturity] slack_sdk not installed — alert logged only")
        return None

    try:
        client = WebClient(token=token.get_secret_value())
        resp = client.chat_postMessage(
            channel=channel,
            text=summary_text,
            blocks=blocks,
        )
        return resp.get("ts")
    except SlackApiError as exc:
        logger.error(
            "[repeat_maturity] Slack post failed for %s/%d: %s",
            alert.monitor_type, alert.buyer_entity_id, exc,
        )
        return None


# --------------------------------------------------------------------------- #
# Idempotency write
# --------------------------------------------------------------------------- #

def _record_fired(session: Session, alert: MonitorAlert, slack_ts: Optional[str]) -> None:
    session.execute(text("""
        INSERT INTO borrower_monitor_log
            (buyer_entity_id, monitor_type, source_event_id, monitor_value, slack_ts)
        VALUES
            (:entity_id, :monitor_type, :source_event_id, :monitor_value, :slack_ts)
        ON CONFLICT (buyer_entity_id, monitor_type, source_event_id) DO NOTHING
    """), {
        "entity_id": alert.buyer_entity_id,
        "monitor_type": alert.monitor_type,
        "source_event_id": alert.source_event_id,
        "monitor_value": alert.extra.get("milestone"),
        "slack_ts": slack_ts,
    })


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #

def run_monitors(
    session: Session,
    today: Optional[date] = None,
    *,
    deliver: bool = True,
) -> list[MonitorAlert]:
    """
    Run all 4 monitors and return eligible alerts. When ``deliver`` is true,
    post candidates to Slack and record only confirmed deliveries. Preview
    mode performs neither side effect.
    Does not commit — caller controls the transaction boundary.
    """
    if today is None:
        today = date.today()

    all_alerts: list[MonitorAlert] = []

    checkers = [
        _check_loan_maturity,
        _check_next_project,
        _check_dscr_day120,
        _check_portfolio_expansion,
    ]

    for checker in checkers:
        try:
            alerts = checker(session, today)
        except Exception as exc:
            logger.error("[repeat_maturity] %s failed: %s", checker.__name__, exc)
            continue

        for alert in alerts:
            all_alerts.append(alert)
            if not deliver:
                continue

            slack_ts = _post_alert(alert)
            if slack_ts is None:
                logger.warning(
                    "[repeat_maturity] delivery failed; leaving %s/%d retryable",
                    alert.monitor_type, alert.buyer_entity_id,
                )
                continue

            _record_fired(session, alert, slack_ts)
            logger.info(
                "[repeat_maturity] fired %s for entity %d (%s)",
                alert.monitor_type, alert.buyer_entity_id, alert.canonical_name,
            )

    return all_alerts
