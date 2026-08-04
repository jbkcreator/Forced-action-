"""
QUALITY-v2.2 Q2 — Monthly agent P&L cron entry point.

Cron: 0 8 1 * *  (08:00 UTC on the 1st of each month, runs for the prior month)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from dateutil.relativedelta import relativedelta
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.agent_pnl_service import build_monthly_email, rollup_month

logger = logging.getLogger(__name__)


def run(
    session: Optional[Session] = None,
    period_month: Optional[date] = None,
) -> None:
    """Compute P&L for all seats, upsert into agent_pnl, send email report.

    Args:
        session:      DB session. If None, opens one from DATABASE_URL.
        period_month: First-of-month date to roll up. Defaults to prior month.
    """
    owns_session = session is None
    if owns_session:
        from src.core.database import SessionLocal
        session = SessionLocal()

    try:
        if period_month is None:
            today = date.today()
            period_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)

        rows = rollup_month(session, period_month)
        _upsert_rows(session, rows)
        if owns_session:
            session.commit()

        body = build_monthly_email(rows, period_month)
        _send_report(subject=f"Agent Fleet P&L — {period_month.strftime('%B %Y')}", body=body)
        logger.info("agent_pnl_monthly complete: period=%s seats=%d", period_month, len(rows))
    except Exception:
        logger.exception("agent_pnl_monthly failed for period=%s", period_month)
        if owns_session:
            session.rollback()
        raise
    finally:
        if owns_session:
            session.close()


def _upsert_rows(session: Session, rows: list[dict]) -> None:
    for row in rows:
        session.execute(sa_text("""
            INSERT INTO agent_pnl (
                seat, period_month,
                attributed_gp_cents, compute_cost_cents, data_cost_cents,
                founder_minutes_cost_cents, net_contribution_cents,
                binding_constraint, approval_count, queue_dwell_median_minutes
            ) VALUES (
                :seat, :period_month,
                :attributed_gp_cents, :compute_cost_cents, :data_cost_cents,
                :founder_minutes_cost_cents, :net_contribution_cents,
                :binding_constraint, :approval_count, :queue_dwell_median_minutes
            )
            ON CONFLICT (seat, period_month) DO UPDATE SET
                attributed_gp_cents         = EXCLUDED.attributed_gp_cents,
                compute_cost_cents          = EXCLUDED.compute_cost_cents,
                data_cost_cents             = EXCLUDED.data_cost_cents,
                founder_minutes_cost_cents  = EXCLUDED.founder_minutes_cost_cents,
                net_contribution_cents      = EXCLUDED.net_contribution_cents,
                binding_constraint          = EXCLUDED.binding_constraint,
                approval_count              = EXCLUDED.approval_count,
                queue_dwell_median_minutes  = EXCLUDED.queue_dwell_median_minutes
        """), row)


def _send_report(subject: str, body: str) -> None:
    settings = get_settings()
    to = getattr(settings, "alert_email", None)
    if not to:
        logger.warning("agent_pnl_monthly: ALERT_EMAIL not set — skipping email")
        return
    try:
        from src.services.email import send_email
        send_email(to, subject, body_text=body)
    except Exception:
        logger.warning("agent_pnl_monthly: failed to send email report", exc_info=True)


if __name__ == "__main__":
    run()
