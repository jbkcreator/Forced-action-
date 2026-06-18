"""Monthly affiliate payout job (Stream D).

Runs on the 1st of each month for the *prior* calendar month: writes Commission
accrual lines for every Active Paid Referral and Commission Clawback lines for
reversed invoices. Log-only — records amounts owed in the Payout Ledger; no
Stripe disbursement. Idempotent, so a re-run for the same month is safe.
"""
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from src.core.database import get_db_context
from src.services.affiliate_engine import run_monthly_payout

logger = logging.getLogger(__name__)


def _prior_calendar_month(today: date) -> date:
    first_of_this = today.replace(day=1)
    return (first_of_this - timedelta(days=1)).replace(day=1)


def run_affiliate_payout(period_month: Optional[date] = None) -> dict:
    if period_month is None:
        period_month = _prior_calendar_month(datetime.now(timezone.utc).date())
    with get_db_context() as db:
        result = run_monthly_payout(db, period_month)
        # get_db_context commits on clean exit
    logger.info("Affiliate payout complete: %s", result)
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    run_affiliate_payout()
