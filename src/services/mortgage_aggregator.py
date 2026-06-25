"""
Mortgage Aggregator (Sprint 4.4) — rolls each property's most recent recorded
mortgage amount into financials.est_mortgage_bal.

This is the missing link in the equity pipeline: deeds carry a mortgage_amount
(extracted from mortgage-type documents), but nothing fed it into financials, so
equity_compute treated mortgage debt as zero. This aggregator populates
est_mortgage_bal so the equity formula can subtract real mortgage debt.

We take the LATEST mortgage per property (by record_date), not the sum: a newer
mortgage refinances/replaces older ones, so the most recent recorded amount is
the best estimate of current debt. Summing would double-count refinances.
"""
import logging
from typing import Optional

from sqlalchemy import text

from src.core.database import Database

logger = logging.getLogger(__name__)

_SQL = text(
    """
    WITH latest_mortgage AS (
        SELECT DISTINCT ON (d.property_id)
            d.property_id,
            d.mortgage_amount AS est_mortgage_bal
        FROM deeds d
        JOIN properties p ON p.id = d.property_id
        WHERE d.mortgage_amount IS NOT NULL
          AND (:county_id IS NULL OR p.county_id = :county_id)
        ORDER BY d.property_id, d.record_date DESC NULLS LAST
    )
    INSERT INTO financials (property_id, est_mortgage_bal, county_id)
    SELECT lm.property_id, lm.est_mortgage_bal, p.county_id
    FROM latest_mortgage lm
    JOIN properties p ON p.id = lm.property_id
    ON CONFLICT (property_id)
    DO UPDATE SET
        est_mortgage_bal = EXCLUDED.est_mortgage_bal,
        county_id        = EXCLUDED.county_id
    WHERE financials.est_mortgage_bal IS DISTINCT FROM EXCLUDED.est_mortgage_bal
    """
)


def aggregate_mortgage_balances(
    db: Optional[Database] = None,
    county_id: Optional[str] = None,
    session=None,
) -> int:
    """Populate financials.est_mortgage_bal from each property's latest deed
    mortgage_amount. Returns the number of financials rows updated/inserted.

    If `session` is provided the statement runs on it without committing (the
    caller owns the transaction — used in tests). Otherwise a Database session
    is opened and committed.
    """
    params = {"county_id": county_id}

    if session is not None:
        return session.execute(_SQL, params).rowcount

    if db is None:
        db = Database()
    try:
        with db.session_scope() as s:
            affected = s.execute(_SQL, params).rowcount
            s.commit()
            logger.info("Mortgage aggregator: %d financials rows updated/inserted", affected)
            return affected
    except Exception:
        logger.exception("Mortgage aggregator failed (county_id=%s)", county_id or "all")
        raise
