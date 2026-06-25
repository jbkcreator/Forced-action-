"""
Equity Compute — calculates net equity profile for each property.

Formula:
    total_debt   = COALESCE(est_mortgage_bal, 0) + COALESCE(total_lien_amount, 0)
    est_equity   = assessed_value_mkt - total_debt
    equity_pct   = (est_equity / NULLIF(assessed_value_mkt, 0)) * 100

Sprint 4.4: nightly batch sweep. Skips properties with NULL assessed_value_mkt.
"""
import logging
from typing import Optional

from sqlalchemy import text

from src.core.database import Database

logger = logging.getLogger(__name__)


def compute_equity_profiles(
    db: Optional[Database] = None,
    county_id: Optional[str] = None,
    session=None,
) -> int:
    """
    Compute est_equity, equity_pct, and total_debt for all properties
    that have an assessed_value_mkt.

    Runs as a single SQL upsert: for each property with a non-NULL
    assessed_value_mkt, computes the debt and equity figures, then
    UPSERTs into financials.

    Returns the number of financials rows updated/inserted.
    """
    sql = """
        WITH equity_inputs AS (
            SELECT
                p.id AS property_id,
                p.county_id,
                f.assessed_value_mkt,
                COALESCE(f.est_mortgage_bal, 0) AS est_mortgage_bal,
                COALESCE(f.total_lien_amount, 0) AS total_lien_amount
            FROM properties p
            JOIN financials f ON f.property_id = p.id
            WHERE f.assessed_value_mkt IS NOT NULL
              AND (:county_id IS NULL OR p.county_id = :county_id)
        ),
        computed AS (
            SELECT
                property_id,
                county_id,
                assessed_value_mkt,
                est_mortgage_bal,
                total_lien_amount,
                (est_mortgage_bal + total_lien_amount) AS total_debt,
                (assessed_value_mkt - (est_mortgage_bal + total_lien_amount)) AS est_equity,
                CASE
                    WHEN assessed_value_mkt = 0 THEN NULL
                    ELSE ROUND(
                        ((assessed_value_mkt - (est_mortgage_bal + total_lien_amount))
                         / assessed_value_mkt * 100)::numeric, 2
                    )
                END AS equity_pct
            FROM equity_inputs
        )
        INSERT INTO financials (
            property_id, est_equity, equity_pct, total_debt,
            est_mortgage_bal, total_lien_amount, county_id
        )
        SELECT
            c.property_id,
            c.est_equity,
            c.equity_pct,
            c.total_debt,
            c.est_mortgage_bal,
            c.total_lien_amount,
            c.county_id
        FROM computed c
        ON CONFLICT (property_id)
        DO UPDATE SET
            est_equity        = EXCLUDED.est_equity,
            equity_pct        = EXCLUDED.equity_pct,
            total_debt        = EXCLUDED.total_debt,
            est_mortgage_bal  = EXCLUDED.est_mortgage_bal,
            total_lien_amount = EXCLUDED.total_lien_amount,
            county_id         = EXCLUDED.county_id
        WHERE financials.est_equity IS DISTINCT FROM EXCLUDED.est_equity
           OR financials.equity_pct IS DISTINCT FROM EXCLUDED.equity_pct
           OR financials.total_debt IS DISTINCT FROM EXCLUDED.total_debt
    """
    params = {"county_id": county_id}

    if session is not None:
        return session.execute(text(sql), params).rowcount

    if db is None:
        db = Database()
    try:
        with db.session_scope() as s:
            result = s.execute(text(sql), params)
            s.commit()
            affected = result.rowcount
            logger.info("Equity compute: %d financials rows updated/inserted", affected)
            return affected
    except Exception:
        logger.exception("Equity compute failed (county_id=%s)", county_id or "all")
        raise