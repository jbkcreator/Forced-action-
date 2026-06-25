"""
Lien Aggregator — aggregates record-level lien amounts from spoke tables
into financials.total_lien_amount.

Sprint 4.4: reads from legal_and_liens, tax_delinquencies, and code_violations
(is_lien = True) per property, sums them, and upserts financials.total_lien_amount.
"""
import logging
from typing import Optional

from sqlalchemy import text

from src.core.database import Database

logger = logging.getLogger(__name__)


def aggregate_lien_amounts(
    db: Optional[Database] = None,
    county_id: Optional[str] = None,
) -> int:
    """
    Aggregate lien amounts from spoke tables into financials.total_lien_amount.

    Sources:
      - legal_and_liens.amount        (all liens and judgments)
      - tax_delinquencies.total_amount_due  (tax certificate amounts)
      - code_violations.fine_amount   (only where is_lien = True)

    Returns the number of financials rows updated/inserted.
    """
    if db is None:
        db = Database()

    sql = """
            WITH lien_totals AS (
                SELECT
                    property_id,
                    COALESCE(SUM(amount), 0) AS legal_lien_total
                FROM legal_and_liens
                GROUP BY property_id
            ),
            tax_totals AS (
                SELECT
                    property_id,
                    COALESCE(SUM(total_amount_due), 0) AS tax_lien_total
                FROM tax_delinquencies
                GROUP BY property_id
            ),
            code_totals AS (
                SELECT
                    property_id,
                    COALESCE(SUM(fine_amount), 0) AS code_lien_total
                FROM code_violations
                WHERE is_lien = TRUE
                GROUP BY property_id
            ),
            all_properties AS (
                SELECT DISTINCT p.id AS property_id
                FROM properties p
                WHERE (:county_id IS NULL OR p.county_id = :county_id)
            ),
            aggregated AS (
                SELECT
                    ap.property_id,
                    COALESCE(lt.legal_lien_total, 0) +
                    COALESCE(tt.tax_lien_total, 0) +
                    COALESCE(ct.code_lien_total, 0) AS total_lien_amount
                FROM all_properties ap
                LEFT JOIN lien_totals lt ON ap.property_id = lt.property_id
                LEFT JOIN tax_totals tt ON ap.property_id = tt.property_id
                LEFT JOIN code_totals ct ON ap.property_id = ct.property_id
            )
            INSERT INTO financials (property_id, total_lien_amount, county_id)
            SELECT
                ag.property_id,
                ag.total_lien_amount,
                p.county_id
            FROM aggregated ag
            JOIN properties p ON p.id = ag.property_id
            ON CONFLICT (property_id)
            DO UPDATE SET
                total_lien_amount = EXCLUDED.total_lien_amount,
                county_id         = EXCLUDED.county_id
            WHERE financials.total_lien_amount IS DISTINCT FROM EXCLUDED.total_lien_amount
        """
    params = {"county_id": county_id}

    try:
        with db.session_scope() as session:
            result = session.execute(text(sql), params)
            session.commit()
            affected = result.rowcount
            logger.info("Lien aggregator: %d financials rows updated/inserted", affected)
            return affected
    except Exception:
        logger.exception("Lien aggregator failed (county_id=%s)", county_id or "all")
        raise