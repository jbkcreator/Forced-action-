"""
Appraiser sale-outcome connector.

Reads `financials` rows with a persisted `last_sale_qualified` flag
(populated by src/loaders/property_appraiser.py from pa_parser.py's HCPA
sales-history scrape — the flag was extracted at scrape time long before this
connector existed, just never written to the DB until now) and stages each
as a qualified_sale / unqualified_sale OutcomeCandidate.

No parcel-matching needed — every Financial row is already tied to a
Property via its FK, set at ingestion time by the existing appraiser loader.

CLI:
    python -m src.connectors.appraiser_sale_outcomes --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_QUALIFIED_SALE,
    EVENT_TYPE_UNQUALIFIED_SALE,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "appraiser_sale_outcomes"


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    rows = session.execute(
        text(
            "SELECT id, property_id, last_sale_date, last_sale_price, last_sale_qualified "
            "FROM financials "
            "WHERE county_id = :cid AND last_sale_qualified IS NOT NULL AND last_sale_date IS NOT NULL"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read = len(rows)

    for row in rows:
        try:
            event_type = EVENT_TYPE_QUALIFIED_SALE if row.last_sale_qualified else EVENT_TYPE_UNQUALIFIED_SALE
            candidate = OutcomeCandidateData(
                property_id=row.property_id,
                county_id=county_id,
                source_type=SOURCE_TYPE,
                source_table="financials",
                source_id=row.id,
                event_type=event_type,
                event_date=row.last_sale_date,
                amount=row.last_sale_price,
            )
            upsert_outcome_candidate(session, candidate)
            result.staged += 1
        except Exception:
            logger.exception("Failed to stage appraiser sale outcome for financials.id=%s", row.id)
            result.errors += 1

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage appraiser sale outcomes as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
