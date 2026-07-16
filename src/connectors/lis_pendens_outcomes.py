"""
Lis-pendens distress-to-resolution outcome connector.

`Foreclosure.lis_pendens_date` and `Foreclosure.auction_date` live on the same
row (src/loaders/lis_pendens.py routes LP filings into the `foreclosures`
table). Rows that DO reach auction are fully owned by
`foreclosure_outcomes.py` (sold_to/case_status classification) — this
connector never touches them.

This connector's scope is the complement: LP filed, `auction_date IS NULL`
(the case never reached the auction stage). Resolution signal is a subsequent
deed on the same property, never case-status text (ADR 0022, same rule
CDE-03/CDE-08 use) — a qualifying resale after `lis_pendens_date` means the
property sold before auction. No window cap: an LP-to-sale gap can legitimately
span years. No deed within the data -> unresolved -> nothing emitted (a cured
or dismissed case with no sale is invisible in v1, same accepted gap as
CDE-08's probate/lien arcs).

Note: the quitclaim-exclusion check below duplicates CDE-03's `classify_deed`
by substring rather than importing it — CDE-05 branched directly off dev
before CDE-03 merged. Converge on the shared import once CDE-03/05/08 land
on the same branch (same debt CDE-08 already flagged for itself).

CLI:
    python -m src.connectors.lis_pendens_outcomes --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_LP_SOLD_PRE_AUCTION,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "lis_pendens_outcomes"
SOURCE_TABLE = "foreclosures"

_RESALE_JOIN = (
    "LEFT JOIN LATERAL ("
    "  SELECT d.instrument_number, d.record_date, d.sale_price, d.grantee"
    "  FROM deeds d"
    "  WHERE d.property_id = f.property_id"
    "    AND d.record_date > f.lis_pendens_date"
    "    AND (d.sale_price IS NULL OR d.sale_price >= 100)"
    "    AND (d.deed_type IS NULL OR d.deed_type NOT ILIKE '%quit%')"
    "  ORDER BY d.record_date ASC LIMIT 1"
    ") r ON true"
)


def _build_payload(row) -> dict:
    return {
        "source_ref": f"lp:{row.id}:{row.resale_instrument}",
        "foreclosure_case_number": row.case_number,
        "lis_pendens_date": row.lis_pendens_date.isoformat(),
        "sale_instrument": row.resale_instrument,
        "sale_price": float(row.resale_price) if row.resale_price is not None else None,
        "sale_date": row.resale_date.isoformat(),
        "days_lp_to_sale": (row.resale_date - row.lis_pendens_date).days,
    }


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    rows = session.execute(
        text(
            "SELECT f.id, f.property_id, f.case_number, f.lis_pendens_date, "
            "r.instrument_number AS resale_instrument, r.record_date AS resale_date, "
            "r.sale_price AS resale_price, r.grantee AS resale_grantee "
            "FROM foreclosures f " + _RESALE_JOIN + " "
            "WHERE f.county_id = :cid AND f.lis_pendens_date IS NOT NULL "
            "AND f.auction_date IS NULL AND f.property_id IS NOT NULL"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read = len(rows)

    for row in rows:
        if row.resale_instrument is None:
            result.skipped += 1
            continue
        try:
            candidate = OutcomeCandidateData(
                property_id=row.property_id,
                county_id=county_id,
                source_type=SOURCE_TYPE,
                source_table=SOURCE_TABLE,
                source_id=row.id,
                event_type=EVENT_TYPE_LP_SOLD_PRE_AUCTION,
                event_date=row.resale_date,
                amount=row.resale_price,
                counterparty=row.resale_grantee,
                raw_status=row.case_number,
                raw_payload=_build_payload(row),
            )
            upsert_outcome_candidate(session, candidate)
            result.staged += 1
        except Exception:
            logger.exception("Failed to stage lis-pendens outcome for foreclosures.id=%s", row.id)
            result.errors += 1

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage lis-pendens pre-auction sale outcomes as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
