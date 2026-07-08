"""
Tax-deed auction-outcome connector.

`TaxDeedAuction.status` is an unmapped free-text passthrough scraped verbatim
from a DOM element (`.ASTAT_MSGB`) that realtaxdeed.com reuses for both
terminal outcomes and non-terminal messages — there is no fixed vocabulary
to map from. Verified against live data: the one genuinely SOLD case in the
current dataset has `status = '01/21/2026 11:26 AM ET'` (a resolution
timestamp, not a word like "Sold"), while `sold_amount`/`sold_to` were
correctly populated ($120,100.00 / "3rd Party Bidder"). A naive
keyword-match on `status` alone would have skipped the single most valuable
outcome in the table.

Classification therefore treats `sold_amount` + `sold_to` as the PRIMARY
signal (a real winning bid and buyer means the auction genuinely resolved to
a sale, regardless of what the status text says) and only falls back to
keyword-matching `status` for the non-sale terminal states (redeemed,
cancelled) that have no monetary signal to key off. Anything that matches
neither is left unclassified and skipped — not guessed at — so the
vocabulary can be safely expanded later as new status strings are observed.

CLI:
    python -m src.connectors.tax_deed_outcomes --county-id pinellas
"""
from __future__ import annotations

import argparse
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_TAX_DEED_CANCELLED,
    EVENT_TYPE_TAX_DEED_REDEEMED,
    EVENT_TYPE_TAX_DEED_SOLD,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "tax_deed_outcomes"


def classify_status(status: Optional[str], sold_amount, sold_to: Optional[str]) -> Optional[str]:
    """Return an EVENT_TYPE_TAX_DEED_* value, or None if unclassifiable."""
    if sold_amount is not None and sold_to:
        return EVENT_TYPE_TAX_DEED_SOLD

    if not status:
        return None
    normalized = status.strip().lower()
    if "redeem" in normalized:
        return EVENT_TYPE_TAX_DEED_REDEEMED
    if "cancel" in normalized:
        return EVENT_TYPE_TAX_DEED_CANCELLED
    return None


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    rows = session.execute(
        text(
            "SELECT id, property_id, auction_date, status, sold_amount, sold_to "
            "FROM tax_deed_auctions WHERE county_id = :cid"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read = len(rows)

    for row in rows:
        if row.property_id is None:
            result.skipped += 1
            continue

        event_type = classify_status(row.status, row.sold_amount, row.sold_to)
        if event_type is None:
            logger.info(
                "Unclassifiable tax-deed status, skipping: case id=%s status=%r", row.id, row.status,
            )
            result.skipped += 1
            continue

        try:
            candidate = OutcomeCandidateData(
                property_id=row.property_id,
                county_id=county_id,
                source_type=SOURCE_TYPE,
                source_table="tax_deed_auctions",
                source_id=row.id,
                event_type=event_type,
                event_date=row.auction_date,
                amount=row.sold_amount,
                counterparty=row.sold_to,
                raw_status=row.status,
            )
            upsert_outcome_candidate(session, candidate)
            result.staged += 1
        except Exception:
            logger.exception("Failed to stage tax-deed outcome for tax_deed_auctions.id=%s", row.id)
            result.errors += 1

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage tax-deed auction outcomes as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
