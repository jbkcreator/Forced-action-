"""
Foreclosure auction-outcome connector.

`Foreclosure.sold_to` is a clean, confirmed 3-value signal captured directly
off RealForeclose's `.ASTAT_MSG_SOLDTO_MSG` element (live-site recon, 5 real
auction dates, no other values seen): "3rd Party Bidder" (genuine sale),
"Plaintiff" (reverted to lender via credit-bid), or empty (not yet resolved).
Unlike tax-deed's status field, no timestamp-vs-status-word ambiguity here —
sold_to alone is sufficient to classify a resolved auction. `case_status`
(the existing free-text field, e.g. "Canceled per County") is only consulted
as a fallback for auctions that never reached a sold_to value.

CLI:
    python -m src.connectors.foreclosure_outcomes --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_AUCTION_CANCELLED,
    EVENT_TYPE_AUCTION_REVERTED_TO_LENDER,
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "foreclosure_outcomes"

SOLD_TO_THIRD_PARTY = "3rd Party Bidder"
SOLD_TO_PLAINTIFF = "Plaintiff"


def classify(sold_to: Optional[str], case_status: Optional[str]) -> Optional[str]:
    """Return an EVENT_TYPE_AUCTION_* value, or None if unclassifiable."""
    if sold_to == SOLD_TO_THIRD_PARTY:
        return EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY
    if sold_to == SOLD_TO_PLAINTIFF:
        return EVENT_TYPE_AUCTION_REVERTED_TO_LENDER

    if not case_status:
        return None
    normalized = case_status.strip().lower()
    if "cancel" in normalized:
        return EVENT_TYPE_AUCTION_CANCELLED
    # "redeem" and anything else (Waiting, Pending, Auction Starts ...) has no
    # matching auction_* event type registered -- left unclassified, not guessed.
    return None


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    rows = session.execute(
        text(
            "SELECT id, property_id, auction_date, case_status, winning_bid, sold_to "
            "FROM foreclosures WHERE county_id = :cid"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read = len(rows)

    for row in rows:
        if row.auction_date is None:
            result.skipped += 1
            continue

        event_type = classify(row.sold_to, row.case_status)
        if event_type is None:
            result.skipped += 1
            continue

        try:
            candidate = OutcomeCandidateData(
                property_id=row.property_id,
                county_id=county_id,
                source_type=SOURCE_TYPE,
                source_table="foreclosures",
                source_id=row.id,
                event_type=event_type,
                event_date=row.auction_date.date() if hasattr(row.auction_date, "date") else row.auction_date,
                amount=row.winning_bid,
                counterparty=row.sold_to,
                raw_status=row.case_status,
            )
            upsert_outcome_candidate(session, candidate)
            result.staged += 1
        except Exception:
            logger.exception("Failed to stage foreclosure outcome for foreclosures.id=%s", row.id)
            result.errors += 1

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage foreclosure auction outcomes as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
