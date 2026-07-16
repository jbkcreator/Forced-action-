"""
Deed flip-outcome connector.

Resolution signal is a subsequent deed on the same property, never
case-status text (ADR 0022). A distressed acquisition (certificate of
title / tax deed / sheriff's deed) followed by a later qualifying resale
within 24 months is staged as a `deed_flip` outcome candidate. No deed
within the window -> unresolved -> nothing emitted.

Live vocabulary check against `deeds.deed_type` (shared DB, 2026-07-16):
'(D) DEED', 'DEED', 'Warranty Deed', '(TAXDEED) TAX DEED', 'TAX DEED',
'Tax Deed' — tax-deed variants confirm the keyword list below; no
certificate-of-title/sheriff rows observed yet in this county's data but
the keywords stay for counties/sources that do produce them.

CLI:
    python -m src.connectors.deed_flip_outcomes --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from typing import Literal, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_DEED_FLIP,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "deed_flip_outcomes"
SOURCE_TABLE = "deeds"

DISTRESSED_KEYWORDS = ("certificate of title", "cert of title", "tax deed", "sheriff")
EXCLUDED_RESALE_KEYWORDS = ("quit", "quitclaim", "quit claim")
FLIP_WINDOW_DAYS = 730  # ~24 months; >window resale emits nothing in v1 (no `hold` label yet)


def classify_deed(deed_type: Optional[str]) -> Literal["distressed", "excluded", "normal"]:
    """Case-insensitive substring classification of a deed's free-text type."""
    if not deed_type:
        return "normal"
    normalized = deed_type.strip().lower()
    if any(k in normalized for k in EXCLUDED_RESALE_KEYWORDS):
        return "excluded"
    if any(k in normalized for k in DISTRESSED_KEYWORDS):
        return "distressed"
    return "normal"


def find_flip_pairs(deeds_for_property: list) -> list[tuple]:
    """
    Deeds for ONE property, already sorted by record_date ascending.
    Returns (acquisition_row, resale_row) pairs — one per distressed
    acquisition, paired with its own first qualifying resale.
    """
    pairs = []
    for i, acq in enumerate(deeds_for_property):
        if classify_deed(acq.deed_type) != "distressed":
            continue
        deadline = acq.record_date + timedelta(days=FLIP_WINDOW_DAYS)
        for resale in deeds_for_property[i + 1:]:
            if resale.record_date <= acq.record_date:
                continue
            if resale.record_date > deadline:
                break  # sorted ascending -- nothing further can qualify
            if classify_deed(resale.deed_type) == "excluded":
                continue
            pairs.append((acq, resale))
            break
    return pairs


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    rows = session.execute(
        text(
            "SELECT id, property_id, record_date, sale_price, deed_type, "
            "instrument_number, grantee "
            "FROM deeds "
            "WHERE county_id = :cid AND property_id IS NOT NULL "
            "AND record_date IS NOT NULL AND sale_price >= 100 "
            "ORDER BY property_id, record_date ASC"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read = len(rows)

    by_property: dict[int, list] = {}
    for row in rows:
        by_property.setdefault(row.property_id, []).append(row)

    for property_id, deeds_for_property in by_property.items():
        distressed_count = sum(1 for d in deeds_for_property if classify_deed(d.deed_type) == "distressed")
        pairs = find_flip_pairs(deeds_for_property)
        result.skipped += distressed_count - len(pairs)

        for acq, resale in pairs:
            try:
                event_date = resale.record_date.date() if hasattr(resale.record_date, "date") else resale.record_date
                candidate = OutcomeCandidateData(
                    property_id=property_id,
                    county_id=county_id,
                    source_type=SOURCE_TYPE,
                    source_table=SOURCE_TABLE,
                    source_id=acq.id,
                    event_type=EVENT_TYPE_DEED_FLIP,
                    event_date=event_date,
                    amount=resale.sale_price,
                    counterparty=resale.grantee,
                    raw_status=acq.deed_type,
                )
                upsert_outcome_candidate(session, candidate)
                result.staged += 1
            except Exception:
                logger.exception(
                    "Failed to stage deed flip outcome for deeds.id=%s (resale=%s)", acq.id, resale.id,
                )
                result.errors += 1

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage deed flip outcomes as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
