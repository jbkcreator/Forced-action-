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
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import groupby
from typing import Iterator, Literal, NamedTuple, Optional

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

STREAM_BATCH = 1000  # yield_per size — deeds is a tens-of-thousands+ table, never fetchall it


class ClassifiedDeed(NamedTuple):
    """One deed row plus its deed_type classification, computed once."""
    id: int
    property_id: int
    record_date: date
    sale_price: Decimal
    deed_type: Optional[str]
    instrument_number: str
    grantee: Optional[str]
    kind: Literal["distressed", "excluded", "normal"]


class FlipPair(NamedTuple):
    acquisition: ClassifiedDeed
    resale: ClassifiedDeed


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


def _to_date(value) -> date:
    return value.date() if isinstance(value, datetime) else value


def find_flip_pairs(deeds_for_property: list[ClassifiedDeed]) -> list[FlipPair]:
    """
    Deeds for ONE property, already sorted by record_date ascending, each
    pre-classified. Returns one pair per distressed acquisition, paired with
    its own first qualifying resale within the flip window.
    """
    pairs: list[FlipPair] = []
    for i, acq in enumerate(deeds_for_property):
        if acq.kind != "distressed":
            continue
        deadline = acq.record_date + timedelta(days=FLIP_WINDOW_DAYS)
        for resale in deeds_for_property[i + 1:]:
            if resale.record_date <= acq.record_date:
                continue
            if resale.record_date > deadline:
                break  # sorted ascending -- nothing further can qualify
            if resale.kind == "excluded":
                continue
            pairs.append(FlipPair(acq, resale))
            break
    return pairs


def _build_payload(pair: FlipPair) -> dict:
    acq, resale = pair
    purchase_price = Decimal(acq.sale_price)
    resale_price = Decimal(resale.sale_price)
    margin = resale_price - purchase_price
    margin_pct = float(round(margin / purchase_price * 100, 2)) if purchase_price else None
    return {
        "source_ref": f"flip:{acq.property_id}:{resale.instrument_number}",
        "acquisition_instrument": acq.instrument_number,
        "resale_instrument": resale.instrument_number,
        "purchase_price": float(purchase_price),
        "resale_price": float(resale_price),
        "hold_days": (_to_date(resale.record_date) - _to_date(acq.record_date)).days,
        "margin": float(margin),
        "margin_pct": margin_pct,
        "deed_type_raw": acq.deed_type,
    }


def _stream_property_groups(session: Session, county_id: str) -> Iterator[list[ClassifiedDeed]]:
    """Stream deeds ordered by property_id, yielding one pre-classified group per property."""
    result = session.execute(
        text(
            "SELECT id, property_id, record_date, sale_price, deed_type, "
            "instrument_number, grantee "
            "FROM deeds "
            "WHERE county_id = :cid AND property_id IS NOT NULL "
            "AND record_date IS NOT NULL AND sale_price >= 100 "
            "ORDER BY property_id, record_date ASC"
        ),
        {"cid": county_id},
    ).yield_per(STREAM_BATCH)

    for _, rows in groupby(result, key=lambda r: r.property_id):
        yield [
            ClassifiedDeed(
                id=r.id, property_id=r.property_id, record_date=r.record_date,
                sale_price=r.sale_price, deed_type=r.deed_type,
                instrument_number=r.instrument_number, grantee=r.grantee,
                kind=classify_deed(r.deed_type),
            )
            for r in rows
        ]


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    for group in _stream_property_groups(session, county_id):
        result.total_read += len(group)
        distressed_count = sum(1 for d in group if d.kind == "distressed")
        pairs = find_flip_pairs(group)
        result.skipped += distressed_count - len(pairs)   # distressed acquisitions with no qualifying resale

        for pair in pairs:
            acq, resale = pair
            try:
                candidate = OutcomeCandidateData(
                    property_id=acq.property_id,
                    county_id=county_id,
                    source_type=SOURCE_TYPE,
                    source_table=SOURCE_TABLE,
                    source_id=acq.id,
                    event_type=EVENT_TYPE_DEED_FLIP,
                    event_date=_to_date(resale.record_date),
                    amount=resale.sale_price,
                    counterparty=resale.grantee,
                    raw_status=acq.deed_type,
                    raw_payload=_build_payload(pair),
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
