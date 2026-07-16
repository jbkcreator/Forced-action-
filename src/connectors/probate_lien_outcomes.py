"""
Probate + code-enforcement-lien outcome connector.

Resolution signal is a subsequent deed on the same property_id, never
case-status text (ADR 0022) — no window cap, unlike deed_flip: a probate or
lien sale can legitimately lag its filing by years and still be "distress
ended in a sale."

Sources (all already ingested, no new fetch):
  - legal_proceedings, record_type='Probate'         -> probate_sale
  - legal_and_liens, document_type IN ('TCL','CCL')  -> lien_sale
  - code_violations, is_lien=true                    -> lien_sale
    (skipped if the same property also has a TCL/CCL row -- that one wins)

CLI:
    python -m src.connectors.probate_lien_outcomes --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_LIEN_SALE,
    EVENT_TYPE_PROBATE_SALE,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "probate_lien_outcomes"

# ponytail: '%quit%' ILIKE covers quit/quitclaim/quit claim — same exclusion
# CDE-03's classify_deed encodes; converge on importing it if divergence appears.
_RESALE_JOIN = (
    "LEFT JOIN LATERAL ("
    "  SELECT d.instrument_number, d.record_date, d.sale_price"
    "  FROM deeds d"
    "  WHERE d.property_id = s.property_id"
    "    AND d.record_date > s.filing_date"
    "    AND (d.sale_price IS NULL OR d.sale_price >= 100)"
    "    AND (d.deed_type IS NULL OR d.deed_type NOT ILIKE '%quit%')"
    "  ORDER BY d.record_date ASC LIMIT 1"
    ") r ON true"
)


def _build_payload(row, source_table: str, source_ref_prefix: str) -> dict:
    return {
        "source_ref": f"{source_ref_prefix}:{row.source_key}:{row.resale_instrument}",
        "case_number_or_instrument": row.source_key,
        "filing_date": row.filing_date.isoformat(),
        "sale_instrument": row.resale_instrument,
        "sale_price": float(row.resale_price) if row.resale_price is not None else None,
        "sale_date": row.resale_date.isoformat(),
        "days_filing_to_sale": (row.resale_date - row.filing_date).days,
        "source_table": source_table,
    }


def _stage_rows(session: Session, county_id: str, result: ConnectorRunResult,
                rows, source_table: str, event_type: str, source_ref_prefix: str) -> None:
    for row in rows:
        if row.resale_instrument is None:
            result.skipped += 1
            continue
        try:
            candidate = OutcomeCandidateData(
                property_id=row.property_id,
                county_id=county_id,
                source_type=SOURCE_TYPE,
                source_table=source_table,
                source_id=row.id,
                event_type=event_type,
                event_date=row.resale_date,
                amount=row.resale_price,
                counterparty=row.source_key,
                raw_status=row.resale_instrument,
                raw_payload=_build_payload(row, source_table, source_ref_prefix),
            )
            upsert_outcome_candidate(session, candidate)
            result.staged += 1
        except Exception:
            logger.exception("Failed to stage %s outcome for %s.id=%s", event_type, source_table, row.id)
            result.errors += 1


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    probate_rows = session.execute(
        text(
            "SELECT s.id, s.property_id, s.case_number AS source_key, s.filing_date, "
            "r.instrument_number AS resale_instrument, r.record_date AS resale_date, r.sale_price AS resale_price "
            "FROM legal_proceedings s " + _RESALE_JOIN + " "
            "WHERE s.county_id = :cid AND s.record_type = 'Probate' "
            "AND s.property_id IS NOT NULL AND s.filing_date IS NOT NULL"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read += len(probate_rows)
    _stage_rows(session, county_id, result, probate_rows, "legal_proceedings", EVENT_TYPE_PROBATE_SALE, "probate")

    lien_rows = session.execute(
        text(
            "SELECT s.id, s.property_id, s.instrument_number AS source_key, s.filing_date, "
            "r.instrument_number AS resale_instrument, r.record_date AS resale_date, r.sale_price AS resale_price "
            "FROM legal_and_liens s " + _RESALE_JOIN + " "
            # Live vocabulary is verbose — 'TAMPA CODE LIENS (TCL)', 'COUNTY CODE LIENS (CCL)' —
            # so match the (TCL)/(CCL) code anywhere, plus the bare codes for safety.
            "WHERE s.county_id = :cid AND (s.document_type IN ('TCL', 'CCL') "
            "OR s.document_type LIKE '%(TCL)%' OR s.document_type LIKE '%(CCL)%') "
            "AND s.property_id IS NOT NULL AND s.filing_date IS NOT NULL"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read += len(lien_rows)
    lien_property_ids = {row.property_id for row in lien_rows}
    _stage_rows(session, county_id, result, lien_rows, "legal_and_liens", EVENT_TYPE_LIEN_SALE, "lien")

    violation_rows = session.execute(
        text(
            "SELECT s.id, s.property_id, s.record_number AS source_key, s.filing_date, "
            "r.instrument_number AS resale_instrument, r.record_date AS resale_date, r.sale_price AS resale_price "
            "FROM (SELECT id, property_id, record_number, opened_date AS filing_date "
            "      FROM code_violations WHERE county_id = :cid AND is_lien = true "
            "      AND property_id IS NOT NULL AND opened_date IS NOT NULL) s "
            + _RESALE_JOIN
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read += len(violation_rows)
    # Same property already covered by a TCL/CCL row -- that one wins (one lien_sale per property+resale).
    deduped = [row for row in violation_rows if row.property_id not in lien_property_ids]
    result.skipped += len(violation_rows) - len(deduped)
    _stage_rows(session, county_id, result, deduped, "code_violations", EVENT_TYPE_LIEN_SALE, "lien")

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage probate/lien-sale outcomes as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
