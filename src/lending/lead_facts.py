"""Gather scoring inputs and context-card facts for properties, with provenance.

One query per batch of properties. Sources and their labels:

- Loan amount and lender: PropertyRadar recorded loan (known).
- Maturity: PropertyRadar ``est_maturity_date`` is computed from the loan
  date and term, so it is labelled estimated; nothing here marks a maturity
  known until a verified source exists.
- Entity standing: Sunbiz status on the owner record, known only when the
  owner was matched on Sunbiz.
- Equity: ``financials.equity_pct`` is itself an estimate.
- Repeat operator: properties and recent permits held by the same Sunbiz
  entity (by document number, never by name), known only when the owner has
  a document number.
- Decision maker: unknown until a caller confirms it, so always missing here.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Sequence

from sqlalchemy import text as sa_text

from config.lead_scoring import REPEAT_OPERATOR_PERMIT_LOOKBACK_DAYS
from src.lending.lead_scoring import LeadSignals, Signal

logger = logging.getLogger(__name__)

_MATURITY_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m", "%m/%Y")


@dataclass(frozen=True)
class LeadFacts:
    property_id: int
    property_address: Optional[str]
    lender_name: Optional[str]
    latest_permit: Optional[str]
    signals: LeadSignals


def parse_maturity(raw: Optional[str]) -> Optional[date]:
    """PropertyRadar maturity strings to a date; None when unparseable."""
    if not raw or not raw.strip():
        return None
    for fmt in _MATURITY_FORMATS:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    logger.debug("lead_facts: unparseable maturity format")
    return None


def _decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


_FACTS_SQL = """
WITH radar AS (
    SELECT DISTINCT ON (property_id)
           property_id, lender_name, loan_amount, est_maturity_date
    FROM property_radar_records
    WHERE property_id = ANY(:property_ids) AND status = 'active'
    ORDER BY property_id, last_seen_at DESC
),
subject AS (
    SELECT p.id AS property_id, p.address AS property_address,
           o.entity_status, o.sunbiz_status, o.sunbiz_doc_number,
           f.equity_pct
    FROM properties p
    LEFT JOIN owners o ON o.property_id = p.id
    LEFT JOIN financials f ON f.property_id = p.id
    WHERE p.id = ANY(:property_ids)
),
entity_holdings AS (
    SELECT o.sunbiz_doc_number,
           count(DISTINCT o.property_id) AS property_count,
           count(DISTINCT bp.id) FILTER (
               WHERE bp.issue_date >= :permit_since AND NOT bp.is_enforcement_permit
           ) AS recent_permit_count
    FROM owners o
    LEFT JOIN building_permits bp ON bp.property_id = o.property_id
    WHERE o.sunbiz_doc_number IN (
        SELECT sunbiz_doc_number FROM subject WHERE sunbiz_doc_number IS NOT NULL
    )
    GROUP BY o.sunbiz_doc_number
),
latest_permit AS (
    SELECT DISTINCT ON (property_id)
           property_id,
           concat_ws(', ', permit_type, to_char(issue_date, 'YYYY-MM-DD')) AS summary
    FROM building_permits
    WHERE property_id = ANY(:property_ids) AND NOT is_enforcement_permit
    ORDER BY property_id, issue_date DESC NULLS LAST
)
SELECT s.property_id, s.property_address, s.entity_status, s.sunbiz_status,
       s.sunbiz_doc_number, s.equity_pct,
       r.lender_name, r.loan_amount, r.est_maturity_date,
       h.property_count, h.recent_permit_count,
       lp.summary AS latest_permit
FROM subject s
LEFT JOIN radar r ON r.property_id = s.property_id
LEFT JOIN entity_holdings h ON h.sunbiz_doc_number = s.sunbiz_doc_number
LEFT JOIN latest_permit lp ON lp.property_id = s.property_id
"""


def _signals_from_row(row: Any) -> LeadSignals:
    maturity = parse_maturity(row["est_maturity_date"])
    loan = _decimal(row["loan_amount"])
    equity = _decimal(row["equity_pct"])
    sunbiz_matched = row["sunbiz_status"] == "matched" and row["entity_status"]
    has_entity = row["sunbiz_doc_number"] is not None and row["property_count"] is not None
    return LeadSignals(
        maturity_date=Signal.estimated(maturity) if maturity else Signal.missing(),
        entity_status=Signal.known(row["entity_status"]) if sunbiz_matched else Signal.missing(),
        equity_pct=Signal.estimated(equity) if equity is not None else Signal.missing(),
        decision_maker_confirmed=Signal.missing(),
        loan_amount=Signal.known(loan) if loan is not None else Signal.missing(),
        entity_property_count=Signal.known(int(row["property_count"])) if has_entity else Signal.missing(),
        entity_recent_permit_count=(
            Signal.known(int(row["recent_permit_count"])) if has_entity else Signal.missing()
        ),
    )


def load_lead_facts(session, property_ids: Sequence[int], *, today: date) -> dict[int, LeadFacts]:
    """Facts for each property that exists, keyed by property id."""
    if not property_ids:
        return {}
    rows = session.execute(
        sa_text(_FACTS_SQL),
        {
            "property_ids": list(property_ids),
            "permit_since": today - timedelta(days=REPEAT_OPERATOR_PERMIT_LOOKBACK_DAYS),
        },
    ).mappings().all()
    return {
        row["property_id"]: LeadFacts(
            property_id=row["property_id"],
            property_address=row["property_address"],
            lender_name=row["lender_name"],
            latest_permit=row["latest_permit"] or None,
            signals=_signals_from_row(row),
        )
        for row in rows
    }
