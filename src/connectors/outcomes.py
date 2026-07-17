"""
Canonical outcome-candidate shape for the Cora Data Engine's outcome
connectors, and the upsert helper that stages it into outcome_candidates
(src/core/models.py:OutcomeCandidate).

Every connector's source data looks different today — a raw status string on
Foreclosure, a structured sold_to/sold_amount pair on TaxDeedAuction, a
qualified-sale flag on appraiser records. OutcomeCandidate is the one shape
the label layer (CDE-10, src/connectors/label_layer.py) consumes uniformly
to promote rows into DealOutcome.

This module deliberately never writes to deal_outcomes — that is exclusively
the label layer's job.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import case, func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.models import OutcomeCandidate

EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY = "auction_sold_third_party"
EVENT_TYPE_AUCTION_REVERTED_TO_LENDER = "auction_reverted_to_lender"
EVENT_TYPE_AUCTION_CANCELLED = "auction_cancelled"
EVENT_TYPE_TAX_DEED_SOLD = "tax_deed_sold"
EVENT_TYPE_TAX_DEED_CANCELLED = "tax_deed_cancelled"
EVENT_TYPE_TAX_DEED_REDEEMED = "tax_deed_redeemed"
EVENT_TYPE_QUALIFIED_SALE = "qualified_sale"
EVENT_TYPE_UNQUALIFIED_SALE = "unqualified_sale"

# Keep in sync with OutcomeCandidate.check_outcome_candidate_event_type (models.py)
# and scripts/apply_cde09_outcome_candidates_table.py's CHECK constraint.
OUTCOME_EVENT_TYPES = frozenset({
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
    EVENT_TYPE_AUCTION_REVERTED_TO_LENDER,
    EVENT_TYPE_AUCTION_CANCELLED,
    EVENT_TYPE_TAX_DEED_SOLD,
    EVENT_TYPE_TAX_DEED_CANCELLED,
    EVENT_TYPE_TAX_DEED_REDEEMED,
    EVENT_TYPE_QUALIFIED_SALE,
    EVENT_TYPE_UNQUALIFIED_SALE,
})


@dataclass
class OutcomeCandidateData:
    property_id: int                       # never None — connectors only emit for resolved properties
    county_id: str
    source_type: str                       # matches a connector's registry source_type
    source_table: str                      # e.g. 'foreclosures', 'tax_deed_auctions', 'financials'
    source_id: int                         # PK of the row in source_table (traceability)
    event_type: str
    event_date: date
    amount: Optional[Decimal] = None
    counterparty: Optional[str] = None
    raw_status: Optional[str] = None       # untranslated source string, for audit/debugging
    match_confidence: Optional[float] = None   # only set when resolve_or_quarantine() produced this row
    match_method: Optional[str] = None

    def __post_init__(self) -> None:
        if self.event_type not in OUTCOME_EVENT_TYPES:
            raise ValueError(
                f"Invalid event_type {self.event_type!r}; must be one of {sorted(OUTCOME_EVENT_TYPES)}"
            )


def upsert_outcome_candidate(session: Session, candidate: OutcomeCandidateData) -> None:
    """
    Upsert one OutcomeCandidate row, keyed on (source_type, source_table,
    source_id) so re-running a connector against the same source row updates
    it in place instead of duplicating. Mirrors the Core insert +
    on_conflict_do_update idiom already used by quarantine_unmatched and
    record_scraper_stats — not the ORM query API.
    """
    stmt = pg_insert(OutcomeCandidate).values(
        property_id=candidate.property_id,
        county_id=candidate.county_id,
        source_type=candidate.source_type,
        source_table=candidate.source_table,
        source_id=candidate.source_id,
        event_type=candidate.event_type,
        event_date=candidate.event_date,
        amount=candidate.amount,
        counterparty=candidate.counterparty,
        raw_status=candidate.raw_status,
        match_confidence=candidate.match_confidence,
        match_method=candidate.match_method,
        updated_at=func.now(),
    )
    excluded = stmt.excluded
    # Only property_id/event_type/event_date/amount feed the promoted
    # DealOutcome row (label_layer._promote_one) — a re-stage that changes one
    # of these (e.g. a revised winning bid or a corrected terminal status)
    # must clear consumed_at so the label layer re-promotes it. Any other
    # field changing (counterparty/raw_status/match_*, audit-only) leaves
    # consumed_at alone. Unconditionally clearing it on every upsert would
    # make the label layer reprocess every row on every connector re-run
    # forever, defeating the point of consumed_at.
    outcome_changed = or_(
        OutcomeCandidate.property_id.is_distinct_from(excluded.property_id),
        OutcomeCandidate.event_type.is_distinct_from(excluded.event_type),
        OutcomeCandidate.event_date.is_distinct_from(excluded.event_date),
        OutcomeCandidate.amount.is_distinct_from(excluded.amount),
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_outcome_candidate",
        set_=dict(
            property_id=excluded.property_id,
            event_type=excluded.event_type,
            event_date=excluded.event_date,
            amount=excluded.amount,
            counterparty=excluded.counterparty,
            raw_status=excluded.raw_status,
            match_confidence=excluded.match_confidence,
            match_method=excluded.match_method,
            updated_at=excluded.updated_at,
            consumed_at=case(
                (outcome_changed, None),
                else_=OutcomeCandidate.consumed_at,
            ),
        ),
    )
    session.execute(stmt)
