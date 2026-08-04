"""
Hunter — portfolio cadence, capacity, financing pattern, and hold-time
profiling (HUNTER-04).

Generalizes src.connectors.deed_flip_outcomes's acquisition->resale pairing
beyond its distressed-acquisition anchor: this runs over an entity's FULL
purchase history (every deed linked to it as buyer via buyer_entity_links),
not just deeds that classify as a distressed acquisition. A normal investor
who only ever buys non-distressed properties still gets profiled here --
deliberately, since HUNTER-03's buyer-type classifier (buyer_type_classification.py)
reads this module's persisted portfolio_evidence rather than re-deriving its
own pairing, and would otherwise never see that investor at all.

Same "next deed on the property is the resolution signal, not case-status
text" principle as deed_flip_outcomes.py (ADR 0022) -- an exit is the next
QUALIFYING deed recorded on the property after an entity's acquisition,
regardless of who the grantor/grantee on that later deed is (deeds are only
ever linked to an entity as grantee, never grantor -- see
buyer_entity_resolution.extract_deed_candidates).

"Qualifying" here is wider than deed_flip_outcomes.EXCLUDED_RESALE_KEYWORDS
(quitclaim only): deeds.deed_type is free text shared with non-transfer
instruments recorded through the same pipeline (Deed.mortgage_amount's own
docstring -- "populated from Filing Amt column on mortgage-type docs" --
confirms mortgage/deed-of-trust rows live in this same table). Un-gated by a
distressed-acquisition anchor, this module's pairing is far more exposed to
walking into one of those rows between an acquisition and its real exit, so
NON_TRANSFER_DEED_TYPE_KEYWORDS below is wider than deed_flip_outcomes.py's
own list. That module doesn't need the wider list as urgently for its
narrower use case -- worth a note to whoever owns it next, not a silent
divergence.

Financing is a 3-state signal (financed | cash_inferred | unknown), computed
per acquisition then majority-voted onto the entity -- never inferred from a
bare NULL. 'cash_inferred' means "no correlated mortgage-type deed found for
this acquisition" -- a public-record-inferred signal (same epistemic tier as
DealOutcome.confidence_tier='public_record_inferred' elsewhere in this
codebase), not a legal certainty that the purchase was cash. 'unknown' is
reserved for genuinely indeterminate cases (nothing else recorded for that
property to corroborate against) and never boosts
estimated_annual_acquisition_capacity's multiplier the way 'cash_inferred' does.

Usage:
    from src.agents.hunter.portfolio_profiling import refresh_portfolio_profiling
    refresh_portfolio_profiling(session)                       # full-table (backfill only)
    refresh_portfolio_profiling(session, entity_ids=[1, 2])     # incremental
"""
from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import groupby
from typing import Iterator, NamedTuple, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

STREAM_BATCH = 1000  # yield_per size -- deeds is a tens-of-thousands+ table, never fetchall it

# Same $1/$10/$0 family/trust/corrective-re-recording floor already used by
# src.services.whale_detection.NOMINAL_CONSIDERATION_FLOOR.
NOMINAL_CONSIDERATION_FLOOR = 1000

# See module docstring -- wider than deed_flip_outcomes.EXCLUDED_RESALE_KEYWORDS
# deliberately, for this module's un-gated, full-history pairing.
NON_TRANSFER_DEED_TYPE_KEYWORDS = (
    "quit", "quitclaim", "quit claim",
    "mortgage", "deed of trust", "satisfaction", "release",
    "lien", "assignment", "lis pendens", "notice", "affidavit", "ucc",
)
FINANCING_DEED_TYPE_KEYWORDS = ("mortgage", "deed of trust")
FINANCING_CORRELATION_WINDOW_DAYS = 30

FLIP_MAX_HOLD_DAYS = 730   # reused naming/value from deed_flip_outcomes.FLIP_WINDOW_DAYS
WHOLESALE_MAX_HOLD_DAYS = 7

DEFAULT_CADENCE_WINDOW_DAYS = 365

CASH_MULTIPLIER = 1.5
BASELINE_MULTIPLIER = 1.0


def _to_date(value) -> date:
    return value.date() if isinstance(value, datetime) else value


def _is_transfer_deed(deed_type: Optional[str]) -> bool:
    """No type recorded -> can't positively exclude it, treat as a transfer --
    same convention deed_flip_outcomes.classify_deed uses for its 'normal' bucket."""
    if not deed_type:
        return True
    normalized = deed_type.strip().lower()
    return not any(k in normalized for k in NON_TRANSFER_DEED_TYPE_KEYWORDS)


def _is_financing_deed(deed_type: Optional[str]) -> bool:
    if not deed_type:
        return False
    normalized = deed_type.strip().lower()
    return any(k in normalized for k in FINANCING_DEED_TYPE_KEYWORDS)


def _qualifies_as_transfer(deed_type: Optional[str], sale_price: Optional[Decimal]) -> bool:
    if not _is_transfer_deed(deed_type):
        return False
    if sale_price is not None and sale_price < NOMINAL_CONSIDERATION_FLOOR:
        return False
    return True


class DeedRow(NamedTuple):
    id: int
    property_id: int
    record_date: date
    sale_price: Optional[Decimal]
    deed_type: Optional[str]
    mortgage_amount: Optional[Decimal]
    buyer_entity_id: Optional[int]


class AcquisitionEvidence(NamedTuple):
    """One entity's acquisition of one property -- hold_days=None means still held."""
    property_id: int
    acquisition_date: date
    hold_days: Optional[int]
    financing_state: str  # 'financed' | 'cash_inferred' | 'unknown'


def _stream_property_groups(
    session: Session, property_ids: Optional[list[int]],
) -> Iterator[list[DeedRow]]:
    """
    Stream ALL deeds for the given properties (or every property, if
    property_ids is None -- the full-backfill case), ordered so each
    property's history arrives as one contiguous, chronologically-sorted
    group. Not scoped to any one buyer_entity_id or county -- a buyer isn't
    bound to one county (see hunter_nightly_sweep.py's own note on this), and
    an exit's identity as grantor/grantee is irrelevant to whether it
    resolves an acquisition (ADR 0022).
    """
    where_clause = "WHERE d.property_id = ANY(:property_ids)" if property_ids is not None else "WHERE d.property_id IS NOT NULL"
    result = session.execute(
        text(f"""
            SELECT d.id, d.property_id, d.record_date, d.sale_price, d.deed_type, d.mortgage_amount,
                   bel.buyer_entity_id
            FROM deeds d
            LEFT JOIN buyer_entity_links bel
                ON bel.source_table = 'deeds' AND bel.source_id = d.id
            {where_clause} AND d.record_date IS NOT NULL
            ORDER BY d.property_id, d.record_date ASC, d.id ASC
        """),
        {"property_ids": property_ids} if property_ids is not None else {},
    ).yield_per(STREAM_BATCH)

    for _, rows in groupby(result, key=lambda r: r.property_id):
        yield [
            DeedRow(
                id=r.id, property_id=r.property_id, record_date=_to_date(r.record_date),
                sale_price=r.sale_price, deed_type=r.deed_type, mortgage_amount=r.mortgage_amount,
                buyer_entity_id=r.buyer_entity_id,
            )
            for r in rows
        ]


def _find_exit(acq_index: int, group: list[DeedRow]) -> tuple[Optional[DeedRow], set[int]]:
    """First later deed on the property that qualifies as a real transfer --
    skips over (never stops at) mortgages/liens/corrective rows in between,
    matching deed_flip_outcomes.find_flip_pairs's `continue`-not-`break` style
    for excluded rows.

    A later qualifying deed still linked to the SAME buyer_entity_id as the
    acquisition is not a resale -- deeds carry no stable transaction identity
    (a corrective/re-recorded deed gets its own instrument_number, same as a
    real transfer), so a repeat of the acquiring entity here is the
    signature of a corrective re-recording of THIS SAME acquisition, not the
    entity selling to itself. These rows are returned as `duplicate_ids` so
    the caller can skip them entirely rather than minting a second, bogus
    acquisition event for the same purchase (see
    buyer_entity_resolution.refresh_portfolio_aggregates's docstring for the
    same corrective-re-recording failure mode, handled there via a DISTINCT
    ON canonical-row pick instead, since that function's shape doesn't need
    exit/hold-time evidence)."""
    acq = group[acq_index]
    duplicate_ids: set[int] = set()
    for later in group[acq_index + 1:]:
        if later.record_date <= acq.record_date:
            continue
        if not _qualifies_as_transfer(later.deed_type, later.sale_price):
            continue
        if later.buyer_entity_id is not None and later.buyer_entity_id == acq.buyer_entity_id:
            duplicate_ids.add(later.id)
            continue
        return later, duplicate_ids
    return None, duplicate_ids


def _financing_state(acq: DeedRow, group: list[DeedRow]) -> str:
    """financed (positive evidence) / cash_inferred (checked, found nothing) /
    unknown (nothing else recorded for this property to check against) --
    never a guess from a bare NULL. See module docstring."""
    if acq.mortgage_amount is not None:
        return "financed"
    if len(group) == 1:
        return "unknown"
    window_start = acq.record_date - timedelta(days=FINANCING_CORRELATION_WINDOW_DAYS)
    window_end = acq.record_date + timedelta(days=FINANCING_CORRELATION_WINDOW_DAYS)
    for other in group:
        if other.id == acq.id:
            continue
        if _is_financing_deed(other.deed_type) and window_start <= other.record_date <= window_end:
            return "financed"
    return "cash_inferred"


def compute_acquisition_evidence(
    session: Session, entity_ids: Optional[list[int]] = None,
) -> dict[int, list[AcquisitionEvidence]]:
    """
    One pass over the relevant deed history, returning every acquisition
    event (with its exit/still-held outcome and financing state) grouped by
    buyer_entity_id. Shared evidence source for both refresh_portfolio_profiling
    (H4) and buyer_type_classification.classify_buyer_types (H3) -- H3 never
    re-queries deeds itself, it reads the portfolio_evidence this function's
    caller persists.

    entity_ids=None scans every property (full backfill). entity_ids=[] is
    explicitly a no-op (not "everything") -- a caller passing an empty list
    means nothing changed, never "process the whole table."
    """
    if entity_ids is not None and len(entity_ids) == 0:
        return {}

    property_ids: Optional[list[int]] = None
    if entity_ids is not None:
        rows = session.execute(
            text("""
                SELECT DISTINCT d.property_id
                FROM buyer_entity_links bel
                JOIN deeds d ON d.id = bel.source_id AND bel.source_table = 'deeds'
                WHERE bel.buyer_entity_id = ANY(:entity_ids)
            """),
            {"entity_ids": entity_ids},
        ).fetchall()
        property_ids = [r.property_id for r in rows]
        if not property_ids:
            return {}

    entity_filter = set(entity_ids) if entity_ids is not None else None
    evidence_by_entity: dict[int, list[AcquisitionEvidence]] = defaultdict(list)

    for group in _stream_property_groups(session, property_ids):
        # Corrective/re-recorded deeds for an already-open acquisition are
        # collected here (by _find_exit, as it scans forward for that
        # acquisition's real exit) and must not be visited as their own
        # acquisition when the outer loop reaches them -- see _find_exit's
        # docstring.
        duplicate_ids: set[int] = set()
        for i, row in enumerate(group):
            if row.id in duplicate_ids:
                continue
            if row.buyer_entity_id is None:
                continue
            if entity_filter is not None and row.buyer_entity_id not in entity_filter:
                continue
            if not _qualifies_as_transfer(row.deed_type, row.sale_price):
                continue
            exit_row, row_duplicate_ids = _find_exit(i, group)
            duplicate_ids |= row_duplicate_ids
            hold_days = (exit_row.record_date - row.record_date).days if exit_row else None
            evidence_by_entity[row.buyer_entity_id].append(AcquisitionEvidence(
                property_id=row.property_id,
                acquisition_date=row.record_date,
                hold_days=hold_days,
                financing_state=_financing_state(row, group),
            ))

    return evidence_by_entity


def _aggregate_entity_evidence(records: list[AcquisitionEvidence], window_days: int) -> dict:
    today = date.today()
    exit_hold_days = [r.hold_days for r in records if r.hold_days is not None]
    still_held = [r for r in records if r.hold_days is None]

    still_held_past_730 = sum(
        1 for r in still_held if (today - r.acquisition_date).days > FLIP_MAX_HOLD_DAYS
    )

    financing_counts = Counter(r.financing_state for r in records)
    top_count = max(financing_counts.values())
    leaders = [k for k, v in financing_counts.items() if v == top_count]
    # tie broken toward 'unknown' -- never assert cash/financed on a tie
    financing_signal = leaders[0] if len(leaders) == 1 else "unknown"

    avg_hold_days = round(sum(exit_hold_days) / len(exit_hold_days)) if exit_hold_days else None

    cutoff = today - timedelta(days=window_days)
    recent_count = sum(1 for r in records if r.acquisition_date >= cutoff)
    cadence = round(recent_count * (365.0 / window_days), 2)

    multiplier = CASH_MULTIPLIER if financing_signal == "cash_inferred" else BASELINE_MULTIPLIER
    capacity = round(cadence * multiplier)

    evidence = {
        "acquisition_count": len(records),
        "exit_count": len(exit_hold_days),
        "still_held_count": len(still_held),
        "exit_within_730_days": sum(1 for h in exit_hold_days if h <= FLIP_MAX_HOLD_DAYS),
        "exit_within_7_days": sum(1 for h in exit_hold_days if h <= WHOLESALE_MAX_HOLD_DAYS),
        "still_held_past_730_days": still_held_past_730,
        "financed_count": financing_counts.get("financed", 0),
        "cash_inferred_count": financing_counts.get("cash_inferred", 0),
        "unknown_count": financing_counts.get("unknown", 0),
        "window_days": window_days,
    }
    return {
        "cadence_purchases_per_year": cadence,
        "estimated_annual_acquisition_capacity": capacity,
        "financing_signal": financing_signal,
        "avg_hold_days": avg_hold_days,
        "portfolio_evidence": evidence,
    }


def refresh_portfolio_profiling(
    session: Session,
    entity_ids: Optional[list[int]] = None,
    window_days: int = DEFAULT_CADENCE_WINDOW_DAYS,
) -> int:
    """
    Recompute cadence/capacity/financing/hold-time for entity_ids (or every
    entity with acquisition history, if None -- the one-time backfill case;
    the nightly sweep always passes a non-empty entity_ids list). One batched
    UPDATE (executemany-style, a list of per-entity param dicts against one
    parameterized statement) -- never a per-entity round trip, matching
    src.services.whale_detection._mint_opportunity_ids's own batching
    convention for the same reason.
    """
    evidence_by_entity = compute_acquisition_evidence(session, entity_ids=entity_ids)
    if not evidence_by_entity:
        logger.info("refresh_portfolio_profiling: no acquisition evidence in scope -- nothing to do.")
        return 0

    updates = []
    for entity_id, records in evidence_by_entity.items():
        result = _aggregate_entity_evidence(records, window_days=window_days)
        updates.append({
            "entity_id": entity_id,
            "cadence_purchases_per_year": result["cadence_purchases_per_year"],
            "estimated_annual_acquisition_capacity": result["estimated_annual_acquisition_capacity"],
            "financing_signal": result["financing_signal"],
            "avg_hold_days": result["avg_hold_days"],
            "portfolio_evidence": json.dumps(result["portfolio_evidence"]),
        })

    session.execute(
        text("""
            UPDATE buyer_entities
            SET cadence_purchases_per_year = :cadence_purchases_per_year,
                estimated_annual_acquisition_capacity = :estimated_annual_acquisition_capacity,
                financing_signal = :financing_signal,
                avg_hold_days = :avg_hold_days,
                portfolio_evidence = CAST(:portfolio_evidence AS JSONB),
                portfolio_profiled_at = now()
            WHERE id = :entity_id
        """),
        updates,
    )
    session.commit()
    logger.info("refresh_portfolio_profiling: profiled %d entit(y/ies).", len(updates))
    return len(updates)
