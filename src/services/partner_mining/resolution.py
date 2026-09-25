"""
Stage B — Counterparty identity resolution (SPEC Stage B / GRILL Q4).

extract_counterparty_candidates  — queries mortgage deed rows and emits
    CandidateRecords with source_table='deed_lender'. These feed through the
    shared cluster_against_anchors pipeline (same buyer_entities graph WP-T2-8
    uses) so a lender that also appears as an owner or deed grantee collapses
    to one entity.

extract_wholesaler_candidates  — queries deed rows for quick-reconveyance pairs
    (same party appears as grantee then grantor on the same parcel within 120 days)
    and emits CandidateRecords with source_table='deed_wholesaler'. Mirrors
    extract_counterparty_candidates so run_wholesaler_resolution can create
    buyer_entity_links that resolve_counterparty_names can look up.

resolve_counterparty_names  — lightweight lookup: given raw name strings that
    have already been through extract+resolve, returns {name: buyer_entity_id}
    so the sweep can patch PartnerRow.buyer_entity_id without re-running the
    full clustering pipeline.

run_counterparty_resolution  — end-to-end lender: extract → cluster → attach.
run_wholesaler_resolution     — end-to-end wholesaler: extract → cluster → attach.
    Both mirror run_incremental in buyer_entity_resolution.py.

BuyerEntityLink.source_table values added here:
  'deed_lender'     — grantee on a mortgage/deed-of-trust row
  'deed_wholesaler' — intermediate party on a quick re-conveyance pair
Both are added to the CHECK constraint via apply_partner_counterparty_links.py.
"""

from __future__ import annotations

import logging
from typing import Iterator, Optional, TYPE_CHECKING

from sqlalchemy import text

from src.loaders.base import BaseLoader
from src.services.buyer_entity_resolution import (
    CandidateRecord,
    cluster_against_anchors,
    attach_or_create_entities,
    load_existing_entity_candidates,
    record_ambiguous_pair_exceptions,
    _STREAM_BATCH,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_LENDER_ENTITY_TYPE_HINT = "Corporate"   # lenders are institutional by definition

_LENDER_QUERY = """
    SELECT d.id, d.grantee, d.county_id, d.mortgage_amount
    FROM deeds d
    LEFT JOIN buyer_entity_links bel
        ON bel.source_table = 'deed_lender' AND bel.source_id = d.id
    WHERE d.mortgage_amount IS NOT NULL
      AND d.grantee IS NOT NULL AND d.grantee != ''
      {county_filter}
      {unresolved_filter}
    ORDER BY d.id
"""


def extract_counterparty_candidates(
    session: "Session",
    county_id: Optional[str] = None,
    only_unresolved: bool = True,
) -> Iterator[CandidateRecord]:
    """
    Stream mortgage deed rows as CandidateRecords (source_table='deed_lender').
    The grantee on a mortgage row IS the mortgagee (lender) — the deed-data
    finding confirmed in GRILL Q2a.

    only_unresolved=True (default) restricts to rows not yet linked — the
    incremental mode. False = full backfill.
    """
    county_filter = "AND d.county_id = :county_id" if county_id else ""
    unresolved_filter = "AND bel.id IS NULL" if only_unresolved else ""
    sql = _LENDER_QUERY.format(
        county_filter=county_filter, unresolved_filter=unresolved_filter
    )
    params: dict = {}
    if county_id:
        params["county_id"] = county_id

    result = session.execute(text(sql), params).yield_per(_STREAM_BATCH)
    for row in result:
        raw_name = (row.grantee or "").strip()
        if not raw_name:
            continue
        yield CandidateRecord(
            source_table="deed_lender",
            source_id=row.id,
            raw_name=raw_name,
            normalized_name=BaseLoader.normalize_owner_name(raw_name),
            mailing_address=None,     # lenders have no mailing address on the deed
            entity_type_hint=_LENDER_ENTITY_TYPE_HINT,
            managing_members=None,
            county_id=row.county_id,
        )


def resolve_counterparty_names(
    session: "Session",
    names: list[str],
    *,
    source_table: str = "deed_lender",
) -> dict[str, Optional[int]]:
    """
    Map raw counterparty name strings → buyer_entity_id.

    Looks up existing buyer_entity_links for the given source_table and joins
    to buyer_entities.canonical_name. Returns {name: entity_id or None}.
    Call after the relevant counterparty resolution has committed the links.

    For names not yet resolved (no link), returns None — the sweep will flag
    those rows as needs-enrichment.
    """
    if not names:
        return {}

    result = session.execute(
        text("""
            SELECT be.canonical_name, be.id AS buyer_entity_id
            FROM buyer_entities be
            JOIN buyer_entity_links bel ON bel.buyer_entity_id = be.id
            WHERE bel.source_table = :source_table
              AND be.canonical_name = ANY(:names)
        """),
        {"source_table": source_table, "names": names},
    ).fetchall()

    mapping: dict[str, Optional[int]] = {n: None for n in names}
    for row in result:
        mapping[row.canonical_name] = row.buyer_entity_id
    return mapping


_WHOLESALER_QUERY = """
    SELECT buy.id AS deed_id, buy.grantee, buy.county_id
    FROM deeds buy
    LEFT JOIN buyer_entity_links bel
        ON bel.source_table = 'deed_wholesaler' AND bel.source_id = buy.id
    WHERE buy.grantee IS NOT NULL AND buy.grantee != ''
      AND EXISTS (
          SELECT 1 FROM deeds sell
          WHERE sell.property_id = buy.property_id
            AND sell.grantor ILIKE buy.grantee
            AND sell.record_date > buy.record_date
            AND sell.record_date <= buy.record_date + INTERVAL '{window_days} days'
      )
      {county_filter}
      {unresolved_filter}
    ORDER BY buy.id
"""

_WHOLESALER_WINDOW_DAYS = 120
_WHOLESALER_ENTITY_TYPE_HINT = "Corporate"


def extract_wholesaler_candidates(
    session: "Session",
    county_id: Optional[str] = None,
    only_unresolved: bool = True,
) -> Iterator[CandidateRecord]:
    """
    Stream quick-reconveyance deed rows as CandidateRecords (source_table='deed_wholesaler').

    A wholesaler is a party that appears as grantee (buyer) on a deed and then
    as grantor (seller) on a later deed for the same parcel within 120 days.
    We emit the buy-side deed id as source_id so the entity link is traceable.
    """
    county_filter = "AND buy.county_id = :county_id" if county_id else ""
    unresolved_filter = "AND bel.id IS NULL" if only_unresolved else ""
    sql = _WHOLESALER_QUERY.format(
        window_days=_WHOLESALER_WINDOW_DAYS,
        county_filter=county_filter,
        unresolved_filter=unresolved_filter,
    )
    params: dict = {}
    if county_id:
        params["county_id"] = county_id

    result = session.execute(text(sql), params).yield_per(_STREAM_BATCH)
    for row in result:
        raw_name = (row.grantee or "").strip()
        if not raw_name:
            continue
        yield CandidateRecord(
            source_table="deed_wholesaler",
            source_id=row.deed_id,
            raw_name=raw_name,
            normalized_name=BaseLoader.normalize_owner_name(raw_name),
            mailing_address=None,
            entity_type_hint=_WHOLESALER_ENTITY_TYPE_HINT,
            managing_members=None,
            county_id=row.county_id,
        )


def run_wholesaler_resolution(
    session: "Session",
    county_id: Optional[str] = None,
) -> dict:
    """
    End-to-end wholesaler identity resolution for a county.

    Same pipeline as run_counterparty_resolution but for deed_wholesaler links.
    Must be called before the sweep looks up wholesaler names via
    resolve_counterparty_names(..., source_table='deed_wholesaler').
    """
    new_candidates = list(
        extract_wholesaler_candidates(session, county_id=county_id, only_unresolved=True)
    )

    if not new_candidates:
        logger.info("[PartnerMining] run_wholesaler_resolution: no new wholesaler candidates")
        return {"new_entities": 0, "new_links": 0, "conflicts": 0,
                "processed": 0, "changed_entity_ids": []}

    existing_entities = load_existing_entity_candidates(session, county_id=None)
    combined = new_candidates + existing_entities

    relevant_clusters, confidences, evidence_index, ambiguous_pairs = cluster_against_anchors(combined)
    stats = attach_or_create_entities(session, relevant_clusters, confidences, evidence_index)
    exceptions_recorded = record_ambiguous_pair_exceptions(session, ambiguous_pairs)

    session.commit()

    logger.info(
        "[PartnerMining] wholesaler resolution: %d processed, %d new entities, "
        "%d new links, %d exceptions",
        len(new_candidates), stats.get("new_entities", 0),
        stats.get("new_links", 0), exceptions_recorded,
    )
    return {**stats, "processed": len(new_candidates), "exceptions_recorded": exceptions_recorded}


def run_counterparty_resolution(
    session: "Session",
    county_id: Optional[str] = None,
) -> dict:
    """
    End-to-end counterparty identity resolution for a county.

    Mirrors run_incremental in buyer_entity_resolution.py:
      1. Extract new lender counterparty CandidateRecords.
      2. Load existing entity anchors (all counties — cross-county dedup).
      3. Cluster + attach/create entities.
      4. Record ambiguous pairs in EXCEPTIONS lane.
      5. Commit.

    Returns stats dict matching run_incremental's shape.
    """
    new_candidates = list(
        extract_counterparty_candidates(session, county_id=county_id, only_unresolved=True)
    )

    if not new_candidates:
        logger.info("[PartnerMining] run_counterparty_resolution: no new lender candidates")
        return {"new_entities": 0, "new_links": 0, "conflicts": 0,
                "processed": 0, "changed_entity_ids": []}

    # Anchors cross-county — same rule as run_incremental (GRILL Q4).
    existing_entities = load_existing_entity_candidates(session, county_id=None)
    combined = new_candidates + existing_entities

    relevant_clusters, confidences, evidence_index, ambiguous_pairs = cluster_against_anchors(combined)
    stats = attach_or_create_entities(session, relevant_clusters, confidences, evidence_index)
    exceptions_recorded = record_ambiguous_pair_exceptions(session, ambiguous_pairs)

    session.commit()

    logger.info(
        "[PartnerMining] counterparty resolution: %d processed, %d new entities, "
        "%d new links, %d exceptions",
        len(new_candidates), stats.get("new_entities", 0),
        stats.get("new_links", 0), exceptions_recorded,
    )
    return {**stats, "processed": len(new_candidates), "exceptions_recorded": exceptions_recorded}
