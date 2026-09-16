"""
Backfill borrower_ledger_events from existing property and buyer-entity data.

Reads from: deeds, foreclosures, building_permits, legal_and_liens,
            legal_proceedings, tax_delinquencies, opportunity_scores.

Resolves each source record to a buyer_entity_id via buyer_entity_links
(source_table='deeds' for deed acquisitions; source_table='owners' joined
via property_id for all property-linked tables).

Idempotent: ON CONFLICT (source_table, source_id) DO NOTHING means re-running
produces the same result. Run dry-run first, then --apply against prod.

Each source query streams via yield_per(_COMMIT_BATCH) (server-side cursor)
rather than materializing the full result set -- these source tables can run
into the hundreds of thousands of rows.

Usage:
    PYTHONPATH=. python scripts/backfill_borrower_ledger.py            # dry-run
    PYTHONPATH=. python scripts/backfill_borrower_ledger.py --apply    # write to DB
    PYTHONPATH=. python scripts/backfill_borrower_ledger.py --source deeds --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from config.settings import get_settings
from src.services.borrower_ledger import record_event

logger = logging.getLogger(__name__)

_COMMIT_BATCH = 500  # commit every N inserts to avoid long-held transactions


@dataclass
class BackfillStats:
    source: str
    attempted: int = 0
    inserted: int = 0
    skipped: int = 0
    unresolved: int = 0
    errors: int = 0

    def report(self) -> str:
        return (
            f"{self.source}: attempted={self.attempted} inserted={self.inserted} "
            f"skipped={self.skipped} unresolved={self.unresolved} errors={self.errors}"
        )


# --------------------------------------------------------------------------- #
# Source processors
# --------------------------------------------------------------------------- #

def _backfill_deed_acquisitions(session: Session, dry_run: bool) -> BackfillStats:
    """
    deed_acquisition — deeds where the grantee (buyer) is a resolved buyer entity.
    buyer_entity_links.source_table='deeds' links deeds directly to buyer_entity_id.
    """
    stats = BackfillStats("deeds/deed_acquisition")
    rows = session.execute(text("""
        SELECT
            d.id,
            bel.buyer_entity_id,
            d.property_id,
            d.record_date   AS event_date,
            d.sale_price    AS amount,
            d.grantee,
            d.deed_type,
            d.instrument_number
        FROM deeds d
        JOIN buyer_entity_links bel
            ON bel.source_table = 'deeds' AND bel.source_id = d.id
        WHERE d.record_date IS NOT NULL
        ORDER BY d.id
    """).execution_options(yield_per=_COMMIT_BATCH)).mappings()

    for r in rows:
        stats.attempted += 1
        summary = f"Acquired {r['instrument_number'] or ''} ({r['deed_type'] or 'deed'})"
        if not dry_run:
            try:
                inserted = record_event(
                    session,
                    buyer_entity_id=r["buyer_entity_id"],
                    event_type="deed_acquisition",
                    event_date=r["event_date"],
                    source_table="deeds",
                    source_id=r["id"],
                    property_id=r["property_id"],
                    summary=summary,
                    amount=r["amount"],
                    meta={"grantee": r["grantee"], "deed_type": r["deed_type"]},
                )
                stats.inserted += inserted
                stats.skipped += not inserted
                if stats.attempted % _COMMIT_BATCH == 0:
                    session.commit()
            except Exception as exc:
                logger.error("deed_acquisition id=%d: %s", r["id"], exc)
                session.rollback()
                stats.errors += 1
        else:
            stats.inserted += 1  # count as would-insert in dry-run

    if not dry_run:
        session.commit()
    return stats


def _backfill_via_owner_link(
    session: Session,
    dry_run: bool,
    *,
    source_table: str,
    event_type: str,
    query: str,
) -> BackfillStats:
    """
    Generic backfill for tables linked to properties (foreclosures, permits,
    liens, legal_proceedings, tax_delinquencies). Resolves buyer_entity_id by
    joining property_id → owners → buyer_entity_links (source_table='owners').

    `query` must SELECT: id, property_id, event_date, amount (nullable),
    summary, and any extra columns packed into a 'meta_json' text column.
    The query is responsible for joining to the owner-resolution path.
    """
    stats = BackfillStats(f"{source_table}/{event_type}")
    rows = session.execute(
        text(query).execution_options(yield_per=_COMMIT_BATCH),
    ).mappings()

    for r in rows:
        stats.attempted += 1
        if r["buyer_entity_id"] is None:
            stats.unresolved += 1
            continue

        if not dry_run:
            try:
                import json
                meta = json.loads(r["meta_json"]) if r.get("meta_json") else None
                inserted = record_event(
                    session,
                    buyer_entity_id=r["buyer_entity_id"],
                    event_type=event_type,
                    event_date=r["event_date"],
                    source_table=source_table,
                    source_id=r["id"],
                    property_id=r["property_id"],
                    summary=r.get("summary"),
                    amount=r.get("amount"),
                    meta=meta,
                )
                stats.inserted += inserted
                stats.skipped += not inserted
                if stats.attempted % _COMMIT_BATCH == 0:
                    session.commit()
            except Exception as exc:
                logger.error("%s id=%d: %s", source_table, r["id"], exc)
                session.rollback()
                stats.errors += 1
        else:
            stats.inserted += 1

    if not dry_run:
        session.commit()
    return stats


_FORECLOSURE_QUERY = """
    SELECT
        f.id,
        f.property_id,
        f.filing_date            AS event_date,
        NULL::numeric            AS amount,
        bel.buyer_entity_id,
        'Foreclosure filed: ' || COALESCE(f.case_number, '') AS summary,
        json_build_object(
            'case_number', f.case_number,
            'plaintiff',   f.plaintiff,
            'defendant',   f.defendant
        )::text AS meta_json
    FROM foreclosures f
    JOIN owners o ON o.property_id = f.property_id
    JOIN buyer_entity_links bel
        ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE f.filing_date IS NOT NULL
    ORDER BY f.id
"""

_PERMIT_FILED_QUERY = """
    SELECT
        bp.id,
        bp.property_id,
        bp.issue_date            AS event_date,
        NULL::numeric            AS amount,
        bel.buyer_entity_id,
        'Permit filed: ' || COALESCE(bp.permit_type, '') AS summary,
        json_build_object(
            'permit_number', bp.permit_number,
            'permit_type',   bp.permit_type,
            'description',   bp.description
        )::text AS meta_json
    FROM building_permits bp
    JOIN owners o ON o.property_id = bp.property_id
    JOIN buyer_entity_links bel
        ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE bp.issue_date IS NOT NULL
    ORDER BY bp.id
"""

_LIEN_QUERY = """
    SELECT
        ll.id,
        ll.property_id,
        ll.filing_date           AS event_date,
        ll.amount,
        bel.buyer_entity_id,
        'Lien filed: ' || COALESCE(ll.document_type, ll.record_type) AS summary,
        json_build_object(
            'record_type',    ll.record_type,
            'document_type',  ll.document_type,
            'creditor',       ll.creditor,
            'instrument_number', ll.instrument_number
        )::text AS meta_json
    FROM legal_and_liens ll
    JOIN owners o ON o.property_id = ll.property_id
    JOIN buyer_entity_links bel
        ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE ll.filing_date IS NOT NULL
    ORDER BY ll.id
"""

_LEGAL_PROCEEDING_QUERY = """
    SELECT
        lp.id,
        lp.property_id,
        lp.filing_date           AS event_date,
        lp.amount,
        bel.buyer_entity_id,
        lp.record_type || ' proceeding filed: ' || COALESCE(lp.case_number, '') AS summary,
        json_build_object(
            'record_type',     lp.record_type,
            'case_number',     lp.case_number,
            'case_status',     lp.case_status,
            'associated_party', lp.associated_party
        )::text AS meta_json
    FROM legal_proceedings lp
    JOIN owners o ON o.property_id = lp.property_id
    JOIN buyer_entity_links bel
        ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE lp.filing_date IS NOT NULL
    ORDER BY lp.id
"""

_TAX_DELINQUENCY_QUERY = """
    SELECT
        td.id,
        td.property_id,
        MAKE_DATE(td.tax_year, 1, 1) AS event_date,
        td.total_amount_due          AS amount,
        bel.buyer_entity_id,
        'Tax delinquency: year ' || td.tax_year::text AS summary,
        json_build_object(
            'tax_year',          td.tax_year,
            'certificate_number', td.certificate_number,
            'account_status',    td.account_status
        )::text AS meta_json
    FROM tax_delinquencies td
    JOIN owners o ON o.property_id = td.property_id
    JOIN buyer_entity_links bel
        ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE td.tax_year IS NOT NULL
    ORDER BY td.id
"""

_OPPORTUNITY_QUERY = """
    SELECT
        os.id,
        NULL::int                AS property_id,
        os.created_at::date      AS event_date,
        os.expected_revenue_cents / 100.0 AS amount,
        os.buyer_entity_id,
        'Opportunity opened: ' || os.opportunity_thread_id AS summary,
        json_build_object(
            'opportunity_thread_id', os.opportunity_thread_id,
            'segment',               os.segment,
            'revenue_type',          os.revenue_type
        )::text AS meta_json
    FROM opportunity_scores os
    WHERE os.buyer_entity_id IS NOT NULL
    ORDER BY os.id
"""

# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #

_ALL_SOURCES = {
    "deeds": None,  # handled separately
    "foreclosures": ("foreclosures", "foreclosure_filed", _FORECLOSURE_QUERY),
    "permits": ("building_permits", "permit_filed", _PERMIT_FILED_QUERY),
    "liens": ("legal_and_liens", "lien_filed", _LIEN_QUERY),
    "legal": ("legal_proceedings", "legal_proceeding_filed", _LEGAL_PROCEEDING_QUERY),
    "tax": ("tax_delinquencies", "tax_delinquency", _TAX_DELINQUENCY_QUERY),
    "opportunities": ("opportunity_scores", "opportunity_opened", _OPPORTUNITY_QUERY),
}


def run(source_filter: Optional[str], dry_run: bool) -> None:
    engine = create_engine(get_settings().database_url)
    Session_ = sessionmaker(bind=engine)

    all_stats: list[BackfillStats] = []

    with Session_() as session:
        sources = [source_filter] if source_filter else list(_ALL_SOURCES)

        for src in sources:
            if src not in _ALL_SOURCES:
                logger.error("Unknown source: %s", src)
                continue

            if src == "deeds":
                stats = _backfill_deed_acquisitions(session, dry_run)
            else:
                table, event_type, query = _ALL_SOURCES[src]
                stats = _backfill_via_owner_link(
                    session, dry_run,
                    source_table=table,
                    event_type=event_type,
                    query=query,
                )
            all_stats.append(stats)
            logger.info(stats.report())

    print("\n--- Borrower Ledger Backfill ---")
    print(f"Mode: {'DRY RUN' if dry_run else 'APPLIED'}")
    for s in all_stats:
        print(s.report())
    total_inserted = sum(s.inserted for s in all_stats)
    total_unresolved = sum(s.unresolved for s in all_stats)
    print(f"\nTotal would-insert: {total_inserted}  Unresolved: {total_unresolved}")
    if dry_run:
        print("\nRe-run with --apply to write to the database.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Backfill borrower_ledger_events")
    parser.add_argument(
        "--apply", action="store_true",
        help="Write to DB (default is dry-run)",
    )
    parser.add_argument(
        "--source",
        choices=list(_ALL_SOURCES.keys()),
        help="Backfill only this source (default: all)",
    )
    args = parser.parse_args()

    run(source_filter=args.source, dry_run=not args.apply)
