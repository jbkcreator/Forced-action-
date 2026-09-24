"""WP-T2-9 Partner Mining & Producer Ranking — nightly sweep cron driver.

Reads new deed records since last run, extracts counterparty signals, resolves
identities via the shared buyer_entities graph, ranks by observed investor-
transaction count, and upserts top-25-per-class into fa_max_partners.

    python -m src.tasks.partner_mining_sweep
    python -m src.tasks.partner_mining_sweep --county-id hillsborough
    python -m src.tasks.partner_mining_sweep --dry-run

Cron: 0 6 * * *  (06:00 UTC — after deed load 05:00–05:10 UTC, before CDS at
07:00; incremental, no full rebuild).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def run(
    *,
    county_id: Optional[str] = None,
    dry_run: bool = False,
    as_of: Optional[date] = None,
) -> dict:
    """
    Execute the partner mining sweep for one or all counties.

    Returns a summary dict with keys: counties_processed, partners_ranked,
    errors, success.
    """
    from src.core.database import get_db_context
    from src.utils.scraper_db_helper import record_scraper_stats

    effective_date = as_of or date.today()
    counties = [county_id] if county_id else ["hillsborough", "pinellas", "pasco"]
    total_ranked = 0
    errors: list[str] = []
    failed_counties: set[str] = set()

    start = time.monotonic()

    for cid in counties:
        try:
            ranked = _run_county(cid, dry_run=dry_run, as_of=effective_date)
            total_ranked += ranked
            logger.info("[PartnerMining] %s: %d partners ranked", cid, ranked)
        except Exception as exc:
            # Pasco is best-effort — log and continue (GRILL Q1).
            msg = f"{cid}: {exc}"
            errors.append(msg)
            failed_counties.add(cid)
            logger.warning("[PartnerMining] %s — skipping county: %s", cid, exc)

    duration = time.monotonic() - start
    success = not failed_counties or (
        # Pasco-only failure is still a partial success.
        failed_counties == {"pasco"} and total_ranked > 0
    )

    if not dry_run:
        try:
            record_scraper_stats(
                source_type="partner_mining",
                total_scraped=total_ranked,
                matched=total_ranked,
                unmatched=0,
                skipped=0,
                run_success=success,
                error_message=("; ".join(errors)[:500] if errors else None),
                duration_seconds=round(duration, 2),
                county_id=county_id or "all",
            )
        except Exception as exc:
            logger.warning("[PartnerMining] failed to record stats: %s", exc)

    return {
        "counties_processed": len(counties) - len(errors),
        "partners_ranked": total_ranked,
        "errors": errors,
        "success": success,
    }


def _run_county(county_id: str, *, dry_run: bool, as_of: date) -> int:
    """
    Run partner mining for a single county.
    Returns count of partners ranked (across all classes).
    """
    from src.core.database import get_db_context
    from src.services.partner_mining.investor_txn import is_investor_transaction
    from src.services.partner_mining.extract import extract_lender, find_wholesaler_candidates
    from src.services.partner_mining.classify import assign_partner_class, PartnerClass
    from src.services.partner_mining.rank import rank_partners, PartnerRow
    from src.services.partner_mining.persist import upsert_partner_rows
    from sqlalchemy import text
    from datetime import timedelta

    lookback_days = 36 * 30  # ~36 months (GRILL Q1)
    since = as_of - timedelta(days=lookback_days)

    with get_db_context() as db:
        # Stage B: resolve new lender counterparties into buyer_entities graph.
        from src.services.partner_mining.resolution import (
            run_counterparty_resolution, run_wholesaler_resolution,
            resolve_counterparty_names,
        )
        # Both resolution steps commit internally, so dry-run must skip them.
        if not dry_run:
            run_counterparty_resolution(db, county_id=county_id)
            run_wholesaler_resolution(db, county_id=county_id)

        # Load deed rows for this county within the lookback window.
        result = db.execute(
            text("""
                SELECT
                    d.id, d.property_id, d.instrument_number,
                    d.grantor, d.grantee, d.deed_type, d.doc_type,
                    d.sale_price, d.sale_qualified, d.mortgage_amount,
                    d.record_date, d.county_id,
                    f.homestead_exempt
                FROM deeds d
                LEFT JOIN financials f ON f.property_id = d.property_id
                WHERE d.county_id = :county_id
                  AND d.record_date >= :since
                  AND d.record_date <= :as_of
                ORDER BY d.record_date
            """),
            {"county_id": county_id, "since": since, "as_of": as_of},
        )
        # Materialise into a list: find_wholesaler_candidates requires two passes
        # (group by property_id, then iterate for lender extraction). The lookback
        # window (36 months) bounds total size; yield_per(1000) keeps the server-side
        # cursor open rather than fetching all rows at once.
        deed_rows = list(result.yield_per(1000))

        if not deed_rows:
            logger.info("[PartnerMining] %s: no deed rows in window", county_id)
            return 0

        # ── Stage A: extract counterparties ──────────────────────────────────
        lender_counts: dict[str, dict] = {}
        wholesaler_names = set(find_wholesaler_candidates(deed_rows))
        wholesaler_txn_counts: dict[str, int] = {}

        for row in deed_rows:
            homestead = bool(getattr(row, "homestead_exempt", False))

            lender = extract_lender(row)
            if lender:
                key = lender.strip().upper()
                if key not in lender_counts:
                    lender_counts[key] = {
                        "count": 0,
                        "first": row.record_date,
                        "last": row.record_date,
                        "volume": 0.0,
                    }
                entry = lender_counts[key]
                entry["count"] += 1
                if row.record_date < entry["first"]:
                    entry["first"] = row.record_date
                if row.record_date > entry["last"]:
                    entry["last"] = row.record_date
                entry["volume"] += float(row.mortgage_amount or 0)
                continue

            if not is_investor_transaction(row, homestead_exempt=homestead):
                continue

            # Track wholesaler transaction volume for observed-count ranking.
            grantee = (getattr(row, "grantee", None) or "").strip().upper()
            if grantee in wholesaler_names:
                wholesaler_txn_counts[grantee] = wholesaler_txn_counts.get(grantee, 0) + 1

        # ── Stage B lookup: map raw lender names → buyer_entity_id ──────────
        lender_name_map = resolve_counterparty_names(db, list(lender_counts.keys()))

        # ── Stage C + D: build PartnerRow list and rank ───────────────────────
        partner_rows: list[PartnerRow] = []

        for name, stats in lender_counts.items():
            partner_rows.append(PartnerRow(
                buyer_entity_id=lender_name_map.get(name) or 0,
                canonical_name=name,
                partner_class=PartnerClass.LENDER.value,
                observed_transaction_count=stats["count"],
                first_observed_at=stats["first"],
                last_observed_at=stats["last"],
                total_cash_volume=stats["volume"],
            ))

        wholesaler_name_map = resolve_counterparty_names(
            db, list(wholesaler_names), source_table="deed_wholesaler"
        )

        for name in wholesaler_names:
            partner_rows.append(PartnerRow(
                buyer_entity_id=wholesaler_name_map.get(name) or 0,
                canonical_name=name,
                partner_class=PartnerClass.WHOLESALER.value,
                observed_transaction_count=wholesaler_txn_counts.get(name, 1),
                last_observed_at=as_of,
                first_observed_at=as_of,
            ))

        ranked = rank_partners(partner_rows)

        if dry_run:
            logger.info("[PartnerMining] DRY RUN %s: %d partners ranked (not persisted)",
                        county_id, len(ranked))
            return len(ranked)

        # ── Stage E: enrich top-25 per class ─────────────────────────────────
        from src.services.partner_mining.enrich import enrich_top_partners
        enrich_stats = enrich_top_partners(db, ranked, county_id=county_id)
        logger.info("[PartnerMining] %s enrichment: %s", county_id, enrich_stats)

        upsert_partner_rows(db, ranked, county_id=county_id)
        return len(ranked)


def main(argv: Optional[list[str]] = None) -> None:
    # run.sh keeps only lines matching " - WARNING|ERROR|CRITICAL - ".
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="WP-T2-9 partner mining sweep")
    parser.add_argument("--county-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    result = run(county_id=args.county_id, dry_run=args.dry_run, as_of=as_of)
    logger.info("[PartnerMining] done: %s", result)
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
