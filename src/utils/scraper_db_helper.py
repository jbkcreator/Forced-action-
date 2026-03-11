"""
Helper module for integrating scrapers with database loaders.

Provides a unified interface for scrapers to automatically load
their scraped data into the database.
"""

import logging
import time
from datetime import date as date_type
from pathlib import Path
from typing import Optional, Tuple

from src.core.database import get_db_context
from src.loaders import (
    ViolationLoader,
    ForeclosureLoader,
    LienLoader,
    DeedLoader,
    EvictionLoader,
    ProbateLoader,
    BuildingPermitLoader,
    BankruptcyLoader,
    TaxDelinquencyLoader,
)

logger = logging.getLogger(__name__)

# Maps lien document_type labels (from lien_engine classification) to scraper_run_stats source_type keys.
# Liens arrive in one combined CSV but each row carries a document_type; stats are broken out per subtype.
LIEN_DOCTYPE_TO_SOURCE = {
    'TAMPA CODE LIENS (TCL)':   'lien_tcl',
    'COUNTY CODE LIENS (CCL)':  'lien_ccl',
    'HOA LIENS (HL)':           'lien_hoa',
    'MECHANICS LIENS (ML)':     'lien_ml',
    'TAX LIENS (TL)':           'lien_tl',
}

# Maps the data_type key used by load_scraped_data_to_db → source_type stored in scraper_run_stats.
# 'liens' is handled specially (split by document_type).  'judgments' maps to its own key.
DATA_TYPE_TO_SOURCE = {
    'violations':   'violations',
    'foreclosures': 'foreclosures',
    'judgments':    'judgments',
    'deeds':        'deeds',
    'evictions':    'evictions',
    'probate':      'probate',
    'permits':      'permits',
    'bankruptcy':   'bankruptcy',
    'tax':          'tax_delinquencies',
}


LOADER_MAP = {
    'violations': ViolationLoader,
    'foreclosures': ForeclosureLoader,
    'liens': LienLoader,
    'judgments': LienLoader,
    'deeds': DeedLoader,
    'evictions': EvictionLoader,
    'probate': ProbateLoader,
    'permits': BuildingPermitLoader,
    'bankruptcy': BankruptcyLoader,
    'tax': TaxDelinquencyLoader,
}


def record_scraper_stats(
    source_type: str,
    total_scraped: int,
    matched: int,
    unmatched: int,
    skipped: int,
    scored: int = 0,
    run_success: bool = True,
    error_message: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    run_date=None,
    county_id: str = 'hillsborough',
) -> None:
    """
    Upsert a row into scraper_run_stats for today's run.

    Uses INSERT … ON CONFLICT DO UPDATE so calling this multiple times
    (e.g. on retry) accumulates rather than duplicates.

    Args:
        source_type: One of the values in the check_run_stats_source_type constraint.
        total_scraped: Total rows scraped from the source.
        matched/unmatched/skipped: Loader output counts.
        scored: Number of properties rescored by CDS after this load.
        run_success: False if the run errored out.
        error_message: Short error description on failure.
        duration_seconds: Wall-clock seconds for the load+rescore step.
        run_date: datetime.date; defaults to today.
        county_id: County this run applies to.
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from src.core.models import ScraperRunStats

    if run_date is None:
        run_date = date_type.today()

    try:
        with get_db_context() as session:
            stmt = pg_insert(ScraperRunStats).values(
                run_date=run_date,
                source_type=source_type,
                county_id=county_id,
                total_scraped=total_scraped,
                matched=matched,
                unmatched=unmatched,
                skipped=skipped,
                scored=scored,
                run_success=run_success,
                error_message=error_message,
                duration_seconds=duration_seconds,
            ).on_conflict_do_update(
                constraint='uq_scraper_run_stats',
                set_=dict(
                    total_scraped=total_scraped,
                    matched=matched,
                    unmatched=unmatched,
                    skipped=skipped,
                    scored=scored,
                    run_success=run_success,
                    error_message=error_message,
                    duration_seconds=duration_seconds,
                )
            )
            session.execute(stmt)
            session.commit()
            logger.info(f"✓ Scraper stats recorded: {source_type} | scraped={total_scraped} matched={matched} unmatched={unmatched} skipped={skipped} scored={scored}")
    except Exception as e:
        logger.warning(f"⚠ Could not record scraper stats for {source_type} (non-critical): {e}")


def load_scraped_data_to_db(
    data_type: str,
    csv_path: Path,
    destination_dir: Optional[Path] = None,
    skip_duplicates: bool = True,
    sample_mode: bool = False
) -> Tuple[int, int, int]:
    """
    Load scraped CSV data into database using appropriate loader.

    After successful database insertion, rotates CSV archives to prevent
    storage buildup (moves new/ → old/, deletes old archives).

    Args:
        data_type: Type of data ('violations', 'foreclosures', 'liens', etc.)
        csv_path: Path to the CSV file to load
        destination_dir: Base directory containing old/ and new/ subdirectories
                        (required for CSV rotation after successful load)
        skip_duplicates: Skip existing records
        sample_mode: Load only sample data (for testing)

    Returns:
        Tuple of (matched, unmatched, skipped)

    Raises:
        ValueError: If data_type is unknown
        Exception: If database load fails
    """
    if data_type not in LOADER_MAP:
        raise ValueError(f"Unknown data type: {data_type}. Available: {list(LOADER_MAP.keys())}")

    logger.info("\n" + "=" * 60)
    logger.info(f"Loading {data_type} into database...")
    logger.info(f"CSV file: {csv_path}")
    logger.info("=" * 60)

    t_start = time.monotonic()

    try:
        with get_db_context() as session:
            loader_class = LOADER_MAP[data_type]
            loader = loader_class(session)

            loader_kwargs = {'skip_duplicates': skip_duplicates}
            if sample_mode:
                loader_kwargs['sample_mode'] = True
                logger.info("🧪 SAMPLE MODE enabled")

            matched, unmatched, skipped = loader.load_from_csv(
                str(csv_path),
                **loader_kwargs
            )
            session.commit()

            # Print summary
            logger.info(f"\n{'='*60}")
            logger.info(f"DATABASE LOAD SUMMARY - {data_type.upper()}")
            logger.info(f"{'='*60}")
            logger.info(f"  Matched:   {matched:>6}")
            logger.info(f"  Unmatched: {unmatched:>6}")
            logger.info(f"  Skipped:   {skipped:>6}")

            total = matched + unmatched + skipped
            match_rate = (matched / total * 100) if total > 0 else 0
            logger.info(f"  Match Rate: {match_rate:>5.1f}%")
            logger.info(f"{'='*60}\n")

            logger.info("✓ Database load completed!")

            # Delete CSV after successful DB insertion; keep it on error for debugging
            try:
                csv_path.unlink()
                logger.info(f"✓ CSV deleted after successful DB insertion: {csv_path.name}")
            except Exception as e:
                logger.warning(f"⚠ Could not delete CSV (non-critical): {e}")

            # Ingestion-time rescoring: rescore only the properties touched by
            # this scraper run so scores are always up-to-date in real time.
            affected_ids = loader.get_affected_property_ids()
            scored = 0
            if affected_ids:
                logger.info(f"Triggering CDS rescore for {len(affected_ids)} affected properties...")
                try:
                    from src.services.cds_engine import MultiVerticalScorer
                    with get_db_context() as score_session:
                        scorer = MultiVerticalScorer(score_session)
                        scorer.score_properties_by_ids(affected_ids, save_to_db=True)
                        score_session.commit()
                    scored = len(affected_ids)
                    logger.info("✓ CDS rescore completed")
                except Exception as score_err:
                    logger.warning(f"⚠ CDS rescore failed (non-critical): {score_err}")
            else:
                logger.debug("No matched properties to rescore")

            duration = round(time.monotonic() - t_start, 2)

            # ── Record per-source stats ────────────────────────────────
            if data_type == 'liens':
                # Liens CSV has mixed document types; stats are split per subtype
                # using per-document_type counts captured by the loader.
                lien_counts = getattr(loader, 'stats_by_doc_type', {})
                if lien_counts:
                    for doc_type_label, counts in lien_counts.items():
                        src = LIEN_DOCTYPE_TO_SOURCE.get(doc_type_label, 'lien_ml')
                        record_scraper_stats(
                            source_type=src,
                            total_scraped=counts.get('total', 0),
                            matched=counts.get('matched', 0),
                            unmatched=counts.get('unmatched', 0),
                            skipped=counts.get('skipped', 0),
                            scored=0,
                            duration_seconds=duration,
                        )
                else:
                    # Loader doesn't expose per-type breakdown; record aggregate under a generic key
                    record_scraper_stats(
                        source_type='lien_ml',  # placeholder when breakdown unavailable
                        total_scraped=total,
                        matched=matched,
                        unmatched=unmatched,
                        skipped=skipped,
                        scored=scored,
                        duration_seconds=duration,
                    )
            else:
                source_type_key = DATA_TYPE_TO_SOURCE.get(data_type)
                if source_type_key:
                    record_scraper_stats(
                        source_type=source_type_key,
                        total_scraped=total,
                        matched=matched,
                        unmatched=unmatched,
                        skipped=skipped,
                        scored=scored,
                        duration_seconds=duration,
                    )

            return matched, unmatched, skipped

    except Exception as e:
        duration = round(time.monotonic() - t_start, 2)
        # Record the failure in stats (non-critical — don't let it mask original error)
        source_type_key = DATA_TYPE_TO_SOURCE.get(data_type)
        if source_type_key:
            record_scraper_stats(
                source_type=source_type_key,
                total_scraped=0,
                matched=0,
                unmatched=0,
                skipped=0,
                scored=0,
                run_success=False,
                error_message=str(e)[:500],
                duration_seconds=duration,
            )
        logger.error(f"✗ Database load failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise


def get_daily_scrape_counts(target_date=None):
    """
    Return a dict of {table_name: record_count} for records scraped on target_date.

    Uses the date_added field on each signal table. Defaults to today if not specified.

    Args:
        target_date: datetime.date or None (defaults to today)

    Returns:
        dict with table names as keys, counts as values, plus '_total' and '_date' keys.

    Example:
        >>> counts = get_daily_scrape_counts()
        >>> print(counts)
        {'code_violations': 45, 'deeds': 120, ..., '_total': 389, '_date': '2026-03-03'}
    """
    from src.core.models import (
        CodeViolation, LegalAndLien, Deed, LegalProceeding,
        TaxDelinquency, Foreclosure, BuildingPermit, Incident,
    )

    if target_date is None:
        target_date = date_type.today()

    TABLE_MODELS = {
        'code_violations':  CodeViolation,
        'legal_and_liens':  LegalAndLien,
        'deeds':            Deed,
        'legal_proceedings': LegalProceeding,
        'tax_delinquencies': TaxDelinquency,
        'foreclosures':     Foreclosure,
        'building_permits': BuildingPermit,
        'incidents':        Incident,
    }

    with get_db_context() as session:
        counts = {}
        for table_name, model in TABLE_MODELS.items():
            counts[table_name] = session.query(model).filter(
                model.date_added == target_date
            ).count()

    counts['_total'] = sum(v for k, v in counts.items() if not k.startswith('_'))
    counts['_date'] = str(target_date)
    return counts


def print_daily_scrape_report(target_date=None):
    """Print a formatted daily scrape count report to the log."""
    counts = get_daily_scrape_counts(target_date)
    logger.info("=" * 50)
    logger.info(f"DAILY SCRAPE COUNTS — {counts['_date']}")
    logger.info("=" * 50)
    for table, count in counts.items():
        if not table.startswith('_'):
            logger.info(f"  {table:<20} {count:>6}")
    logger.info("-" * 50)
    logger.info(f"  {'TOTAL':<20} {counts['_total']:>6}")
    logger.info("=" * 50)


def add_load_to_db_arg(parser):
    """
    Add --load-to-db argument to an argparse parser.
    
    Args:
        parser: argparse.ArgumentParser instance
        
    Returns:
        The modified parser
    """
    parser.add_argument(
        "--load-to-db",
        action="store_true",
        help="Automatically load scraped data into database after scraping"
    )
    return parser
