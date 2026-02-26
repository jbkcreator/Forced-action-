"""
Helper module for integrating scrapers with database loaders.

Provides a unified interface for scrapers to automatically load
their scraped data into the database.
"""

import logging
from pathlib import Path
from typing import Optional, Tuple

from src.core.database import get_db_context
from src.loaders import (
    ViolationLoader,
    ForeclosureLoader,
    LienLoader,
    EvictionLoader,
    ProbateLoader,
    BuildingPermitLoader,
    BankruptcyLoader,
    TaxDelinquencyLoader,
)
from src.utils.csv_deduplicator import rotate_csv_archives

logger = logging.getLogger(__name__)


LOADER_MAP = {
    'violations': ViolationLoader,
    'foreclosures': ForeclosureLoader,
    'liens': LienLoader,
    'evictions': EvictionLoader,
    'probate': ProbateLoader,
    'permits': BuildingPermitLoader,
    'bankruptcy': BankruptcyLoader,
    'tax': TaxDelinquencyLoader,
}


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
            
            # Rotate CSV archives after successful DB insertion
            if destination_dir:
                logger.info("\nRotating CSV archives...")
                if rotate_csv_archives(destination_dir):
                    logger.info("✓ CSV archives rotated successfully")
                else:
                    logger.warning("⚠ CSV archive rotation failed (non-critical)")
            else:
                logger.debug("Skipping CSV rotation (destination_dir not provided)")
            
            return matched, unmatched, skipped
            
    except Exception as e:
        logger.error(f"✗ Database load failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise


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
