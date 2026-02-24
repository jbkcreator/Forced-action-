"""
CLI tool for loading scraped data into database.

This is a testing/manual script. In production, scrapers call loaders directly.

Usage:
    # Initialize database
    python scripts/load_data.py --init-db
    
    # Load specific data type
    python scripts/load_data.py --type violations
    
    # Load multiple types
    python scripts/load_data.py --types master,violations,liens
    
    # Load all data
    python scripts/load_data.py --all
    
    # Custom CSV file
    python scripts/load_data.py --type violations --file data/my_violations.csv
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError

from src.core.database import get_db_context, init_database, get_table_counts
from src.loaders import (
    MasterPropertyLoader,
    ViolationLoader,
    LienLoader,
    DeedLoader,
    ProbateLoader,
    EvictionLoader,
    BankruptcyLoader,
    TaxDelinquencyLoader,
    ForeclosureLoader,
    BuildingPermitLoader,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/data_loading.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


# Default file paths
DEFAULT_PATHS = {
    'master': 'data/raw/master/master.csv',
    'violations': 'data/raw/violations/hcfl_code_enforcement_violations.csv',
    'liens': 'data/raw/liens/all_liens_judgments.csv',
    'deeds': 'data/raw/deeds/all_deeds.csv',
    'probate': 'data/raw/probate/probate_leads.csv',
    'evictions': 'data/raw/evictions/eviction_leads_20260220.csv',
    'bankruptcy': 'data/raw/bankruptcy/tampa_bankruptcy_leads.csv',
    'tax': 'data/raw/tax_delinquencies/tax_deliquencies.csv',
    'foreclosures': 'data/raw/foreclosures/hillsborough_realforeclose_20260218.csv',
    'permits': 'data/raw/permits/building_permits_sample.csv',
}

# Loader mapping
LOADERS = {
    'master': MasterPropertyLoader,
    'violations': ViolationLoader,
    'liens': LienLoader,
    'deeds': DeedLoader,
    'probate': ProbateLoader,
    'evictions': EvictionLoader,
    'bankruptcy': BankruptcyLoader,
    'tax': TaxDelinquencyLoader,
    'foreclosures': ForeclosureLoader,
    'permits': BuildingPermitLoader,
}


def validate_file(file_path: str) -> bool:
    """Check if file exists and is readable."""
    path = Path(file_path)
    if not path.exists():
        logger.error(f"File not found: {file_path}")
        return False
    if not path.is_file():
        logger.error(f"Not a file: {file_path}")
        return False
    return True


def load_data_type(
    data_type: str,
    file_path: Optional[str] = None,
    skip_duplicates: bool = True
) -> None:
    """
    Load a specific data type.
    
    Args:
        data_type: Type of data to load (master, violations, etc.)
        file_path: Custom file path (optional)
        skip_duplicates: Skip existing records
    """
    if data_type not in LOADERS:
        logger.error(f"Unknown data type: {data_type}")
        logger.error(f"Available types: {', '.join(LOADERS.keys())}")
        sys.exit(1)
    
    # Use custom path or default
    csv_path = file_path or DEFAULT_PATHS.get(data_type)
    
    if not csv_path:
        logger.error(f"No default path for {data_type}. Please specify --file")
        sys.exit(1)
    
    # Validate file
    if not validate_file(csv_path):
        sys.exit(1)
    
    logger.info("=" * 70)
    logger.info(f"LOADING {data_type.upper()}")
    logger.info(f"File: {csv_path}")
    logger.info("=" * 70)
    
    try:
        with get_db_context() as session:
            # Create loader
            loader_class = LOADERS[data_type]
            loader = loader_class(session)
            
            # Load data
            matched, unmatched, skipped = loader.load_from_csv(
                csv_path,
                skip_duplicates=skip_duplicates
            )
            
            # Commit transaction
            session.commit()
            
            # Print summary
            logger.info(f"\n{'='*70}")
            logger.info(f"SUMMARY - {data_type.upper()}")
            logger.info(f"{'='*70}")
            logger.info(f"  Matched:   {matched:>6}")
            logger.info(f"  Unmatched: {unmatched:>6}")
            logger.info(f"  Skipped:   {skipped:>6}")
            
            total = matched + unmatched + skipped
            match_rate = (matched / total * 100) if total > 0 else 0
            logger.info(f"  Match Rate: {match_rate:>5.1f}%")
            logger.info(f"{'='*70}\n")
            
    except IntegrityError as e:
        logger.error(f"Database integrity error: {e}")
        logger.error("This may be due to duplicate records.")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error loading {data_type}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def load_all_data(
    file_overrides: Dict[str, str] = None,
    skip_duplicates: bool = True
) -> None:
    """
    Load all data types in order.
    
    Args:
        file_overrides: Custom file paths for specific types
        skip_duplicates: Skip existing records
    """
    file_overrides = file_overrides or {}
    
    # Define loading order (dependencies first)
    load_order = [
        'master',       # Must be first (properties are required for matching)
        'tax',          # Parcel ID matching (high accuracy)
        'foreclosures', # Parcel ID matching (high accuracy)
        'violations',   # Address matching
        'liens',        # Name matching
        'deeds',        # Name matching
        'probate',      # Address + name matching
        'evictions',    # Address matching
        'permits',      # Address matching
        'bankruptcy',   # Name matching (low accuracy)
    ]
    
    logger.info("#" * 70)
    logger.info("LOADING ALL DATA TYPES")
    logger.info("#" * 70)
    
    results = {}
    
    for data_type in load_order:
        file_path = file_overrides.get(data_type)
        
        try:
            logger.info(f"\n>>> Loading {data_type}...")
            load_data_type(data_type, file_path, skip_duplicates)
            results[data_type] = 'SUCCESS'
            
        except Exception as e:
            logger.error(f"Failed to load {data_type}: {e}")
            results[data_type] = 'FAILED'
            continue
    
    # Print final summary
    logger.info("\n" + "#" * 70)
    logger.info("FINAL SUMMARY")
    logger.info("#" * 70)
    
    for data_type, status in results.items():
        symbol = "✓" if status == 'SUCCESS' else "✗"
        logger.info(f"  {symbol} {data_type:<15} {status}")
    
    # Print table counts
    try:
        counts = get_table_counts()
        logger.info("\nDATABASE TABLE COUNTS:")
        for table, count in counts.items():
            logger.info(f"  {table:<25} {count:>8,}")
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")
    
    logger.info("#" * 70 + "\n")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Load scraped data into database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize database
  python scripts/load_data.py --init-db
  
  # Load specific type
  python scripts/load_data.py --type violations
  
  # Load multiple types
  python scripts/load_data.py --types master,violations,liens
  
  # Load all data
  python scripts/load_data.py --all

  # Custom file
  python scripts/load_data.py --type violations --file data/my_violations.csv
        """
    )
    
    parser.add_argument(
        '--init-db',
        action='store_true',
        help='Initialize database (create tables)'
    )
    
    parser.add_argument(
        '--type',
        type=str,
        help='Load single data type (master, violations, liens, etc.)'
    )
    
    parser.add_argument(
        '--types',
        type=str,
        help='Load multiple data types (comma-separated)'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Load all data types in order'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Custom CSV file path (for use with --type)'
    )
    
    parser.add_argument(
        '--fail-on-duplicates',
        action='store_true',
        help='Fail on duplicate records instead of skipping'
    )
    
    args = parser.parse_args()
    
    # Initialize database if requested
    if args.init_db:
        logger.info("Initializing database...")
        init_database()
        logger.info("✓ Database initialized\n")
        if not (args.type or args.types or args.all):
            return
    
    skip_duplicates = not args.fail_on_duplicates
    
    # Load data
    if args.all:
        load_all_data(skip_duplicates=skip_duplicates)
    
    elif args.type:
        load_data_type(args.type, args.file, skip_duplicates)
    
    elif args.types:
        types_list = [t.strip() for t in args.types.split(',')]
        for data_type in types_list:
            load_data_type(data_type, None, skip_duplicates)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
