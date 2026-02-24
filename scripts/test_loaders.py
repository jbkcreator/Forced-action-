"""
Test script for data loaders module.

This script provides comprehensive testing for individual and all loaders,
including validation, dry-run mode, and detailed reporting.

Usage:
    # Test single loader
    python scripts/test_loaders.py --type violations
    
    # Test multiple loaders
    python scripts/test_loaders.py --types master,violations,liens
    
    # Test all loaders
    python scripts/test_loaders.py --all
    
    # Dry run (validate only, no database insertion)
    python scripts/test_loaders.py --type violations --dry-run
    
    # Test with custom file
    python scripts/test_loaders.py --type violations --file data/test_violations.csv
    
    # Verbose mode
    python scripts/test_loaders.py --all --verbose
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import pandas as pd

from src.core.database import get_db_context, check_connection, get_table_counts
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/loader_tests.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


# Test configuration
TEST_DATA_PATHS = {
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

LOADER_CLASSES = {
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

EXPECTED_COLUMNS = {
    'master': ['FOLIO', 'OWNER', 'SITE_ADDR'],
    'violations': ['Record Number', 'Address', 'Status'],
    'liens': ['Instrument', 'Grantor', 'Grantee', 'RecordDate', 'document_type'],
    'deeds': ['Instrument', 'Grantor', 'Grantee', 'RecordDate'],
    'probate': ['CaseNumber', 'PartyAddress', 'FilingDate', 'PartyType'],
    'evictions': ['CaseNumber', 'PartyAddress', 'FilingDate', 'PartyType'],
    'bankruptcy': ['Docket Number', 'Lead Name', 'Date Filed'],
    'tax': ['Account Number', 'Tax Yr', 'Owner Name'],
    'foreclosures': ['Case Number', 'Parcel ID', 'Property Address'],
    'permits': ['Record Number', 'Address', 'Status'],
}


def print_section_header(title: str, char: str = "=", width: int = 70) -> None:
    """Print formatted section header."""
    logger.info("\n" + char * width)
    logger.info(title)
    logger.info(char * width)


def validate_file(file_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate file exists and has expected structure.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    path = Path(file_path)
    
    if not path.exists():
        return False, f"File not found: {file_path}"
    
    if not path.is_file():
        return False, f"Not a file: {file_path}"
    
    try:
        df = pd.read_csv(file_path, nrows=5)
        if len(df) == 0:
            return False, "File is empty"
        return True, None
    except Exception as e:
        return False, f"Failed to read CSV: {e}"


def validate_columns(file_path: str, data_type: str) -> Tuple[bool, List[str]]:
    """
    Validate CSV has expected columns.
    
    Returns:
        Tuple of (has_all_required, missing_columns)
    """
    if data_type not in EXPECTED_COLUMNS:
        return True, []  # No validation for unknown types
    
    try:
        df = pd.read_csv(file_path, nrows=1)
        required = set(EXPECTED_COLUMNS[data_type])
        actual = set(df.columns)
        missing = list(required - actual)
        return len(missing) == 0, missing
    except Exception as e:
        logger.error(f"Failed to validate columns: {e}")
        return False, []


def get_file_stats(file_path: str) -> Dict:
    """Get file statistics."""
    path = Path(file_path)
    df = pd.read_csv(file_path)
    
    return {
        'path': str(path),
        'size_mb': path.stat().st_size / (1024 ** 2),
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': df.columns.tolist(),
    }


def test_loader(
    data_type: str,
    file_path: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False
) -> Dict:
    """
    Test a single loader.
    
    Args:
        data_type: Type of data to test
        file_path: Custom file path (optional)
        dry_run: If True, only validate, don't insert
        verbose: Enable verbose logging
        
    Returns:
        Dict with test results
    """
    print_section_header(f"TESTING {data_type.upper()} LOADER")
    
    # Use custom path or default
    csv_path = file_path or TEST_DATA_PATHS.get(data_type)
    
    if not csv_path:
        return {
            'data_type': data_type,
            'status': 'FAILED',
            'error': 'No file path configured',
        }
    
    logger.info(f"File: {csv_path}")
    
    # Step 1: Validate file
    logger.info("\n[1/4] Validating file...")
    is_valid, error = validate_file(csv_path)
    if not is_valid:
        return {
            'data_type': data_type,
            'status': 'FAILED',
            'error': error,
        }
    logger.info("  ✓ File exists and is readable")
    
    # Step 2: Validate columns
    logger.info("\n[2/4] Validating columns...")
    has_columns, missing = validate_columns(csv_path, data_type)
    if not has_columns:
        logger.warning(f"  ⚠ Missing required columns: {missing}")
    else:
        logger.info("  ✓ All required columns present")
    
    # Step 3: Get file stats
    logger.info("\n[3/4] Analyzing file...")
    stats = get_file_stats(csv_path)
    logger.info(f"  Rows: {stats['row_count']:,}")
    logger.info(f"  Columns: {stats['column_count']}")
    logger.info(f"  Size: {stats['size_mb']:.2f} MB")
    if verbose:
        logger.info(f"  Column names: {', '.join(stats['columns'])}")
    
    if dry_run:
        logger.info("\n[4/4] SKIPPED - Dry run mode (no database insertion)")
        return {
            'data_type': data_type,
            'status': 'DRY_RUN',
            'file_stats': stats,
            'matched': 0,
            'unmatched': 0,
            'skipped': 0,
        }
    
    # Step 4: Load data
    logger.info("\n[4/4] Loading data into database...")
    
    try:
        if data_type not in LOADER_CLASSES:
            return {
                'data_type': data_type,
                'status': 'FAILED',
                'error': f'Unknown data type: {data_type}',
            }
        
        with get_db_context() as session:
            loader_class = LOADER_CLASSES[data_type]
            loader = loader_class(session)
            
            start_time = datetime.now()
            matched, unmatched, skipped = loader.load_from_csv(csv_path, skip_duplicates=True)
            duration = (datetime.now() - start_time).total_seconds()
            
            session.commit()
            
            logger.info(f"\n  ✓ Load complete in {duration:.2f}s")
            logger.info(f"    Matched:   {matched:>6}")
            logger.info(f"    Unmatched: {unmatched:>6}")
            logger.info(f"    Skipped:   {skipped:>6}")
            
            total = matched + unmatched + skipped
            match_rate = (matched / total * 100) if total > 0 else 0
            logger.info(f"    Match Rate: {match_rate:>5.1f}%")
            
            return {
                'data_type': data_type,
                'status': 'SUCCESS',
                'file_stats': stats,
                'matched': matched,
                'unmatched': unmatched,
                'skipped': skipped,
                'match_rate': match_rate,
                'duration_seconds': duration,
            }
            
    except Exception as e:
        logger.error(f"  ✗ Failed to load: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        
        return {
            'data_type': data_type,
            'status': 'FAILED',
            'error': str(e),
        }


def test_all_loaders(
    file_overrides: Dict[str, str] = None,
    dry_run: bool = False,
    verbose: bool = False
) -> List[Dict]:
    """
    Test all loaders in dependency order.
    
    Args:
        file_overrides: Custom file paths
        dry_run: If True, only validate
        verbose: Enable verbose logging
        
    Returns:
        List of test results
    """
    file_overrides = file_overrides or {}
    
    print_section_header("TESTING ALL LOADERS", char="#")
    
    # Test order (dependencies first)
    test_order = [
        'master',       # Must be first
        'tax',          # High accuracy (parcel ID)
        'foreclosures', # High accuracy (parcel ID)
        'violations',   # Medium accuracy (address)
        'liens',        # Medium accuracy (name)
        'deeds',        # Medium accuracy (name)
        'probate',      # Medium accuracy (address + name)
        'evictions',    # Medium accuracy (address)
        'permits',      # Medium accuracy (address)
        'bankruptcy',   # Low accuracy (name only)
    ]
    
    results = []
    
    for data_type in test_order:
        file_path = file_overrides.get(data_type)
        result = test_loader(data_type, file_path, dry_run, verbose)
        results.append(result)
        
        # Short pause between tests
        import time
        time.sleep(0.5)
    
    # Print summary
    print_section_header("TEST SUMMARY", char="#")
    
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed_count = sum(1 for r in results if r['status'] == 'FAILED')
    dry_run_count = sum(1 for r in results if r['status'] == 'DRY_RUN')
    
    for result in results:
        status = result['status']
        symbol = "✓" if status == 'SUCCESS' else "⚠" if status == 'DRY_RUN' else "✗"
        
        data_type = result['data_type']
        logger.info(f"  {symbol} {data_type:<15} {status}")
        
        if status == 'SUCCESS':
            logger.info(f"      → {result['matched']} matched, {result['unmatched']} unmatched, {result['skipped']} skipped ({result['match_rate']:.1f}%)")
        elif status == 'FAILED' and 'error' in result:
            logger.info(f"      → Error: {result['error']}")
    
    logger.info(f"\n  Total: {len(results)} tests")
    logger.info(f"    Success: {success_count}")
    logger.info(f"    Failed:  {failed_count}")
    if dry_run:
        logger.info(f"    Dry run: {dry_run_count}")
    
    # Print table counts if not dry run
    if not dry_run:
        try:
            print_section_header("DATABASE TABLE COUNTS")
            counts = get_table_counts()
            for table, count in counts.items():
                logger.info(f"  {table:<25} {count:>8,}")
        except Exception as e:
            logger.error(f"Failed to get table counts: {e}")
    
    logger.info("#" * 70 + "\n")
    
    return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Test data loaders with comprehensive validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test single loader
  python scripts/test_loaders.py --type violations
  
  # Test multiple loaders
  python scripts/test_loaders.py --types master,violations,liens
  
  # Test all loaders
  python scripts/test_loaders.py --all
  
  # Dry run (validate only)
  python scripts/test_loaders.py --type violations --dry-run
  
  # Custom file
  python scripts/test_loaders.py --type violations --file data/my_violations.csv
  
  # Verbose mode
  python scripts/test_loaders.py --all --verbose
        """
    )
    
    parser.add_argument(
        '--type',
        type=str,
        help='Test single loader (master, violations, liens, etc.)'
    )
    
    parser.add_argument(
        '--types',
        type=str,
        help='Test multiple loaders (comma-separated)'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Test all loaders in order'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Custom CSV file path (for use with --type)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate only, do not insert into database'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Check database connection first
    logger.info("Checking database connection...")
    if not check_connection():
        logger.error("✗ Database connection failed!")
        logger.error("Please check your database configuration in .env")
        sys.exit(1)
    logger.info("✓ Database connection successful\n")
    
    # Run tests
    if args.all:
        test_all_loaders(dry_run=args.dry_run, verbose=args.verbose)
    
    elif args.type:
        test_loader(args.type, args.file, args.dry_run, args.verbose)
    
    elif args.types:
        types_list = [t.strip() for t in args.types.split(',')]
        for data_type in types_list:
            test_loader(data_type, None, args.dry_run, args.verbose)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
