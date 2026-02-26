"""
CSV Deduplication Utility

Filters duplicate records from newly scraped CSVs by comparing against
existing historical CSV files. This prevents duplicate data when daily
scrapes have overlapping date ranges.

Example:
    Day 1: Scrape Feb 23-24 → 200 records
    Day 2: Scrape Feb 24-25 → 200 records (100 duplicates from Feb 24)
    Result: Deduplicator filters out Feb 24 duplicates, keeps only new Feb 25 data
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set

import pandas as pd

logger = logging.getLogger(__name__)


# Unique key column mappings for each data type
UNIQUE_KEY_MAP = {
    'violations': ['Record Number'],
    'foreclosures': ['Case Number'],
    'liens': ['Instrument'],
    'judgments': ['Instrument'],
    'evictions': ['CaseNumber'],
    'probate': ['CaseNumber'],
    'permits': ['Record Number'],
    'bankruptcy': ['Docket Number'],
    'tax': ['Account Number'],
    'deeds': ['Instrument'],
}

# Raw source file patterns to exclude from deduplication checks
# These are the downloaded files that haven't been processed yet
RAW_FILE_EXCLUSIONS = [
    'CivilFiling_*.csv',       # Raw civil filing downloads (evictions source)
    'ProbateFiling_*.csv',     # Raw probate filing downloads
    'hillsborough_realforeclose_*.csv',  # Raw foreclosure downloads (old pattern)
    '*_temp.csv',              # Temporary files
]


def get_existing_unique_keys(
    destination_dir: Path,
    unique_columns: List[str],
    file_pattern: str = "*.csv"
) -> Set[tuple]:
    """
    Extract unique keys from all existing CSV files in directory.
    
    Args:
        destination_dir: Directory containing existing CSV files
        unique_columns: Column names that define uniqueness
        file_pattern: Glob pattern for CSV files (default: *.csv)
        
    Returns:
        Set of tuples representing unique keys from existing CSVs
        
    Example:
        If unique_columns = ['Record Number'], returns:
        {('REC001',), ('REC002',), ('REC003',), ...}
    """
    from fnmatch import fnmatch
    
    existing_keys = set()
    
    if not destination_dir.exists():
        logger.debug(f"Destination directory does not exist: {destination_dir}")
        return existing_keys
    
    csv_files = list(destination_dir.glob(file_pattern))
    
    # Filter out raw source files and temp files that shouldn't be compared against
    filtered_files = []
    for csv_file in csv_files:
        # Check if file matches any exclusion pattern
        should_exclude = any(
            fnmatch(csv_file.name, pattern) 
            for pattern in RAW_FILE_EXCLUSIONS
        )
        
        if not should_exclude:
            filtered_files.append(csv_file)
        else:
            logger.debug(f"Excluding raw source file from deduplication: {csv_file.name}")
    
    csv_files = filtered_files
    
    if not csv_files:
        logger.debug(f"No existing CSV files found in {destination_dir} (after filtering)")
        return existing_keys
    
    logger.info(f"Found {len(csv_files)} existing CSV files to check for duplicates")
    
    for csv_file in csv_files:
        try:
            # Read only the unique key columns for efficiency
            df = pd.read_csv(csv_file, usecols=unique_columns, dtype=str)
            
            # Convert rows to tuples and add to set
            for _, row in df.iterrows():
                key = tuple(str(row[col]).strip() for col in unique_columns)
                existing_keys.add(key)
                
            logger.debug(f"Loaded {len(df)} keys from {csv_file.name}")
            
        except Exception as e:
            logger.warning(f"Error reading {csv_file.name}: {e}")
            continue
    
    logger.info(f"Total unique keys in existing CSVs: {len(existing_keys)}")
    return existing_keys


def deduplicate_csv(
    new_csv_path: Path,
    destination_dir: Path,
    unique_key_columns: List[str],
    output_filename: Optional[str] = None,
    keep_original: bool = False
) -> Path:
    """
    Filter duplicate records from new CSV based on existing CSVs in old/ folder.
    
    This function implements a rotation strategy:
    1. Reads the newly scraped CSV
    2. Extracts unique keys from all existing CSVs in destination/old/
    3. Filters new CSV to keep only records NOT in old/ CSVs
    4. Saves deduplicated CSV to destination/new/ folder
    5. After successful DB load, call rotate_csv_archives() to move new/ → old/
    
    Args:
        new_csv_path: Path to newly scraped CSV (typically in temp/downloads)
        destination_dir: Base directory (will use old/ and new/ subdirectories)
        unique_key_columns: Column names that define record uniqueness
        output_filename: Custom output filename (if None, generates dated name)
        keep_original: If True, keeps the original temp CSV file
        
    Returns:
        Path to the deduplicated CSV file in destination/new/
        
    Raises:
        FileNotFoundError: If new_csv_path doesn't exist
        KeyError: If unique_key_columns not found in CSV
        
    Example:
        >>> deduplicate_csv(
        ...     new_csv_path=Path('downloads/violations.csv'),
        ...     destination_dir=Path('data/raw/violations'),
        ...     unique_key_columns=['Record Number'],
        ... )
        Path('data/raw/violations/new/violations_20260225.csv')
    """
    if not new_csv_path.exists():
        raise FileNotFoundError(f"New CSV file not found: {new_csv_path}")
    
    # Setup old/ and new/ subdirectories
    old_dir = destination_dir / "old"
    new_dir = destination_dir / "new"
    old_dir.mkdir(parents=True, exist_ok=True)
    new_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("CSV DEDUPLICATION")
    logger.info("=" * 60)
    logger.info(f"New CSV: {new_csv_path}")
    logger.info(f"Comparing against: {old_dir}")
    logger.info(f"Saving to: {new_dir}")
    logger.info(f"Unique keys: {unique_key_columns}")
    
    # Read new CSV
    try:
        new_df = pd.read_csv(new_csv_path)
        total_scraped = len(new_df)
        logger.info(f"Total records scraped: {total_scraped}")
    except Exception as e:
        logger.error(f"Failed to read new CSV: {e}")
        raise
    
    # Validate unique key columns exist
    missing_cols = [col for col in unique_key_columns if col not in new_df.columns]
    if missing_cols:
        raise KeyError(f"Unique key columns not found in CSV: {missing_cols}")
    
    # Get existing unique keys from old/ directory
    existing_keys = get_existing_unique_keys(
        destination_dir=old_dir,
        unique_columns=unique_key_columns
    )
    
    # Filter new DataFrame
    if existing_keys:
        # Create mask for new records
        def is_new_record(row):
            key = tuple(str(row[col]).strip() for col in unique_key_columns)
            return key not in existing_keys
        
        original_count = len(new_df)
        new_df = new_df[new_df.apply(is_new_record, axis=1)]
        filtered_count = original_count - len(new_df)
        
        logger.info(f"Duplicate records filtered: {filtered_count}")
        logger.info(f"New records to save: {len(new_df)}")
    else:
        logger.info("No existing CSVs found in old/ - all records are new")
    
    # Generate output filename if not provided
    if output_filename is None:
        # Extract base name and add date
        base_name = new_csv_path.stem.replace('_temp', '').replace('_download', '')
        today = datetime.now().strftime('%Y%m%d')
        output_filename = f"{base_name}_{today}.csv"
    
    # Save deduplicated CSV to new/ directory
    output_path = new_dir / output_filename
    new_df.to_csv(output_path, index=False)
    
    logger.info("=" * 60)
    logger.info("DEDUPLICATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total scraped:       {total_scraped:>6}")
    logger.info(f"  Duplicates filtered: {total_scraped - len(new_df):>6}")
    logger.info(f"  New records saved:   {len(new_df):>6}")
    logger.info(f"  Output file: {output_path}")
    logger.info("=" * 60)
    logger.info("NOTE: After successful DB insertion, call rotate_csv_archives()")
    
    # Remove original temp CSV if requested
    if not keep_original:
        try:
            new_csv_path.unlink()
            logger.debug(f"Removed temp CSV: {new_csv_path}")
        except Exception as e:
            logger.warning(f"Could not remove temp CSV: {e}")
    
    return output_path


def get_unique_keys_for_type(data_type: str) -> List[str]:
    """
    Get the unique key column names for a given data type.
    
    Args:
        data_type: Type of data ('violations', 'foreclosures', etc.)
        
    Returns:
        List of column names that define uniqueness
        
    Raises:
        ValueError: If data_type is not recognized
        
    Example:
        >>> get_unique_keys_for_type('violations')
        ['Record Number']
        >>> get_unique_keys_for_type('evictions')
        ['CaseNumber']
    """
    if data_type not in UNIQUE_KEY_MAP:
        raise ValueError(
            f"Unknown data type: {data_type}. "
            f"Available types: {list(UNIQUE_KEY_MAP.keys())}"
        )
    
    return UNIQUE_KEY_MAP[data_type]


def rotate_csv_archives(destination_dir: Path) -> bool:
    """
    Rotate CSV archives after successful database insertion.
    
    This function implements the rotation strategy:
    1. Delete all files in destination/old/
    2. Move all files from destination/new/ to destination/old/
    3. This prepares for the next scrape cycle
    
    Should be called ONLY after successful database insertion to ensure
    data is safely persisted before deleting old archives.
    
    Args:
        destination_dir: Base directory containing old/ and new/ subdirectories
        
    Returns:
        bool: True if rotation succeeded, False otherwise
        
    Example:
        >>> # After successful DB load:
        >>> rotate_csv_archives(Path('data/raw/violations'))
        True
    """
    import shutil
    
    old_dir = destination_dir / "old"
    new_dir = destination_dir / "new"
    
    try:
        logger.info("=" * 60)
        logger.info("CSV ARCHIVE ROTATION")
        logger.info("=" * 60)
        logger.info(f"Base directory: {destination_dir}")
        
        # Count files before rotation
        old_files = list(old_dir.glob("*.csv")) if old_dir.exists() else []
        new_files = list(new_dir.glob("*.csv")) if new_dir.exists() else []
        
        logger.info(f"Files in old/: {len(old_files)}")
        logger.info(f"Files in new/: {len(new_files)}")
        
        if not new_files:
            logger.warning("No files in new/ directory to rotate")
            return False
        
        # Step 1: Delete all files in old/
        if old_dir.exists():
            for old_file in old_files:
                old_file.unlink()
                logger.debug(f"Deleted: {old_file.name}")
            logger.info(f"✓ Deleted {len(old_files)} old archive(s)")
        else:
            old_dir.mkdir(parents=True, exist_ok=True)
            logger.info("✓ Created old/ directory")
        
        # Step 2: Move all files from new/ to old/
        moved_count = 0
        for new_file in new_files:
            dest_file = old_dir / new_file.name
            shutil.move(str(new_file), str(dest_file))
            logger.debug(f"Moved: {new_file.name} → old/")
            moved_count += 1
        
        logger.info(f"✓ Moved {moved_count} file(s) from new/ to old/")
        logger.info("=" * 60)
        logger.info("✓ CSV archive rotation completed successfully")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ CSV archive rotation failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False

