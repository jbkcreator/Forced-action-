"""
Match violation addresses with absentee owner records.

This script reads violation records and filters the absentee owners CSV
to find properties with code violations where the owner is an absentee owner.
"""

import pandas as pd
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.constants import REFERENCE_DATA_DIR, PROCESSED_DATA_DIR
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def normalize_address(address: str) -> str:
    """
    Normalize address for comparison.
    
    Args:
        address: Raw address string
        
    Returns:
        Normalized address (lowercase, stripped, standardized)
    """
    if pd.isna(address) or not address:
        return ""
    
    addr = str(address).lower().strip()
    
    # Standardize common abbreviations
    replacements = {
        ' street': ' st',
        ' drive': ' dr',
        ' road': ' rd',
        ' avenue': ' ave',
        ' lane': ' ln',
        ' circle': ' cir',
        ' boulevard': ' blvd',
        ' court': ' ct',
        ' place': ' pl',
        ' way': ' wy',
        'florida': 'fl',
    }
    
    for old, new in replacements.items():
        addr = addr.replace(old, new)
    
    # Remove extra spaces
    addr = ' '.join(addr.split())
    
    return addr


def match_violations_to_absentee_owners():
    """
    Find absentee owner records that match violation addresses.
    """
    logger.info("=" * 60)
    logger.info("Matching Violations to Absentee Owners")
    logger.info("=" * 60)
    
    # Read violations CSV
    violations_file = REFERENCE_DATA_DIR / "hcfl_code_enforcement_violations.csv"
    
    if not violations_file.exists():
        logger.error(f"Violations file not found: {violations_file}")
        return
    
    logger.info(f"Reading violations from: {violations_file}")
    violations_df = pd.read_csv(violations_file)
    logger.info(f"Loaded {len(violations_df)} violation records")
    
    # Extract and normalize addresses
    violations_df['normalized_address'] = violations_df['Address'].apply(normalize_address)
    
    # Get unique addresses (excluding empty ones)
    violation_addresses = set(violations_df[violations_df['normalized_address'] != '']['normalized_address'])
    logger.info(f"Found {len(violation_addresses)} unique violation addresses")
    
    # Read absentee owners CSV in chunks to handle large file
    absentee_file = PROCESSED_DATA_DIR / "absentee_owners.csv"
    
    if not absentee_file.exists():
        logger.error(f"Absentee owners file not found: {absentee_file}")
        return
    
    logger.info(f"Reading absentee owners from: {absentee_file}")
    logger.info("Processing in chunks (file is large)...")
    
    matched_owners = []
    chunk_size = 10000
    total_processed = 0
    
    # Process absentee owners in chunks
    for chunk_num, chunk in enumerate(pd.read_csv(absentee_file, chunksize=chunk_size), 1):
        total_processed += len(chunk)
        
        # Check if chunk has address column (try common variations)
        address_col = None
        for col in ['site_addr', 'Address', 'address', 'PropertyAddress', 'PROPERTY_ADDRESS', 'SITE_ADDRESS']:
            if col in chunk.columns:
                address_col = col
                break
        
        if not address_col:
            if chunk_num == 1:
                logger.warning(f"Could not find address column. Available columns: {chunk.columns.tolist()}")
            continue
        
        # Combine site_addr with site_city and site_zip to match violation format
        # Violation format: "10064 Dolphin Gull Cir, Thonotosassa FL 33592"
        def build_full_address(row):
            addr = str(row.get(address_col, '')) if not pd.isna(row.get(address_col)) else ''
            city = str(row.get('site_city', '')) if not pd.isna(row.get('site_city')) else ''
            zip_code = str(row.get('site_zip', '')) if not pd.isna(row.get('site_zip')) else ''
            
            if addr:
                full = addr
                if city:
                    full += f", {city} FL"
                if zip_code:
                    full += f" {zip_code}"
                return full
            return ''
        
        chunk['full_address'] = chunk.apply(build_full_address, axis=1)
        
        # Normalize addresses in chunk
        chunk['normalized_address'] = chunk['full_address'].apply(normalize_address)
        
        # Find matches
        chunk_matches = chunk[chunk['normalized_address'].isin(violation_addresses)]
        
        if len(chunk_matches) > 0:
            matched_owners.append(chunk_matches)
            logger.info(f"Chunk {chunk_num}: Found {len(chunk_matches)} matches (processed {total_processed:,} total records)")
        elif chunk_num % 10 == 0:
            logger.info(f"Processed {total_processed:,} records...")
    
    logger.info(f"Finished processing {total_processed:,} absentee owner records")
    
    if not matched_owners:
        logger.warning("No matches found between violations and absentee owners")
        return
    
    # Combine all matches
    matched_df = pd.concat(matched_owners, ignore_index=True)
    logger.info(f"Found {len(matched_df)} total matches")
    
    # Merge with violation details
    logger.info("Merging violation details with absentee owner data...")
    
    # Create a mapping of normalized address to violation details
    violation_details = violations_df.groupby('normalized_address').agg({
        'Record Number': lambda x: ', '.join(x.fillna('').astype(str)),
        'Date': lambda x: ', '.join(x.fillna('').astype(str)),
        'Status': lambda x: ', '.join(x.fillna('').astype(str)),
        'Record Type': lambda x: ', '.join(x.fillna('').astype(str)),
        'Description': lambda x: ', '.join(x.fillna('').astype(str))
    }).reset_index()
    
    violation_details.columns = [
        'normalized_address',
        'violation_record_numbers',
        'violation_opened_dates',
        'violation_statuses',
        'violation_types',
        'violation_descriptions'
    ]
    
    # Merge
    result_df = matched_df.merge(violation_details, on='normalized_address', how='left')
    
    # Save results
    output_file = PROCESSED_DATA_DIR / "absentee_owners_with_violations.csv"
    result_df.to_csv(output_file, index=False)
    
    size_mb = output_file.stat().st_size / (1024 ** 2)
    logger.info(f"Saved matched records to: {output_file} ({size_mb:.2f} MB)")
    
    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total violations: {len(violations_df)}")
    logger.info(f"Unique violation addresses: {len(violation_addresses)}")
    logger.info(f"Total absentee owners processed: {total_processed:,}")
    logger.info(f"Matched absentee owners with violations: {len(result_df)}")
    logger.info(f"Output file: {output_file}")
    logger.info("=" * 60)
    
    # Show sample of matches
    if len(result_df) > 0:
        logger.info("\nSample of matched records:")
        sample_cols = [col for col in ['Address', address_col, 'violation_record_numbers', 'violation_opened_dates', 'violation_statuses'] if col in result_df.columns]
        logger.info(f"\n{result_df[sample_cols].head(10).to_string()}")


if __name__ == "__main__":
    match_violations_to_absentee_owners()
