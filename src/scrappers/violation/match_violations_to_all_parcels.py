"""
Match violation addresses with all parcel records.

This script reads violation records and filters the all parcels CSV
to find properties with code violations, regardless of owner type.
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


def match_violations_to_all_parcels():
    """
    Find all parcel records that match violation addresses.
    """
    logger.info("=" * 60)
    logger.info("Matching Violations to All Parcels")
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
    
    # Read all parcels CSV in chunks to handle large file
    all_parcels_file = PROCESSED_DATA_DIR / "all_parcels.csv"
    
    if not all_parcels_file.exists():
        logger.error(f"All parcels file not found: {all_parcels_file}")
        return
    
    logger.info(f"Reading all parcels from: {all_parcels_file}")
    logger.info("Processing in chunks (file is large - 530k+ parcels)...")
    
    matched_parcels = []
    chunk_size = 10000
    total_processed = 0
    
    # Process all parcels in chunks
    for chunk_num, chunk in enumerate(pd.read_csv(all_parcels_file, chunksize=chunk_size), 1):
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
            matched_parcels.append(chunk_matches)
            logger.info(f"Chunk {chunk_num}: Found {len(chunk_matches)} matches (processed {total_processed:,} total records)")
        elif chunk_num % 10 == 0:
            logger.info(f"Processed {total_processed:,} records...")
    
    logger.info(f"Finished processing {total_processed:,} parcel records")
    
    if not matched_parcels:
        logger.warning("No matches found between violations and parcels")
        return
    
    # Combine all matches
    matched_df = pd.concat(matched_parcels, ignore_index=True)
    logger.info(f"Found {len(matched_df)} total matches")
    
    # Merge with violation details
    logger.info("Merging violation details with parcel data...")
    
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
    output_file = PROCESSED_DATA_DIR / "all_parcels_with_violations.csv"
    result_df.to_csv(output_file, index=False)
    
    size_mb = output_file.stat().st_size / (1024 ** 2)
    logger.info(f"Saved matched records to: {output_file} ({size_mb:.2f} MB)")
    
    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total violations: {len(violations_df)}")
    logger.info(f"Unique violation addresses: {len(violation_addresses)}")
    logger.info(f"Total parcels processed: {total_processed:,}")
    logger.info(f"Matched parcels with violations: {len(result_df)}")
    logger.info(f"Output file: {output_file}")
    logger.info("=" * 60)
    
    # Show sample of matches
    if len(result_df) > 0:
        logger.info("\nSample of matched records:")
        sample_cols = [col for col in ['folio', 'owner', address_col, 'violation_record_numbers', 'violation_opened_dates', 'violation_statuses'] if col in result_df.columns]
        logger.info(f"\n{result_df[sample_cols].head(10).to_string()}")
    
    # Show breakdown by owner type if mailing address is available
    if 'addr_1' in result_df.columns and address_col in result_df.columns:
        logger.info("\nOwner Type Breakdown:")
        result_df['is_absentee'] = result_df['addr_1'].astype(str).str.strip() != result_df[address_col].astype(str).str.strip()
        absentee_count = result_df['is_absentee'].sum()
        resident_count = len(result_df) - absentee_count
        logger.info(f"  Absentee Owners: {absentee_count}")
        logger.info(f"  Resident Owners: {resident_count}")


if __name__ == "__main__":
    match_violations_to_all_parcels()
