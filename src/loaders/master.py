"""
Master property loader - inserts properties with owners and financials.
"""

import logging
from typing import Tuple, Optional
import re

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import Property, Owner, Financial

logger = logging.getLogger(__name__)


class MasterPropertyLoader(BaseLoader):
    """Loader for master property records (FOLIO/parcel data)."""
    
    @staticmethod
    def _is_valid_zip(value) -> Optional[str]:
        """Validate and extract ZIP code (5 or 5+4 format)."""
        if pd.isna(value):
            return None
        
        s = str(value).strip()
        # ZIP must be numeric or numeric with dash (5 digits or 5+4)
        if re.match(r'^\d{5}(-\d{4})?$', s):
            return s[:10]  # Max 10 chars
        
        # If it looks like a ZIP at the start, extract it
        match = re.match(r'^(\d{5}(-\d{4})?)', s)
        if match:
            return match.group(1)[:10]
        
        return None
    
    @staticmethod
    def _is_valid_small_number(value, max_value: float = 999.9) -> Optional[float]:
        """Validate number fits in NUMERIC(4,1) format (max 999.9)."""
        if pd.isna(value):
            return None
        
        try:
            num = float(value)
            # Check if it's within valid range
            if 0 <= num <= max_value:
                return num
        except (ValueError, TypeError):
            pass
        
        return None
    
    @staticmethod
    def _determine_absentee_status(property_address: Optional[str], 
                                    mailing_address: Optional[str]) -> Optional[str]:
        """Determine if owner is absentee by comparing addresses.
        
        Args:
            property_address: Property street address
            mailing_address: Owner mailing address
            
        Returns:
            'In-County' (owner-occupied), 'Out-of-County' (absentee), or None if cannot determine
        
        Note:
            Since mailing addresses in the data only contain street addresses (no city/state/ZIP),
            we use a simple comparison: matching address = owner-occupied, different = absentee.
            This is conservative - we mark different addresses as Out-of-County (8 pts) rather than
            Out-of-State (15 pts) to avoid over-scoring.
        """
        if not mailing_address or not property_address:
            return None
        
        # Normalize both addresses for comparison
        mail_clean = mailing_address.strip().upper()
        prop_clean = property_address.strip().upper()
        
        # Exact match = owner-occupied
        if mail_clean == prop_clean:
            return 'In-County'
        
        # Different addresses = absentee (investment property)
        # Use Out-of-County (8 pts) as conservative estimate
        return 'Out-of-County'
    
    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True,
        chunksize: int = 10000
    ) -> Tuple[int, int, int]:
        """
        Load data from CSV file with validation, processing in chunks.
        
        Args:
            csv_path: Path to CSV file
            skip_duplicates: Skip existing records
            chunksize: Number of rows to read per chunk from CSV
            
        Returns:
            Tuple of (inserted, 0, skipped)
        """
        logger.info(f"Loading data from: {csv_path}")
        
        total_inserted = 0
        total_unmatched = 0
        total_skipped = 0
        
        # Process CSV in chunks to handle large files efficiently
        chunk_num = 0
        for chunk in pd.read_csv(csv_path, dtype={'folio': str}, chunksize=chunksize):
            chunk_num += 1
            # Normalize column names to uppercase for consistency
            chunk.columns = chunk.columns.str.upper()
            logger.info(f"Processing chunk {chunk_num} ({len(chunk)} rows)")
            
            inserted, unmatched, skipped = self.load_from_dataframe(chunk, skip_duplicates, batch_size=1000)
            total_inserted += inserted
            total_unmatched += unmatched
            total_skipped += skipped
        
        logger.info(f"CSV loading complete: {total_inserted} inserted, {total_skipped} skipped")
        return total_inserted, total_unmatched, total_skipped

        
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
        batch_size: int = 1000
    ) -> Tuple[int, int, int]:
        """
        Load master properties from DataFrame with validation.
        
        Skips rows with invalid/missing critical fields (FOLIO, OWNER).
        Uses None for invalid non-critical fields.
        
        Args:
            df: DataFrame with property data
            skip_duplicates: Skip existing properties
            batch_size: Number of records to commit per batch
            
        Returns:
            Tuple of (inserted, 0, skipped)
        """
        logger.info(f"Loading {len(df)} master properties in batches of {batch_size}")
        
        inserted = 0
        skipped = 0
        skip_reasons = {'no_folio': 0, 'no_owner': 0, 'invalid_owner': 0, 'duplicate': 0, 'error': 0}
        
        for idx, row in df.iterrows():
            # FOLIO is critical - skip if missing/invalid
            parcel_id = str(row.get('FOLIO', '')).strip()
            if not parcel_id or parcel_id == 'nan':
                skip_reasons['no_folio'] += 1
                skipped += 1
                continue
            
            # OWNER is critical - skip if looks like an address or clearly wrong
            owner_name = str(row.get('OWNER', '')).strip()
            if not owner_name or owner_name == 'nan':
                skip_reasons['no_owner'] += 1
                skipped += 1
                continue
            
            # Skip if owner name looks invalid:
            # - Starts with digits (likely an address)
            # - Is a single word with no spaces and all caps (likely a city name like "ODESSA")
            # - Contains typical address indicators
            if (re.match(r'^\d', owner_name) or  # Starts with number
                (len(owner_name.split()) == 1 and owner_name.isupper() and len(owner_name) < 15) or  # Single all-caps short word
                any(indicator in owner_name.upper() for indicator in [' STREET ', ' ST ', ' AVE ', ' AVENUE ', ' ROAD ', ' RD ', ' LANE ', ' LN ', ' DRIVE ', ' DR ', 'W COUNTY', 'E COUNTY', 'N COUNTY', 'S COUNTY'])):
                skip_reasons['invalid_owner'] += 1
                skipped += 1
                continue
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(Property, {'parcel_id': parcel_id}):
                skip_reasons['duplicate'] += 1
                skipped += 1
                continue
            
            try:
                # Safely extract fields - use None if invalid
                site_zip = self._is_valid_zip(row.get('SITE_ZIP'))
                beds = self._is_valid_small_number(row.get('tBEDS'))
                baths = self._is_valid_small_number(row.get('tBATHS'))
                
                # Extract string fields - skip if they look wrong
                site_addr_raw = str(row.get('SITE_ADDR', '')) if pd.notna(row.get('SITE_ADDR')) else ''
                site_addr = site_addr_raw[:255] if site_addr_raw and len(site_addr_raw) > 5 else None
                
                site_city_raw = str(row.get('SITE_CITY', '')) if pd.notna(row.get('SITE_CITY')) else ''
                # Skip if site_city looks like a legal description (too long or has degrees/minutes)
                site_city = None
                if site_city_raw and len(site_city_raw) < 100 and 'DEG' not in site_city_raw.upper() and 'SEC' not in site_city_raw.upper():
                    site_city = site_city_raw[:100]
                
                prop_type = str(row.get('TYPE', ''))[:50] if pd.notna(row.get('TYPE')) else None
                
                # Build legal description
                legal_parts = []
                for col in ['LEGAL1', 'LEGAL2', 'LEGAL3', 'LEGAL4']:
                    val = row.get(col)
                    if pd.notna(val) and str(val).strip():
                        legal_parts.append(str(val).strip())
                legal_desc = ' '.join(legal_parts) if legal_parts else None
                
                # Create property
                property_record = Property(
                    parcel_id=parcel_id,
                    address=site_addr,
                    city=site_city,
                    state='FL',
                    zip=site_zip,
                    jurisdiction='Hillsborough',
                    property_type=prop_type,
                    legal_description=legal_desc,
                    lot_size=self.parse_amount(row.get('ACREAGE')),
                    year_built=None,
                    beds=beds,
                    baths=baths,
                    sq_ft=self.parse_amount(row.get('HEAT_AR')),
                )
                
                # Create owner
                owner_name_clean = owner_name[:255]
                
                # Build mailing address - include all non-empty fields
                mailing_parts = []
                for col in ['ADDR_1', 'CITY', 'STATE', 'ZIP']:
                    val = str(row.get(col, '')).strip() if pd.notna(row.get(col)) else ''
                    # Include if not empty and not too long (allow short fields like "FL")
                    if val and len(val) < 200:
                        mailing_parts.append(val)
                
                mailing_addr = ', '.join(mailing_parts)[:255] if mailing_parts else None
                
                # Determine absentee status by comparing addresses
                absentee_status = self._determine_absentee_status(
                    property_address=address,
                    mailing_address=mailing_addr
                )
                
                owner_record = Owner(
                    property=property_record,
                    owner_name=owner_name_clean,
                    mailing_address=mailing_addr,
                    owner_type='Individual',
                    absentee_status=absentee_status,
                )
                
                # Create financial
                financial_record = Financial(
                    property=property_record,
                    assessed_value_mkt=self.parse_amount(row.get('ASD_VAL')),
                    assessed_value_tax=self.parse_amount(row.get('TAX_VAL')),
                    annual_tax_amount=None,
                )
                
                self.session.add_all([property_record, owner_record, financial_record])
                inserted += 1
                
                # Commit in batches to avoid memory issues
                if inserted % batch_size == 0:
                    self.session.commit()
                    logger.info(f"Progress: {inserted} inserted, {skipped} skipped")
                
            except Exception as e:
                logger.error(f"Error inserting property {parcel_id}: {e}")
                logger.debug(f"Row data: {dict(row)}")
                skip_reasons['error'] += 1
                skipped += 1
                continue
        
        # Commit any remaining records
        if inserted % batch_size != 0:
            self.session.commit()
        
        logger.info(f"Master properties: {inserted} inserted, {skipped} skipped")
        logger.info(f"Skip reasons: {skip_reasons}")
        return inserted, 0, skipped
