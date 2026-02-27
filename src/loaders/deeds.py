"""
Deed (property transfer) loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import Deed

logger = logging.getLogger(__name__)


class DeedLoader(BaseLoader):
    """Loader for property deeds (ownership transfers)."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
        sample_mode: bool = False,
        sample_size: int = 10
    ) -> Tuple[int, int, int]:
        """
        Load deeds from DataFrame.
        
        Args:
            df: DataFrame with columns: Instrument, Grantor, Grantee, etc.
            skip_duplicates: Skip existing records
            sample_mode: If True, only load first N records for testing
            sample_size: Number of records to load when sample_mode=True
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        # Apply sampling if requested
        if sample_mode:
            original_count = len(df)
            df = df.head(sample_size)
            logger.info(f"🧪 SAMPLE MODE: Loading {len(df)} deeds (out of {original_count} total)")
        else:
            logger.info(f"Loading {len(df)} deeds")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            instrument = str(row['Instrument']).strip()
            
            # Check for duplicates
            if skip_duplicates:
                existing = self.session.query(Deed).filter(
                    Deed.instrument_number == instrument
                ).first()
                if existing:
                    logger.debug(f"Skipping duplicate deed: {instrument}")
                    skipped += 1
                    continue
            
            # Match by grantor (seller) name
            property_record = None
            if pd.notna(row.get('Grantor')):
                match_result = self.find_property_by_owner_name(row['Grantor'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched deed by grantor (score: {score}%): {instrument}")
            
            # Fallback: Try matching by grantee (buyer) name
            if not property_record and pd.notna(row.get('Grantee')):
                match_result = self.find_property_by_owner_name(row['Grantee'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched deed by grantee (score: {score}%): {instrument}")
            
            if property_record:
                try:
                    # Handle NaN values
                    grantor_val = row.get('Grantor')
                    if pd.isna(grantor_val):
                        grantor_val = None
                    
                    grantee_val = row.get('Grantee')
                    if pd.isna(grantee_val):
                        grantee_val = None
                    
                    deed_type_val = row.get('DocType')
                    if pd.isna(deed_type_val):
                        deed_type_val = None
                    
                    book_type_val = row.get('BookType')
                    if pd.isna(book_type_val):
                        book_type_val = None
                    
                    book_number_val = row.get('BookNum')
                    if pd.isna(book_number_val):
                        book_number_val = None
                    
                    page_number_val = row.get('PageNum')
                    if pd.isna(page_number_val):
                        page_number_val = None
                    
                    legal_desc_val = row.get('Legal')
                    if pd.isna(legal_desc_val):
                        legal_desc_val = None
                    
                    deed_record = Deed(
                        property_id=property_record.id,
                        instrument_number=instrument,
                        grantor=grantor_val,
                        grantee=grantee_val,
                        record_date=self.parse_date(row.get('RecordDate')),
                        sale_price=self.parse_amount(row.get('SalesPrice')),
                        deed_type=deed_type_val,
                        doc_type=None,  # Not in this CSV format
                        book_type=book_type_val,
                        book_number=book_number_val,
                        page_number=page_number_val,
                        legal_description=legal_desc_val
                    )
                    
                    if self.safe_add(deed_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building deed {instrument}: {e}")
                    unmatched += 1
            else:
                logger.debug(f"No property match for deed: {instrument} (Grantor: {row.get('Grantor')})")
                unmatched += 1
        
        logger.info(f"Deeds: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
