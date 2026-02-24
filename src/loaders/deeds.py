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
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load deeds from DataFrame.
        
        Args:
            df: DataFrame with columns: Instrument, Grantor, Grantee, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
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
                    deed_record = Deed(
                        property_id=property_record.id,
                        instrument_number=instrument,
                        grantor=row.get('Grantor'),
                        grantee=row.get('Grantee'),
                        record_date=self.parse_date(row.get('RecordDate')),
                        sale_price=self.parse_amount(row.get('Consideration')),
                        deed_type=row.get('Deed Type'),
                        doc_type=row.get('Doc Type'),
                        book_type=row.get('BookType'),
                        book_number=row.get('Book'),
                        page_number=row.get('Page'),
                        legal_description=row.get('Legal'),
                    )
                    
                    self.session.add(deed_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting deed {instrument}: {e}")
                    unmatched += 1
            else:
                logger.debug(f"No property match for deed: {instrument} (Grantor: {row.get('Grantor')})")
                unmatched += 1
        
        logger.info(f"Deeds: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
