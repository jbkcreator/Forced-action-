"""
Lien and judgment loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import LegalAndLien

logger = logging.getLogger(__name__)


class LienLoader(BaseLoader):
    """Loader for liens and judgments."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load liens/judgments from DataFrame.
        
        Args:
            df: DataFrame with columns: Instrument, Grantor, Grantee, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading {len(df)} liens/judgments")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            instrument = str(row['Instrument']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalAndLien, {'instrument_number': instrument}):
                logger.debug(f"Skipping duplicate lien: {instrument}")
                skipped += 1
                continue
            
            # Match by owner name (Grantor)
            property_record = None
            if pd.notna(row.get('Grantor')):
                match_result = self.find_property_by_owner_name(row

['Grantor'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched lien by name (score: {score}%): {instrument}")
            
            if property_record:
                try:
                    # Determine record type
                    doc_type = str(row.get('document_type', ''))
                    if 'JUDGMENT' in doc_type.upper() or 'CERTIFIED' in doc_type.upper():
                        record_type = 'Judgment'
                    else:
                        record_type = 'Lien'
                    
                    lien_record = LegalAndLien(
                        property_id=property_record.id,
                        record_type=record_type,
                        instrument_number=instrument,
                        creditor=row.get('Grantee'),
                        debtor=row.get('Grantor'),
                        amount=self.parse_amount(row.get('Filing Amt')),
                        filing_date=self.parse_date(row.get('RecordDate')),
                        book_type=row.get('BookType'),
                        book_number=row.get('Book'),
                        page_number=row.get('Page'),
                        document_type=row.get('document_type'),
                        legal_description=row.get('Legal'),
                    )
                    
                    self.session.add(lien_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting lien {instrument}: {e}")
                    unmatched += 1
            else:
                logger.debug(f"No property match for lien: {instrument} (Grantor: {row.get('Grantor')})")
                unmatched += 1
        
        logger.info(f"Liens/Judgments: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
