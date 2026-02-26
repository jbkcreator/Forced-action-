"""
Code violation loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import CodeViolation

logger = logging.getLogger(__name__)


class ViolationLoader(BaseLoader):
    """Loader for code enforcement violations."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load violations from DataFrame.
        
        Args:
            df: DataFrame with columns: Record Number, Address, Status, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading {len(df)} violations")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            record_number = str(row['Record Number']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(CodeViolation, {'record_number': record_number}):
                logger.debug(f"Skipping duplicate violation: {record_number}")
                skipped += 1
                continue
            
            # Match by address
            property_record = None
            if pd.notna(row.get('Address')):
                match_result = self.find_property_by_address(row['Address'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched violation by address (score: {score}%): {record_number}")
            
            if property_record:
                try:
                    # Map CSV columns to database fields:
                    # CSV 'Record Number' → record_number
                    # CSV 'Record Type' → violation_type  
                    # CSV 'Description' → description
                    # CSV 'Date' → opened_date
                    # CSV 'Status' → status
                    
                    # Handle NaN values from pandas - convert to None
                    description_val = row.get('Description')
                    if pd.isna(description_val):
                        description_val = None
                    
                    violation_type_val = row.get('Record Type')
                    if pd.isna(violation_type_val):
                        violation_type_val = None
                    
                    status_val = row.get('Status')
                    if pd.isna(status_val):
                        status_val = None
                    
                    violation_record = CodeViolation(
                        property_id=property_record.id,
                        record_number=record_number,
                        violation_type=violation_type_val,
                        description=description_val,
                        opened_date=self.parse_date(row.get('Date')),
                        status=status_val,
                        severity_tier=None,
                        fine_amount=None,
                        is_lien=False,
                    )
                    
                    self.session.add(violation_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting violation {record_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for violation: {record_number} at {row.get('Address')}")
                unmatched += 1
        
        logger.info(f"Violations: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
