"""
Building permit loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import BuildingPermit

logger = logging.getLogger(__name__)


class BuildingPermitLoader(BaseLoader):
    """Loader for building permits."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load permits from DataFrame.
        
        Args:
            df: DataFrame with columns: Date, Record Number, Record Type, Address, Status, Expiration Date
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
            
        Note: BuildingPermit model has: permit_number, permit_type, issue_date, expire_date, status
        """
        logger.info(f"Loading {len(df)} building permits")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            record_number = str(row['Record Number']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(BuildingPermit, {'permit_number': record_number}):
                logger.debug(f"Skipping duplicate permit: {record_number}")
                skipped += 1
                continue
            
            # Match by address
            property_record = None
            if pd.notna(row.get('Address')):
                match_result = self.find_property_by_address(row['Address'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched permit by address (score: {score}%): {record_number}")
            
            if property_record:
                try:
                    # Handle NaN values
                    permit_type_val = row.get('Record Type')
                    if pd.isna(permit_type_val):
                        permit_type_val = None
                    
                    status_val = row.get('Status')
                    if pd.isna(status_val):
                        status_val = None
                    
                    permit_record = BuildingPermit(
                        property_id=property_record.id,
                        permit_number=record_number,
                        permit_type=permit_type_val,
                        status=status_val,
                        issue_date=self.parse_date(row.get('Date')),
                        expire_date=self.parse_date(row.get('Expiration Date'))
                    )
                    
                    if self.safe_add(permit_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building permit {record_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for permit: {record_number} at {row.get('Address')}")
                unmatched += 1
        
        logger.info(f"Building Permits: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
