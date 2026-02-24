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
            df: DataFrame with columns: Record Number, Address, Status, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
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
                    permit_record = BuildingPermit(
                        property_id=property_record.id,
                        permit_number=record_number,
                        permit_type=row.get('Type'),
                        description=row.get('Description'),
                        status=row.get('Status'),
                        issue_date=self.parse_date(row.get('Issue Date')),
                        finaled_date=self.parse_date(row.get('Finaled Date')),
                        valuation=self.parse_amount(row.get('Valuation')),
                    )
                    
                    self.session.add(permit_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting permit {record_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for permit: {record_number} at {row.get('Address')}")
                unmatched += 1
        
        logger.info(f"Building Permits: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
