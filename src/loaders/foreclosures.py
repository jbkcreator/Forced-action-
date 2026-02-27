"""
Foreclosure loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import Foreclosure

logger = logging.getLogger(__name__)


class ForeclosureLoader(BaseLoader):
    """Loader for foreclosure records."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load foreclosures from DataFrame.
        
        Args:
            df: DataFrame with columns: Case Number, Parcel ID, Property Address, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading {len(df)} foreclosures")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            case_number = str(row['Case Number']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(Foreclosure, {'case_number': case_number}):
                logger.debug(f"Skipping duplicate foreclosure: {case_number}")
                skipped += 1
                continue
            
            # Try parcel ID first
            property_record = None
            if pd.notna(row.get('Parcel ID')):
                property_record = self.find_property_by_parcel_id(row['Parcel ID'])
            
            # Fallback to address matching
            if not property_record and pd.notna(row.get('Property Address')):
                match_result = self.find_property_by_address(row['Property Address'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched foreclosure by address (score: {score}%): {case_number}")
            
            if property_record:
                try:
                    # Handle NaN values
                    plaintiff_val = row.get('Plaintiff')
                    if pd.isna(plaintiff_val):
                        plaintiff_val = None
                    
                    # Parse auction date from "Auction Start Date/Time"
                    auction_date_val = None
                    if pd.notna(row.get('Auction Start Date/Time')):
                        auction_date_val = self.parse_date(row.get('Auction Start Date/Time'))
                    
                    foreclosure_record = Foreclosure(
                        property_id=property_record.id,
                        case_number=case_number,
                        plaintiff=plaintiff_val,
                        filing_date=None,  # Not in CSV
                        lis_pendens_date=None,  # Not in CSV
                        judgment_amount=self.parse_amount(row.get('Judgment Amount')),
                        auction_date=auction_date_val,
                    )
                    
                    if self.safe_add(foreclosure_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building foreclosure {case_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for foreclosure: {case_number}")
                unmatched += 1
        
        logger.info(f"Foreclosures: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
