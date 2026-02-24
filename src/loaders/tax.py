"""
Tax delinquency loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import TaxDelinquency

logger = logging.getLogger(__name__)


class TaxDelinquencyLoader(BaseLoader):
    """Loader for tax delinquency records."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load tax delinquencies from DataFrame.
        
        Args:
            df: DataFrame with columns: Account Number, Tax Yr, Owner Name, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading {len(df)} tax delinquencies")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            account_number = str(row['Account Number']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(TaxDelinquency, {'account_number': account_number}):
                logger.debug(f"Skipping duplicate tax record: {account_number}")
                skipped += 1
                continue
            
            # Match by parcel ID (account number)
            property_record = self.find_property_by_parcel_id(account_number)
            
            if property_record:
                try:
                    tax_record = TaxDelinquency(
                        property_id=property_record.id,
                        account_number=account_number,
                        tax_year=int(row['Tax Yr']) if pd.notna(row.get('Tax Yr')) else None,
                        delinquent_amount=self.parse_amount(row.get('total_amount_due')),
                        years_delinquent=int(row['years_delinquent_scraped']) if pd.notna(row.get('years_delinquent_scraped')) else None,
                        payment_plan_status=row.get('payment_plan_status'),
                        account_status=row.get('Account Status'),
                        cert_status=row.get('Cert Status'),
                        deed_status=row.get('Deed Status'),
                    )
                    
                    self.session.add(tax_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting tax record for {account_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for parcel: {account_number}")
                unmatched += 1
        
        logger.info(f"Tax delinquencies: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
