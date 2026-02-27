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
            
            # Strip the "A" prefix if present (tax CSV uses "A0000100000", database uses "0000010000")
            parcel_id = account_number.lstrip('A')
            
            # Match by parcel ID (account number)
            property_record = self.find_property_by_parcel_id(parcel_id)
            
            if not property_record:
                logger.warning(f"No property match for parcel: {account_number} (searched as: {parcel_id})")
                unmatched += 1
                continue
            
            # Check for duplicates - check if this property already has a tax record for this year
            tax_year = int(row['Tax Yr']) if pd.notna(row.get('Tax Yr')) else None
            if skip_duplicates and tax_year:
                existing = self.session.query(TaxDelinquency).filter(
                    TaxDelinquency.property_id == property_record.id,
                    TaxDelinquency.tax_year == tax_year
                ).first()
                if existing:
                    logger.debug(f"Skipping duplicate tax record: {account_number} for year {tax_year}")
                    skipped += 1
                    continue
            
            try:
                # Parse years delinquent
                years_delinquent_val = row.get('years_delinquent_scraped')
                if pd.notna(years_delinquent_val):
                    years_delinquent_val = int(years_delinquent_val)
                else:
                    years_delinquent_val = None
                
                # Combine cert and deed status into certificate_data
                cert_status = row.get('Cert Status', '')
                deed_status = row.get('Deed Status', '')
                certificate_data = None
                if pd.notna(cert_status) or pd.notna(deed_status):
                    parts = []
                    if pd.notna(cert_status) and cert_status != '-- None --':
                        parts.append(f"Cert: {cert_status}")
                    if pd.notna(deed_status) and deed_status != '-- None --':
                        parts.append(f"Deed: {deed_status}")
                    if parts:
                        certificate_data = ", ".join(parts)
                
                tax_record = TaxDelinquency(
                    property_id=property_record.id,
                    tax_year=tax_year,
                    years_delinquent=years_delinquent_val,
                    total_amount_due=self.parse_amount(row.get('total_amount_due')),
                    certificate_data=certificate_data,
                    deed_app_date=None,  # Not in CSV
                )
                
                if self.safe_add(tax_record):
                    matched += 1
                else:
                    unmatched += 1

            except Exception as e:
                logger.error(f"Error building tax record for {account_number}: {e}")
                unmatched += 1
        
        logger.info(f"Tax delinquencies: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
