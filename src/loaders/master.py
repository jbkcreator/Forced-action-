"""
Master property loader - inserts properties with owners and financials.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import Property, Owner, Financial

logger = logging.getLogger(__name__)


class MasterPropertyLoader(BaseLoader):
    """Loader for master property records (FOLIO/parcel data)."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load master properties from DataFrame.
        
        Args:
            df: DataFrame with columns: FOLIO, OWNER, SITE_ADDR, etc.
            skip_duplicates: Skip existing properties
            
        Returns:
            Tuple of (inserted, 0, skipped)
        """
        logger.info(f"Loading {len(df)} master properties")
        
        inserted = 0
        skipped = 0
        
        for _, row in df.iterrows():
            parcel_id = str(row['FOLIO']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(Property, {'parcel_id': parcel_id}):
                logger.debug(f"Skipping duplicate property: {parcel_id}")
                skipped += 1
                continue
            
            try:
                # Create property
                property_record = Property(
                    parcel_id=parcel_id,
                    address=row.get('SITE_ADDR'),
                    city=row.get('SITE_CITY'),
                    state='FL',
                    zip_code=row.get('SITE_ZIP'),
                    county='Hillsborough',
                    property_type=row.get('TYPE'),
                    legal_description=f"{row.get('LEGAL1', '')} {row.get('LEGAL2', '')} {row.get('LEGAL3', '')} {row.get('LEGAL4', '')}".strip(),
                    subdivision=row.get('SUB'),
                    lot_size=self.parse_amount(row.get('ACREAGE')),
                    year_built=None,
                    bedrooms=int(row['tBEDS']) if pd.notna(row.get('tBEDS')) else None,
                    bathrooms=float(row['tBATHS']) if pd.notna(row.get('tBATHS')) else None,
                    square_footage=self.parse_amount(row.get('HEAT_AR')),
                    stories=int(row['tSTORIES']) if pd.notna(row.get('tSTORIES')) else None,
                    units=int(row['tUNITS']) if pd.notna(row.get('tUNITS')) else None,
                )
                
                # Create owner
                owner_record = Owner(
                    property=property_record,
                    name=row.get('OWNER'),
                    mailing_address=row.get('ADDR_1'),
                    mailing_city=row.get('CITY'),
                    mailing_state=row.get('STATE'),
                    mailing_zip=row.get('ZIP'),
                    owner_type='individual',
                    is_absentee=(row.get('SITE_ADDR') != row.get('ADDR_1')),
                )
                
                # Create financial
                financial_record = Financial(
                    property=property_record,
                    assessed_value=self.parse_amount(row.get('ASD_VAL')),
                    market_value=self.parse_amount(row.get('TAX_VAL')),
                    land_value=self.parse_amount(row.get('LAND')),
                    building_value=self.parse_amount(row.get('BLDG')),
                    annual_tax_amount=None,
                )
                
                self.session.add_all([property_record, owner_record, financial_record])
                inserted += 1
                
            except Exception as e:
                logger.error(f"Error inserting property {parcel_id}: {e}")
                continue
        
        logger.info(f"Master properties: {inserted} inserted, {skipped} skipped")
        return inserted, 0, skipped
