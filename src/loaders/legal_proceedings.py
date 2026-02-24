"""
Legal proceedings loaders (Probate, Evictions, Bankruptcy).
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import LegalProceeding

logger = logging.getLogger(__name__)


class ProbateLoader(BaseLoader):
    """Loader for probate court cases."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load probate cases from DataFrame.
        
        Args:
            df: DataFrame with columns: CaseNumber, PartyAddress, FilingDate, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading probate cases from {len(df)} rows")
        
        # Group by case number (multiple rows per case)
        grouped = df.groupby('CaseNumber')
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for case_number, group in grouped:
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': case_number}):
                logger.debug(f"Skipping duplicate probate case: {case_number}")
                skipped += 1
                continue
            
            # Get decedent info (first row with decedent)
            decedent_row = group[group['PartyType'] == 'Decedent'].iloc[0] if not group[group['PartyType'] == 'Decedent'].empty else group.iloc[0]
            
            # Match by address
            property_record = None
            if pd.notna(decedent_row.get('PartyAddress')):
                match_result = self.find_property_by_address(decedent_row['PartyAddress'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched probate by address (score: {score}%): {case_number}")
            
            # Fallback to owner name
            if not property_record and pd.notna(decedent_row.get('LastName/CompanyName')):
                full_name = f"{decedent_row.get('FirstName', '')} {decedent_row.get('MiddleName', '')} {decedent_row.get('LastName/CompanyName', '')}".strip()
                match_result = self.find_property_by_owner_name(full_name)
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched probate by name (score: {score}%): {case_number}")
            
            if property_record:
                try:
                    # Get decedent name
                    decedent_name = f"{decedent_row.get('FirstName', '')} {decedent_row.get('MiddleName', '')} {decedent_row.get('LastName/CompanyName', '')}".strip()
                    beneficiary = group[group['PartyType'] == 'Beneficiary']['LastName/CompanyName'].iloc[0] if not group[group['PartyType'] == 'Beneficiary'].empty else None
                    
                    probate_record = LegalProceeding(
                        property_id=property_record.id,
                        record_type='Probate',
                        case_number=case_number,
                        filing_date=self.parse_date(decedent_row.get('FilingDate')),
                        case_status=decedent_row.get('Title'),
                        associated_party=decedent_name,
                        secondary_party=beneficiary,
                        meta_data={
                            'case_type': decedent_row.get('CaseTypeDescription'),
                            'party_address': decedent_row.get('PartyAddress')
                        }
                    )
                    
                    self.session.add(probate_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting probate case {case_number}: {e}")
                    unmatched += 1
            else:
                logger.debug(f"No property match for probate case: {case_number}")
                unmatched += 1
        
        logger.info(f"Probate: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped


class EvictionLoader(BaseLoader):
    """Loader for eviction court cases."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load evictions from DataFrame.
        
        Args:
            df: DataFrame with columns: CaseNumber, PartyAddress, FilingDate, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading evictions from {len(df)} rows")
        
        # Group by case number (plaintiff + defendant rows)
        grouped = df.groupby('CaseNumber')
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for case_number, group in grouped:
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': case_number}):
                logger.debug(f"Skipping duplicate eviction: {case_number}")
                skipped += 1
                continue
            
            # Get defendant info (has address)
            defendant_row = group[group['PartyType'] == 'Defendant'].iloc[0] if not group[group['PartyType'] == 'Defendant'].empty else group.iloc[0]
            
            # Match by address
            property_record = None
            if pd.notna(defendant_row.get('PartyAddress')):
                match_result = self.find_property_by_address(defendant_row['PartyAddress'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched eviction by address (score: {score}%): {case_number}")
            
            if property_record:
                try:
                    # Get plaintiff and defendant names
                    plaintiff_name = group[group['PartyType'] == 'Plaintiff']['LastName/CompanyName'].iloc[0] if not group[group['PartyType'] == 'Plaintiff'].empty else None
                    defendant_name = f"{defendant_row.get('FirstName', '')} {defendant_row.get('MiddleName', '')} {defendant_row.get('LastName/CompanyName', '')}".strip()
                    
                    eviction_record = LegalProceeding(
                        property_id=property_record.id,
                        record_type='Eviction',
                        case_number=case_number,
                        filing_date=self.parse_date(defendant_row.get('FilingDate')),
                        case_status=defendant_row.get('Title'),
                        associated_party=defendant_name,
                        secondary_party=plaintiff_name,
                        meta_data={
                            'case_type': defendant_row.get('CaseTypeDescription'),
                            'party_address': defendant_row.get('PartyAddress')
                        }
                    )
                    
                    self.session.add(eviction_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting eviction {case_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for eviction: {case_number} at {defendant_row.get('PartyAddress')}")
                unmatched += 1
        
        logger.info(f"Evictions: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped


class BankruptcyLoader(BaseLoader):
    """Loader for bankruptcy court cases."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load bankruptcy cases from DataFrame.
        
        Args:
            df: DataFrame with columns: Docket Number, Lead Name, Date Filed, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
            
        Note: Bankruptcy records have NO ADDRESS, only names.
        Match rate will be very low (10-30%) due to name-only matching.
        """
        logger.info(f"Loading {len(df)} bankruptcy cases")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            docket_number = str(row['Docket Number']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': docket_number}):
                logger.debug(f"Skipping duplicate bankruptcy: {docket_number}")
                skipped += 1
                continue
            
            # Match by owner name (only option available)
            property_record = None
            if pd.notna(row.get('Lead Name')):
                match_result = self.find_property_by_owner_name(row['Lead Name'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched bankruptcy by name (score: {score}%): {docket_number}")
            
            if property_record:
                try:
                    bankruptcy_record = LegalProceeding(
                        property_id=property_record.id,
                        record_type='Bankruptcy',
                        case_number=docket_number,
                        filing_date=self.parse_date(row.get('Date Filed')),
                        associated_party=row.get('Lead Name'),
                        meta_data={
                            'case_type': row.get('Case Type'),
                            'division': row.get('Division'),
                            'court_id': row.get('Court ID')
                        }
                    )
                    
                    self.session.add(bankruptcy_record)
                    matched += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting bankruptcy {docket_number}: {e}")
                    unmatched += 1
            else:
                logger.debug(f"No property match for bankruptcy: {docket_number} (Name: {row.get('Lead Name')})")
                unmatched += 1
        
        logger.info(f"Bankruptcy: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        logger.warning("Bankruptcy match rate is low (name-only matching). Consider adding address enrichment.")
        return matched, unmatched, skipped
