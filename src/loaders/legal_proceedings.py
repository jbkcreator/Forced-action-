"""
Legal proceedings loaders (Probate, Evictions, Bankruptcy).

Fixes:
- ProbateLoader now sanitizes NaN -> None for all JSON/meta fields (matches Eviction/Bankruptcy behavior)
- Also sanitizes beneficiary/plaintiff fields and other nullable columns consistently
"""

import logging
from typing import Tuple, Optional

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import LegalProceeding

logger = logging.getLogger(__name__)


def _none_if_nan(value):
    """Convert pandas/NumPy NaN (and None-like) to None."""
    return None if pd.isna(value) else value


def _safe_str(value) -> Optional[str]:
    """Return a stripped string or None if empty/NaN."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


class ProbateLoader(BaseLoader):
    """Loader for probate court cases."""

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
        sample_mode: bool = False,
        sample_size: int = 10
    ) -> Tuple[int, int, int]:
        """
        Load probate cases from DataFrame.

        Args:
            df: DataFrame with columns: CaseNumber, PartyAddress, FilingDate, etc.
            skip_duplicates: Skip existing records
            sample_mode: If True, only load first N rows for testing (includes multiple rows per case)
            sample_size: Number of rows to load when sample_mode=True

        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        # Apply sampling if requested
        if sample_mode:
            original_count = len(df)
            df = df.head(sample_size)
            logger.info(f"🧪 SAMPLE MODE: Loading {len(df)} probate rows (out of {original_count} total)")
        else:
            logger.info(f"Loading probate cases from {len(df)} rows")

        # Group by case number (multiple rows per case)
        grouped = df.groupby('CaseNumber')

        matched = 0
        unmatched = 0
        skipped = 0

        for case_number, group in grouped:
            case_number = _safe_str(case_number) or str(case_number).strip()

            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': case_number}):
                logger.debug(f"Skipping duplicate probate case: {case_number}")
                skipped += 1
                continue

            # Get decedent info (first row with decedent)
            if 'PartyType' in group.columns:
                decedent_candidates = group[group['PartyType'] == 'Decedent']
                decedent_row = decedent_candidates.iloc[0] if not decedent_candidates.empty else group.iloc[0]
            else:
                decedent_row = group.iloc[0]

            # Match by address
            property_record = None
            party_address_val = _none_if_nan(decedent_row.get('PartyAddress'))

            if party_address_val:
                match_result = self.find_property_by_address(str(party_address_val))
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched probate by address (score: {score}%): {case_number}")

            # Fallback to owner name
            if not property_record:
                last_name = _none_if_nan(decedent_row.get('LastName/CompanyName'))
                if last_name:
                    full_name = " ".join(
                        [str(_none_if_nan(decedent_row.get('FirstName')) or "").strip(),
                         str(_none_if_nan(decedent_row.get('MiddleName')) or "").strip(),
                         str(last_name).strip()]
                    ).strip()
                    full_name = " ".join(full_name.split())
                    if full_name:
                        match_result = self.find_property_by_owner_name(full_name)
                        if match_result:
                            property_record, score = match_result
                            logger.info(f"Matched probate by name (score: {score}%): {case_number}")

            if property_record:
                try:
                    # Decedent name
                    first = _none_if_nan(decedent_row.get('FirstName')) or ""
                    middle = _none_if_nan(decedent_row.get('MiddleName')) or ""
                    last = _none_if_nan(decedent_row.get('LastName/CompanyName')) or ""
                    decedent_name = " ".join([str(first).strip(), str(middle).strip(), str(last).strip()]).strip()
                    decedent_name = " ".join(decedent_name.split()) or None

                    # Beneficiary (optional)
                    beneficiary = None
                    if 'PartyType' in group.columns and 'LastName/CompanyName' in group.columns:
                        ben_rows = group[group['PartyType'] == 'Beneficiary']
                        if not ben_rows.empty:
                            beneficiary = _safe_str(ben_rows['LastName/CompanyName'].iloc[0])
                    beneficiary = _none_if_nan(beneficiary)

                    # Other nullable fields
                    case_status_val = _none_if_nan(decedent_row.get('Title'))
                    case_type_val = _none_if_nan(decedent_row.get('CaseTypeDescription'))
                    party_address_val = _none_if_nan(decedent_row.get('PartyAddress'))

                    probate_record = LegalProceeding(
                        property_id=property_record.id,
                        record_type='Probate',
                        case_number=case_number,
                        filing_date=self.parse_date(decedent_row.get('FilingDate')),
                        case_status=case_status_val,
                        associated_party=decedent_name,
                        secondary_party=beneficiary,
                        meta_data={
                            'case_type': case_type_val,
                            'party_address': party_address_val
                        }
                    )

                    # Optional: enforce strict JSON validity (catches NaN early if it ever slips in)
                    # import json
                    # json.dumps(probate_record.meta_data, allow_nan=False)

                    if self.safe_add(probate_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building probate case {case_number}: {e}")
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
        skip_duplicates: bool = True,
        sample_mode: bool = False,
        sample_size: int = 20
    ) -> Tuple[int, int, int]:
        """
        Load evictions from DataFrame.

        Args:
            df: DataFrame with columns: CaseNumber, PartyAddress, FilingDate, etc.
            skip_duplicates: Skip existing records
            sample_mode: If True, only load first N rows for testing (includes multiple rows per case)
            sample_size: Number of rows to load when sample_mode=True

        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        # Apply sampling if requested
        if sample_mode:
            original_count = len(df)
            df = df.head(sample_size)
            logger.info(f"🧪 SAMPLE MODE: Loading {len(df)} eviction rows (out of {original_count} total)")
        else:
            logger.info(f"Loading evictions from {len(df)} rows")

        # Group by case number (plaintiff + defendant rows)
        grouped = df.groupby('CaseNumber')

        matched = 0
        unmatched = 0
        skipped = 0

        for case_number, group in grouped:
            case_number = _safe_str(case_number) or str(case_number).strip()

            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': case_number}):
                logger.debug(f"Skipping duplicate eviction: {case_number}")
                skipped += 1
                continue

            # Get defendant info (has address)
            if 'PartyType' in group.columns:
                defendant_candidates = group[group['PartyType'] == 'Defendant']
                defendant_row = defendant_candidates.iloc[0] if not defendant_candidates.empty else group.iloc[0]
            else:
                defendant_row = group.iloc[0]

            # Match by address
            property_record = None
            party_address_val = _none_if_nan(defendant_row.get('PartyAddress'))

            if party_address_val:
                match_result = self.find_property_by_address(str(party_address_val))
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched eviction by address (score: {score}%): {case_number}")

            if property_record:
                try:
                    # Plaintiff and defendant names
                    plaintiff_name = None
                    if 'PartyType' in group.columns and 'LastName/CompanyName' in group.columns:
                        pl_rows = group[group['PartyType'] == 'Plaintiff']
                        if not pl_rows.empty:
                            plaintiff_name = _safe_str(pl_rows['LastName/CompanyName'].iloc[0])

                    first = _none_if_nan(defendant_row.get('FirstName')) or ""
                    middle = _none_if_nan(defendant_row.get('MiddleName')) or ""
                    last = _none_if_nan(defendant_row.get('LastName/CompanyName')) or ""
                    defendant_name = " ".join([str(first).strip(), str(middle).strip(), str(last).strip()]).strip()
                    defendant_name = " ".join(defendant_name.split()) or None

                    # Nullable fields
                    case_status_val = _none_if_nan(defendant_row.get('Title'))
                    case_type_val = _none_if_nan(defendant_row.get('CaseTypeDescription'))
                    party_address_val = _none_if_nan(defendant_row.get('PartyAddress'))
                    plaintiff_name = _none_if_nan(plaintiff_name)

                    eviction_record = LegalProceeding(
                        property_id=property_record.id,
                        record_type='Eviction',
                        case_number=case_number,
                        filing_date=self.parse_date(defendant_row.get('FilingDate')),
                        case_status=case_status_val,
                        associated_party=defendant_name,
                        secondary_party=plaintiff_name,
                        meta_data={
                            'case_type': case_type_val,
                            'party_address': party_address_val
                        }
                    )

                    if self.safe_add(eviction_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building eviction {case_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(
                    f"No property match for eviction: {case_number} at {defendant_row.get('PartyAddress')}"
                )
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
            docket_number = _safe_str(row.get('Docket Number')) or str(row.get('Docket Number')).strip()

            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': docket_number}):
                logger.debug(f"Skipping duplicate bankruptcy: {docket_number}")
                skipped += 1
                continue

            # Match by owner name (only option available)
            property_record = None
            lead_name_val = _none_if_nan(row.get('Lead Name'))

            if lead_name_val:
                match_result = self.find_property_by_owner_name(str(lead_name_val))
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched bankruptcy by name (score: {score}%): {docket_number}")

            if property_record:
                try:
                    # Nullable fields
                    lead_name_val = _none_if_nan(row.get('Lead Name'))
                    case_type_val = _none_if_nan(row.get('Case Type'))
                    division_val = _none_if_nan(row.get('Division'))
                    court_id_val = _none_if_nan(row.get('Court ID'))

                    bankruptcy_record = LegalProceeding(
                        property_id=property_record.id,
                        record_type='Bankruptcy',
                        case_number=docket_number,
                        filing_date=self.parse_date(row.get('Date Filed')),
                        associated_party=lead_name_val,
                        meta_data={
                            'case_type': case_type_val,
                            'division': division_val,
                            'court_id': court_id_val
                        }
                    )

                    if self.safe_add(bankruptcy_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building bankruptcy {docket_number}: {e}")
                    unmatched += 1
            else:
                logger.debug(f"No property match for bankruptcy: {docket_number} (Name: {row.get('Lead Name')})")
                unmatched += 1

        logger.info(f"Bankruptcy: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        logger.warning("Bankruptcy match rate is low (name-only matching). Consider adding address enrichment.")
        return matched, unmatched, skipped