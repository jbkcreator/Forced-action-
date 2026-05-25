"""
Legal proceedings loaders (Probate, Evictions, Bankruptcy).

Fixes:
- ProbateLoader now sanitizes NaN -> None for all JSON/meta fields (matches Eviction/Bankruptcy behavior)
- Also sanitizes beneficiary/plaintiff fields and other nullable columns consistently
"""

import logging
import re
from typing import Tuple, Optional

import pandas as pd

# Patterns that indicate a PartyAddress field contains a legal description
# (lot/block/unit/condo) rather than a postal street address.
_LEGAL_DESC_RE = re.compile(
    r'^\s*(LOT\s|BLOCK\s|UNIT\s|BLDG\s|BUILDING\s|TRACT\s|PARCEL\s|CONDO\s|'
    r'[A-Z]-\d|UNIT\s*NO\.?\s*\d)',
    re.IGNORECASE,
)

from src.loaders.base import (
    BaseLoader,
    MATCH_METHOD_LEGAL_DESC,
    MATCH_METHOD_NORM_ADDR,
    MATCH_METHOD_OWNER_NAME,
    MATCH_METHOD_OWNER_ZIP,
    MATCH_METHOD_OWNER_CITY,
)
from src.loaders._address_utils import split_address
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

    _LLM_MAX_CALLS = 20

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
        # Normalize column names: some CSVs use "Case Number" instead of "CaseNumber"
        if 'Case Number' in df.columns and 'CaseNumber' not in df.columns:
            df = df.rename(columns={'Case Number': 'CaseNumber'})

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
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': case_number}, scope_county=False):
                logger.debug(f"Skipping duplicate probate case: {case_number}")
                skipped += 1
                continue

            # Get decedent info (first row with decedent)
            if 'PartyType' in group.columns:
                decedent_candidates = group[group['PartyType'] == 'Decedent']
                decedent_row = decedent_candidates.iloc[0] if not decedent_candidates.empty else group.iloc[0]
            else:
                decedent_row = group.iloc[0]

            # ── Cascade match ─────────────────────────────────────────────
            # Build inputs: PartyAddress can be either a street address (cascade
            # stage 2) or a legal description (alternate path). Split on the
            # legal-desc regex so we route to the right stage.
            party_address_val = _none_if_nan(decedent_row.get('PartyAddress'))
            addr_for_cascade: Optional[str] = None
            legal_for_cascade: Optional[str] = None
            zip_for_cascade: Optional[str] = None
            city_for_cascade: Optional[str] = None

            if party_address_val:
                addr_str = str(party_address_val)
                if _LEGAL_DESC_RE.match(addr_str):
                    legal_for_cascade = addr_str
                else:
                    addr_for_cascade = addr_str
                    _, city_for_cascade, zip_for_cascade = split_address(addr_str)

            # Build owner-name variants (try most specific first; cascade is
            # called once per variant until a match meets the threshold).
            name_variants: list = []
            last_name = _none_if_nan(decedent_row.get('LastName/CompanyName'))
            if last_name:
                first = str(_none_if_nan(decedent_row.get('FirstName')) or "").strip()
                middle = str(_none_if_nan(decedent_row.get('MiddleName')) or "").strip()
                last = str(last_name).strip()
                full_name = " ".join([first, middle, last]).strip()
                full_name = " ".join(full_name.split())
                if full_name:
                    name_variants.append(full_name)
                if first and last:
                    first_last = f"{first} {last}"
                    if first_last != full_name:
                        name_variants.append(first_last)
                if '-' in last:
                    for part in last.split('-'):
                        if len(part) > 4 and first:
                            name_variants.append(f"{first} {part}")

            property_record = None
            match_score = 0
            match_method = None

            for variant in (name_variants or [None]):
                # One cascade call per name variant. Address/legal stages don't
                # depend on the name so they get re-evaluated each loop, but
                # that's cheap — the cascade short-circuits at stage 1 or 2.
                prop, method, score = self.find_property_cascade(
                    address=addr_for_cascade,
                    legal_desc=legal_for_cascade,
                    owner_name=variant,
                    zip_code=zip_for_cascade,
                    city=city_for_cascade,
                    addr_threshold=self._thresholds.address_floor,
                    owner_threshold=self._thresholds.owner_name_floor,
                    legal_threshold=self._thresholds.legal_desc_floor,
                )
                if prop:
                    property_record, match_method, match_score = prop, method, score
                    logger.info(f"Matched probate by {method} (score: {score}%, variant: {variant!r}): {case_number}")
                    # LLM verification for owner-based matches (any zip/city/plain)
                    if method in (MATCH_METHOD_OWNER_NAME, MATCH_METHOD_OWNER_ZIP, MATCH_METHOD_OWNER_CITY):
                        property_record, llm_method = self._apply_llm_verification(
                            raw_row=decedent_row.to_dict() if hasattr(decedent_row, 'to_dict') else dict(decedent_row),
                            current_best=property_record, match_score=match_score,
                            record_type='probate', match_field='LastName/CompanyName',
                        )
                        if llm_method:
                            match_method = llm_method
                    break

            if property_record:
                tier = self._classify_match(match_score, match_method)
                if tier == "matched":
                    try:
                        # Decedent name
                        first = _none_if_nan(decedent_row.get('FirstName')) or ""
                        middle = _none_if_nan(decedent_row.get('MiddleName')) or ""
                        last = _none_if_nan(decedent_row.get('LastName/CompanyName')) or ""
                        decedent_name = " ".join([str(first).strip(), str(middle).strip(), str(last).strip()]).strip()
                        decedent_name = " ".join(decedent_name.split()) or None

                        # Collect all heirs (Beneficiary + Next of Kin) — store first in
                        # secondary_party for skip-trace compatibility, full list in meta_data.
                        all_heirs = []
                        if 'PartyType' in group.columns and 'LastName/CompanyName' in group.columns:
                            heir_rows = group[group['PartyType'].isin(['Beneficiary', 'Next of Kin'])]
                            for _, heir_row in heir_rows.iterrows():
                                h_first = str(_none_if_nan(heir_row.get('FirstName')) or "").strip()
                                h_last  = _safe_str(heir_row.get('LastName/CompanyName')) or ""
                                name = " ".join(p for p in [h_first, h_last] if p)
                                if name:
                                    all_heirs.append(name)
                        beneficiary = all_heirs[0] if all_heirs else None

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
                            match_confidence=round(match_score / 100.0, 3),
                            match_method=match_method,
                            county_id=self.county_id,
                            meta_data={
                                'case_type': case_type_val,
                                'party_address': party_address_val,
                                'heirs': all_heirs,
                            }
                        )

                        if self.safe_add(probate_record):
                            matched += 1
                        else:
                            unmatched += 1

                    except Exception as e:
                        logger.error(f"Error building probate case {case_number}: {e}")
                        unmatched += 1
                else:
                    logger.debug(f"Pending review probate: {case_number} (score: {match_score}%, method: {match_method})")
                    self.quarantine_unmatched(
                        source_type="probate",
                        raw_row=decedent_row.to_dict() if hasattr(decedent_row, 'to_dict') else dict(decedent_row),
                        county_id=self.county_id,
                        address_string=str(party_address_val) if party_address_val else None,
                        match_status="pending_review",
                        match_confidence=match_score / 100.0,
                        candidate_property_id=property_record.id,
                        match_method=match_method,
                    )
                    unmatched += 1
            else:
                logger.debug(f"No property match for probate case: {case_number}")
                self.quarantine_unmatched(
                    source_type="probate",
                    raw_row=decedent_row.to_dict() if hasattr(decedent_row, 'to_dict') else dict(decedent_row),
                    county_id=self.county_id,
                    address_string=str(party_address_val) if party_address_val else None,
                )
                unmatched += 1

        logger.info(f"Probate: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped


class EvictionLoader(BaseLoader):
    """Loader for eviction court cases."""

    _LLM_MAX_CALLS = 20

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

        # Normalize column name: some CSVs use "Case Number" (with space)
        if 'Case Number' in df.columns and 'CaseNumber' not in df.columns:
            df = df.rename(columns={'Case Number': 'CaseNumber'})

        # Group by case number (plaintiff + defendant rows)
        grouped = df.groupby('CaseNumber')

        matched = 0
        unmatched = 0
        skipped = 0

        for case_number, group in grouped:
            case_number = _safe_str(case_number) or str(case_number).strip()

            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': case_number}, scope_county=False):
                logger.debug(f"Skipping duplicate eviction: {case_number}")
                skipped += 1
                continue

            # Get defendant info (has address)
            if 'PartyType' in group.columns:
                defendant_candidates = group[group['PartyType'] == 'Defendant']
                defendant_row = defendant_candidates.iloc[0] if not defendant_candidates.empty else group.iloc[0]
            else:
                defendant_row = group.iloc[0]

            # ── Cascade match ─────────────────────────────────────────────
            # Defendant address (tenant address = property address). Defendant
            # NAME is never used for matching (it's the tenant, not the owner).
            # Plaintiff name (landlord/owner) is the owner candidate.
            party_address_val = _none_if_nan(defendant_row.get('PartyAddress'))
            addr_for_cascade = str(party_address_val) if party_address_val else None
            _, city_for_cascade, zip_for_cascade = split_address(addr_for_cascade) if addr_for_cascade else (None, None, None)

            # Plaintiff (owner) name variants
            name_variants: list = []
            plaintiff_row_for_llm = None
            if 'PartyType' in group.columns and 'LastName/CompanyName' in group.columns:
                plaintiff_rows = group[group['PartyType'] == 'Plaintiff']
                if not plaintiff_rows.empty:
                    prow = plaintiff_rows.iloc[0]
                    plaintiff_row_for_llm = prow
                    first = str(_none_if_nan(prow.get('FirstName')) or '').strip()
                    last = str(_none_if_nan(prow.get('LastName/CompanyName')) or '').strip()
                    if last:
                        if first:
                            name_variants.append(f"{first} {last}")
                        else:
                            name_variants.append(last)

            property_record = None
            match_score = 0
            match_method = None

            for variant in (name_variants or [None]):
                prop, method, score = self.find_property_cascade(
                    address=addr_for_cascade,
                    owner_name=variant,
                    zip_code=zip_for_cascade,
                    city=city_for_cascade,
                    addr_threshold=self._thresholds.address_floor,
                    owner_threshold=self._thresholds.owner_name_floor,
                )
                if prop:
                    property_record, match_method, match_score = prop, method, score
                    logger.info(f"Matched eviction by {method} (score: {score}%, variant: {variant!r}): {case_number}")
                    if method in (MATCH_METHOD_OWNER_NAME, MATCH_METHOD_OWNER_ZIP, MATCH_METHOD_OWNER_CITY) and plaintiff_row_for_llm is not None:
                        property_record, llm_method = self._apply_llm_verification(
                            raw_row=plaintiff_row_for_llm.to_dict() if hasattr(plaintiff_row_for_llm, 'to_dict') else dict(plaintiff_row_for_llm),
                            current_best=property_record, match_score=match_score,
                            record_type='eviction', match_field='Plaintiff',
                        )
                        if llm_method:
                            match_method = llm_method
                    break

            if property_record:
                tier = self._classify_match(match_score, match_method)
                if tier == "matched":
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
                            match_confidence=round(match_score / 100.0, 3),
                            match_method=match_method,
                            county_id=self.county_id,
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
                    logger.debug(f"Pending review eviction: {case_number} (score: {match_score}%, method: {match_method})")
                    self.quarantine_unmatched(
                        source_type="evictions",
                        raw_row=defendant_row.to_dict() if hasattr(defendant_row, 'to_dict') else dict(defendant_row),
                        county_id=self.county_id,
                        address_string=str(party_address_val) if party_address_val else None,
                        match_status="pending_review",
                        match_confidence=match_score / 100.0,
                        candidate_property_id=property_record.id,
                        match_method=match_method,
                    )
                    unmatched += 1
            else:
                logger.warning(
                    f"No property match for eviction: {case_number} at {defendant_row.get('PartyAddress')}"
                )
                self.quarantine_unmatched(
                    source_type="evictions",
                    raw_row=defendant_row.to_dict() if hasattr(defendant_row, 'to_dict') else dict(defendant_row),
                    county_id=self.county_id,
                    address_string=str(party_address_val) if party_address_val else None,
                )
                unmatched += 1

        logger.info(f"Evictions: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped


class BankruptcyLoader(BaseLoader):
    """Loader for bankruptcy court cases."""

    _LLM_MAX_CALLS = 15

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
            if skip_duplicates and self.check_duplicate(LegalProceeding, {'case_number': docket_number}, scope_county=False):
                logger.debug(f"Skipping duplicate bankruptcy: {docket_number}")
                skipped += 1
                continue

            # ── Address match (PACER CM/ECF provides debtor street address) ────
            property_record = None
            match_score = 0
            match_method = None
            lead_name_val = _none_if_nan(row.get('Lead Name'))
            debtor_street = _none_if_nan(row.get('Debtor Street'))
            debtor_zip    = _none_if_nan(row.get('Debtor Zip'))

            if debtor_street and debtor_zip:
                result = self.find_property_by_address(
                    address=debtor_street,
                    zip_code=str(debtor_zip).split('-')[0],  # strip ZIP+4 if present
                )
                if result:
                    property_record, match_score = result
                    match_method = MATCH_METHOD_NORM_ADDR
                    logger.info(
                        f"Matched bankruptcy by address (score: {match_score}%): "
                        f"{docket_number} — {debtor_street}"
                    )

            # ── Name match fallback (no address or address match failed) ─────
            if not property_record and lead_name_val:
                name_str = str(lead_name_val).strip()
                name_parts = name_str.split()

                name_variants: list = [name_str]
                if len(name_parts) >= 2:
                    first_last = f"{name_parts[0]} {name_parts[-1]}"
                    if first_last != name_str:
                        name_variants.append(first_last)
                    if '-' in name_parts[-1]:
                        for part in name_parts[-1].split('-'):
                            if len(part) > 4:
                                name_variants.append(f"{name_parts[0]} {part}")

                for variant in name_variants:
                    prop, method, score = self.find_property_cascade(
                        owner_name=variant,
                        owner_threshold=self._thresholds.owner_name_floor,
                    )
                    if prop:
                        property_record, match_method, match_score = prop, method, score
                        logger.info(
                            f"Matched bankruptcy by {method} '{variant}' (score: {score}%): {docket_number}"
                        )
                        if method in (MATCH_METHOD_OWNER_NAME, MATCH_METHOD_OWNER_ZIP, MATCH_METHOD_OWNER_CITY):
                            property_record, llm_method = self._apply_llm_verification(
                                raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                                current_best=property_record, match_score=match_score,
                                record_type='bankruptcy', match_field='Lead Name',
                            )
                            if llm_method:
                                match_method = llm_method
                        break

            if property_record:
                tier = self._classify_match(match_score, match_method)
                if tier == "matched":
                    try:
                        # Nullable fields
                        lead_name_val = _none_if_nan(row.get('Lead Name'))
                        case_type_val = _none_if_nan(row.get('Case Type'))
                        court_id_val  = _none_if_nan(row.get('Court ID'))

                        bankruptcy_record = LegalProceeding(
                            property_id=property_record.id,
                            record_type='Bankruptcy',
                            case_number=docket_number,
                            filing_date=self.parse_date(row.get('Date Filed')),
                            associated_party=lead_name_val,
                            match_confidence=round(match_score / 100.0, 3),
                            match_method=match_method,
                            county_id=self.county_id,
                            meta_data={
                                'case_type': case_type_val,
                                'chapter':   _none_if_nan(row.get('Chapter')),
                                'court_id':  court_id_val,
                                'debtor_address': {
                                    'street': _none_if_nan(row.get('Debtor Street')),
                                    'city':   _none_if_nan(row.get('Debtor City')),
                                    'state':  _none_if_nan(row.get('Debtor State')),
                                    'zip':    _none_if_nan(row.get('Debtor Zip')),
                                },
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
                    logger.debug(f"Pending review bankruptcy: {docket_number} (score: {match_score}%, method: {match_method})")
                    self.quarantine_unmatched(
                        source_type="bankruptcies",
                        raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                        county_id=self.county_id,
                        grantor=str(lead_name_val) if lead_name_val else None,
                        match_status="pending_review",
                        match_confidence=match_score / 100.0,
                        candidate_property_id=property_record.id,
                        match_method=match_method,
                    )
                    unmatched += 1
            else:
                logger.debug(f"No property match for bankruptcy: {docket_number} (Name: {row.get('Lead Name')})")
                self.quarantine_unmatched(
                    source_type="bankruptcies",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    county_id=self.county_id,
                    grantor=str(lead_name_val) if lead_name_val else None,
                )
                unmatched += 1

        logger.info(f"Bankruptcy: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped


class DivorceLoader(BaseLoader):
    """
    Loader for dissolution-of-marriage / domestic-relations court cases.

    Uses the same civil filing CSV format as evictions. Party types are
    Petitioner (person filing) and Respondent — tries petitioner address first,
    then respondent address, then name-based fallback.
    """

    _LLM_MAX_CALLS = 20

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
        sample_mode: bool = False,
        sample_size: int = 20,
    ) -> Tuple[int, int, int]:
        """
        Load dissolution/divorce cases from DataFrame.

        Args:
            df: DataFrame from civil filing CSV (already filtered to DR case types)
            skip_duplicates: Skip existing records
            sample_mode/sample_size: Test subsets

        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        if sample_mode:
            original_count = len(df)
            df = df.head(sample_size)
            logger.info(f"[DivorceLoader] SAMPLE MODE: {len(df)} rows (of {original_count})")
        else:
            logger.info(f"[DivorceLoader] Loading divorce cases from {len(df)} rows")

        # Normalize column name
        if "CaseNumber" in df.columns and "Case Number" not in df.columns:
            df = df.rename(columns={"CaseNumber": "Case Number"})

        grouped = df.groupby("Case Number") if "Case Number" in df.columns else df.groupby("CaseNumber")

        matched = 0
        unmatched = 0
        skipped = 0

        for case_number, group in grouped:
            case_number = _safe_str(case_number) or str(case_number).strip()

            if skip_duplicates and self.check_duplicate(LegalProceeding, {"case_number": case_number}, scope_county=False):
                logger.debug(f"[DivorceLoader] Skipping duplicate: {case_number}")
                skipped += 1
                continue

            # Prefer petitioner row (more likely to have property address)
            petitioner_row = None
            respondent_row = None
            if "PartyType" in group.columns:
                pet_rows = group[group["PartyType"].str.contains("Petitioner", case=False, na=False)]
                res_rows = group[group["PartyType"].str.contains("Respondent", case=False, na=False)]
                petitioner_row = pet_rows.iloc[0] if not pet_rows.empty else group.iloc[0]
                respondent_row = res_rows.iloc[0] if not res_rows.empty else None
            else:
                petitioner_row = group.iloc[0]

            property_record = None
            match_score = 0
            match_method = None
            primary_row = petitioner_row

            # ── Cascade match ─────────────────────────────────────────────
            # Tries petitioner address → respondent address → petitioner name.
            # Each address gets its own cascade call so zip/city from each can
            # be exploited independently. Owner-name cascade also fires once
            # per address-bearing party (in case the address didn't match but
            # the petitioner's name does, scoped to their zip).
            petitioner_addr = _none_if_nan(primary_row.get("PartyAddress"))
            respondent_addr = _none_if_nan(respondent_row.get("PartyAddress")) if respondent_row is not None else None

            # Build name variants from the petitioner
            first = str(_none_if_nan(primary_row.get("FirstName")) or "").strip()
            last = str(_none_if_nan(primary_row.get("LastName/CompanyName")) or "").strip()
            name_variants: list = []
            if last:
                if first:
                    name_variants.append(f"{first} {last}")
                else:
                    name_variants.append(last)
                if "-" in last:
                    for part in last.split("-"):
                        if len(part) > 4 and first:
                            name_variants.append(f"{first} {part}")

            # Cascade attempts: each address tried as a separate cascade call,
            # then plain-name attempts at the end.
            attempts: list = []
            if petitioner_addr:
                _, p_city, p_zip = split_address(str(petitioner_addr))
                attempts.append((str(petitioner_addr), p_zip, p_city, "petitioner-addr"))
            if respondent_addr:
                _, r_city, r_zip = split_address(str(respondent_addr))
                attempts.append((str(respondent_addr), r_zip, r_city, "respondent-addr"))
            if not attempts:
                attempts.append((None, None, None, "name-only"))

            for addr, zip_c, city_c, label in attempts:
                for variant in (name_variants or [None]):
                    prop, method, score = self.find_property_cascade(
                        address=addr,
                        owner_name=variant,
                        zip_code=zip_c,
                        city=city_c,
                        addr_threshold=self._thresholds.address_floor,
                        owner_threshold=self._thresholds.owner_name_floor,
                    )
                    if prop:
                        property_record, match_method, match_score = prop, method, score
                        logger.info(
                            f"[DivorceLoader] Matched by {method} ({label}, variant: {variant!r}, "
                            f"score: {score}%): {case_number}"
                        )
                        if method in (MATCH_METHOD_OWNER_NAME, MATCH_METHOD_OWNER_ZIP, MATCH_METHOD_OWNER_CITY):
                            property_record, llm_method = self._apply_llm_verification(
                                raw_row=primary_row.to_dict() if hasattr(primary_row, "to_dict") else dict(primary_row),
                                current_best=property_record,
                                match_score=match_score,
                                record_type="divorce",
                                match_field="LastName/CompanyName",
                            )
                            if llm_method:
                                match_method = llm_method
                        break
                if property_record:
                    break

            if property_record:
                tier = self._classify_match(match_score, match_method)
                if tier == "matched":
                    try:
                        first = _none_if_nan(primary_row.get("FirstName")) or ""
                        middle = _none_if_nan(primary_row.get("MiddleName")) or ""
                        last = _none_if_nan(primary_row.get("LastName/CompanyName")) or ""
                        petitioner_name = " ".join(
                            [str(first).strip(), str(middle).strip(), str(last).strip()]
                        ).strip()
                        petitioner_name = " ".join(petitioner_name.split()) or None

                        respondent_name = None
                        if respondent_row is not None and "LastName/CompanyName" in group.columns:
                            r_last = _none_if_nan(respondent_row.get("LastName/CompanyName"))
                            r_first = _none_if_nan(respondent_row.get("FirstName")) or ""
                            if r_last:
                                respondent_name = " ".join([str(r_first).strip(), str(r_last).strip()]).strip()
                                respondent_name = " ".join(respondent_name.split()) or None

                        case_status_val = _none_if_nan(primary_row.get("Title"))
                        case_type_val = _none_if_nan(primary_row.get("CaseTypeDescription"))
                        addr_used = _none_if_nan(primary_row.get("PartyAddress"))

                        divorce_record = LegalProceeding(
                            property_id=property_record.id,
                            record_type="Divorce",
                            case_number=case_number,
                            filing_date=self.parse_date(primary_row.get("FilingDate")),
                            case_status=case_status_val,
                            associated_party=petitioner_name,
                            secondary_party=respondent_name,
                            match_confidence=round(match_score / 100.0, 3),
                            match_method=match_method,
                            county_id=self.county_id,
                            meta_data={
                                "case_type": case_type_val,
                                "party_address": addr_used,
                            },
                        )

                        if self.safe_add(divorce_record):
                            matched += 1
                        else:
                            unmatched += 1

                    except Exception as exc:
                        logger.error(f"[DivorceLoader] Error building case {case_number}: {exc}")
                        unmatched += 1
                else:
                    logger.debug(f"[DivorceLoader] Pending review: {case_number} (score: {match_score}%, method: {match_method})")
                    self.quarantine_unmatched(
                        source_type="divorce_filings",
                        raw_row=primary_row.to_dict() if hasattr(primary_row, "to_dict") else dict(primary_row),
                        county_id=self.county_id,
                        address_string=str(party_address_val) if party_address_val else None,
                        match_status="pending_review",
                        match_confidence=match_score / 100.0,
                        candidate_property_id=property_record.id,
                        match_method=match_method,
                    )
                    unmatched += 1
            else:
                logger.debug(
                    f"[DivorceLoader] No property match for case: {case_number} "
                    f"at {primary_row.get('PartyAddress')}"
                )
                self.quarantine_unmatched(
                    source_type="divorce_filings",
                    raw_row=primary_row.to_dict() if hasattr(primary_row, "to_dict") else dict(primary_row),
                    county_id=self.county_id,
                    address_string=str(party_address_val) if party_address_val else None,
                )
                unmatched += 1

        logger.info(f"[DivorceLoader] {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped