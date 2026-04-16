"""
Lien and judgment loader.
"""

import logging
from typing import Optional, Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import LegalAndLien

logger = logging.getLogger(__name__)


class LienLoader(BaseLoader):
    """Loader for liens and judgments."""

    _LLM_MAX_CALLS = 50  # higher budget — liens are highest-volume loader

    # Keywords that identify the governmental/institutional filer side of a code lien.
    # Mirrors the detection logic in lien_engine.py so both modules agree on party roles.
    _CITY_FILER_KEYWORDS = frozenset({'CITY OF TAMPA', 'HILLSBOROUGH COUNTY'})

    # Maps code-lien doc-type key → required Property.city value for geographic validation.
    # TCL (Tampa Code Liens) can only apply to properties inside Tampa city limits.
    # CCL (County Code Liens) apply to unincorporated Hillsborough — no single city value,
    # so None means "skip city filter".
    _CODE_LIEN_CITY_MAP = {'TCL': 'TAMPA', 'CCL': None}

    # ──────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _party_is_filer(self, party_val) -> bool:
        """Return True if the party string contains a known governmental filer keyword."""
        if party_val is None:
            return False
        try:
            if pd.isna(party_val):
                return False
        except (TypeError, ValueError):
            pass
        return any(kw in str(party_val).upper() for kw in self._CITY_FILER_KEYWORDS)

    def _find_code_lien_owner(
        self,
        owner_name: str,
        doc_type_str: str,
        threshold: int = 90,
    ) -> Optional[Tuple]:
        """
        Owner-name matching for code liens (TCL/CCL) with geographic post-filter.

        Wraps find_property_by_owner_name and then validates the matched
        property's city against the expected jurisdiction for the lien type:
          - TCL: Property.city must equal 'TAMPA'
          - CCL: No city filter (unincorporated county has diverse city values)

        Does NOT modify BaseLoader — this is a LienLoader-only concern.

        Args:
            owner_name:    The non-filer party name to match against.
            doc_type_str:  The document_type label (e.g. 'TAMPA CODE LIENS (TCL)').
            threshold:     Minimum rapidfuzz score (default 90 for code liens).

        Returns:
            Tuple of (Property, score) or None.
        """
        match_result = self.find_property_by_owner_name(owner_name, threshold=threshold)
        if not match_result:
            return None

        property_record, score = match_result

        # Geographic validation — check Property.city against the expected jurisdiction
        for key, required_city in self._CODE_LIEN_CITY_MAP.items():
            if key in doc_type_str.upper():
                if required_city is None:
                    # CCL: unincorporated county — skip city filter
                    return property_record, score
                prop_city = (property_record.city or '').upper().strip()
                if prop_city != required_city:
                    logger.warning(
                        f"Code lien city mismatch: matched property city='{prop_city}' "
                        f"expected '{required_city}' for doc_type='{doc_type_str}'. "
                        f"property_id={property_record.id}. Rejecting match."
                    )
                    return None
                return property_record, score

        # No matching key found — accept as-is (defensive)
        return property_record, score

    # ──────────────────────────────────────────────────────────────────────────
    # Main loader
    # ──────────────────────────────────────────────────────────────────────────

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
        sample_mode: bool = False,
        samples_per_type: int = 5
    ) -> Tuple[int, int, int]:
        """
        Load liens/judgments from DataFrame.

        Args:
            df: DataFrame with columns: Instrument, Grantor, Grantee, etc.
            skip_duplicates: Skip existing records
            sample_mode: If True, only load a few samples of each document type (for testing)
            samples_per_type: Number of samples per type when sample_mode=True

        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        # Sample mode: Extract ~5 entries of each type for fast testing
        if sample_mode:
            logger.info(f"SAMPLE MODE: Loading {samples_per_type} samples of each document type")

            # Get samples by document_type
            sampled_dfs = []
            if 'document_type' in df.columns:
                for doc_type in df['document_type'].dropna().unique():
                    samples = df[df['document_type'] == doc_type].head(samples_per_type)
                    sampled_dfs.append(samples)
                    logger.info(f"  Sampled {len(samples)} x {doc_type}")

                df = pd.concat(sampled_dfs, ignore_index=True)
                logger.info(f"Total samples: {len(df)} records (vs {len(df)} full dataset)")
            else:
                # Fallback: just take first N records
                df = df.head(samples_per_type * 6)
                logger.info(f"No document_type column, using first {len(df)} records")

        logger.info(f"Loading {len(df)} liens/judgments")

        matched = 0
        unmatched = 0
        skipped = 0

        # Per-document_type counts exposed for scraper_run_stats telemetry
        # Structure: { document_type_label: {total, matched, unmatched, skipped} }
        self.stats_by_doc_type: dict = {}

        for _, row in df.iterrows():
            instrument = str(row['Instrument']).strip()
            _doc_type_label = str(row.get('document_type', 'UNKNOWN')).strip()

            # LP (Lis Pendens) rows are handled by LisPendensLoader — skip here
            # to avoid creating spurious LegalAndLien records.
            _doc_upper = _doc_type_label.upper()
            if 'LIS PENDENS' in _doc_upper or '(LP)' in _doc_upper:
                logger.debug(f"Skipping LP row (handled by LisPendensLoader): {instrument}")
                skipped += 1
                continue

            if _doc_type_label not in self.stats_by_doc_type:
                self.stats_by_doc_type[_doc_type_label] = {'total': 0, 'matched': 0, 'unmatched': 0, 'skipped': 0}
            self.stats_by_doc_type[_doc_type_label]['total'] += 1

            # Check for duplicates
            if skip_duplicates and self.check_duplicate(LegalAndLien, {'instrument_number': instrument}):
                logger.debug(f"Skipping duplicate lien: {instrument}")
                self.stats_by_doc_type[_doc_type_label]['skipped'] += 1
                skipped += 1
                continue

            # Match property — try legal description first (most accurate),
            # then fall back to owner name using type-aware field selection.
            property_record = None
            match_method = None
            match_score = None
            match_field = None
            owner_name_for_match = None  # set in code-lien branch; used by LLM verifier

            doc_type_str = str(row.get('document_type', ''))
            is_tax_lien = 'TAX LIEN' in doc_type_str.upper()
            is_code_lien = 'TCL' in doc_type_str.upper() or 'CCL' in doc_type_str.upper()
            name_threshold = 90 if is_code_lien else 75

            # Strategy A: Legal description (lot/block/subdivision → parcel)
            if pd.notna(row.get('Legal')):
                match_result = self.find_property_by_legal_description(row['Legal'])
                if match_result:
                    property_record, score = match_result
                    match_method = 'legal_desc'
                    match_score = score
                    match_field = 'Legal'
                    logger.info(f"Matched lien by legal desc (score: {score}%): {instrument}")

            # Strategy B: Owner name — field selection depends on lien type.
            #
            # (a) IRS Tax Liens (TL): Grantor = "UNITED STATES OF AMERICA" (filer),
            #     property owner is in Grantee.
            # (b) Code Liens (TCL/CCL): City/county may appear in EITHER Grantor or
            #     Grantee. Detect which side is the governmental filer keyword and use
            #     the OTHER side as the property owner.
            #     Fix: previously always used Grantor, causing "CITY OF TAMPA" to be
            #     passed as owner_name when the city filed as Grantor — this produced
            #     the 113-record / 2-property-ID cascade incident.
            # (c) All other liens: Grantor = debtor / property owner.

            if not property_record and is_tax_lien and pd.notna(row.get('Grantee')):
                match_result = self.find_property_by_owner_name(row['Grantee'], threshold=name_threshold)
                if match_result:
                    property_record, score = match_result
                    match_method = 'owner_name'
                    match_score = score
                    match_field = 'Grantee'
                    logger.info(f"Matched tax lien by grantee/owner name (score: {score}%): {instrument}")

            elif not property_record and is_code_lien:
                grantor_val = row.get('Grantor')
                grantee_val = row.get('Grantee')
                grantor_is_filer = self._party_is_filer(grantor_val)
                grantee_is_filer = self._party_is_filer(grantee_val)

                if grantor_is_filer and pd.notna(grantee_val):
                    # City/county is Grantor (creditor) → property owner is Grantee
                    owner_name_for_match = grantee_val
                    owner_field_used = 'Grantee'
                    logger.debug(f"Code lien: Grantor is filer, using Grantee as owner: {instrument}")
                elif grantor_is_filer:
                    # Filer detected in Grantor but Grantee is empty — no owner name to match on
                    owner_name_for_match = None
                    owner_field_used = None
                    logger.warning(
                        f"Code lien {instrument}: filer '{grantor_val}' in Grantor "
                        f"but Grantee is empty — quarantining (no owner name available)."
                    )
                elif grantee_is_filer and pd.notna(grantor_val):
                    # City/county is Grantee (creditor) → property owner is Grantor
                    owner_name_for_match = grantor_val
                    owner_field_used = 'Grantor'
                    logger.debug(f"Code lien: Grantee is filer, using Grantor as owner: {instrument}")
                elif grantee_is_filer:
                    # Filer detected in Grantee but Grantor is empty — no owner name to match on
                    owner_name_for_match = None
                    owner_field_used = None
                    logger.warning(
                        f"Code lien {instrument}: filer '{grantee_val}' in Grantee "
                        f"but Grantor is empty — quarantining (no owner name available)."
                    )
                elif pd.notna(grantor_val):
                    # Neither side matched a filer keyword — fall back to Grantor with a warning
                    owner_name_for_match = grantor_val
                    owner_field_used = 'Grantor'
                    logger.warning(
                        f"Code lien {instrument}: no filer keyword found in Grantor='{grantor_val}' "
                        f"or Grantee='{grantee_val}'. Defaulting to Grantor."
                    )
                else:
                    owner_name_for_match = None
                    owner_field_used = None

                if owner_name_for_match:
                    match_result = self._find_code_lien_owner(
                        owner_name=owner_name_for_match,
                        doc_type_str=doc_type_str,
                        threshold=name_threshold,
                    )
                    if match_result:
                        property_record, score = match_result
                        match_method = 'owner_name'
                        match_score = score
                        match_field = owner_field_used
                        logger.info(
                            f"Matched code lien by {owner_field_used} '{owner_name_for_match}' "
                            f"(score: {score}%, threshold: {name_threshold}%): {instrument}"
                        )

            elif not property_record and not is_tax_lien and not is_code_lien:
                # Mechanics Liens (ML): Grantor = contractor/creditor, Grantee = property owner.
                # Try Grantee first so we match against the actual owner, not the contractor.
                # All other liens (judgment, HOA, etc.): Grantor = debtor/owner — try Grantor first.
                is_mechanics_lien = 'ML' in doc_type_str or 'MECHANIC' in doc_type_str
                first_field, second_field = (
                    ('Grantee', 'Grantor') if is_mechanics_lien else ('Grantor', 'Grantee')
                )

                if pd.notna(row.get(first_field)):
                    match_result = self.find_property_by_owner_name(row[first_field], threshold=name_threshold)
                    if match_result:
                        property_record, score = match_result
                        match_method = 'owner_name'
                        match_score = score
                        match_field = first_field
                        logger.info(f"Matched lien by {first_field.lower()} name (score: {score}%, threshold: {name_threshold}%): {instrument}")

                if not property_record and pd.notna(row.get(second_field)):
                    match_result = self.find_property_by_owner_name(row[second_field], threshold=name_threshold)
                    if match_result:
                        property_record, score = match_result
                        match_method = 'owner_name'
                        match_score = score
                        match_field = second_field
                        logger.info(f"Matched lien by {second_field.lower()} name (score: {score}%, threshold: {name_threshold}%): {instrument}")

            # LLM verification — applied to all name-matched liens in the borderline
            # score range (80-94%), with record-type context so Claude understands
            # the party roles. Geographic validation stays in _find_code_lien_owner().
            if property_record is not None and match_score is not None and match_method == 'owner_name':
                lien_rt = (
                    'lien_tcl' if 'TCL' in doc_type_str.upper() else
                    'lien_ccl' if 'CCL' in doc_type_str.upper() else
                    'lien_tl'  if is_tax_lien else
                    'lien_ml'
                )
                property_record, llm_method = self._apply_llm_verification(
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    current_best=property_record,
                    match_score=match_score,
                    record_type=lien_rt,
                    match_field=match_field or 'Grantee',
                )
                if llm_method:
                    match_method = llm_method

            if property_record:
                try:
                    # Determine record type
                    if 'JUDGMENT' in doc_type_str.upper() or 'CERTIFIED' in doc_type_str.upper():
                        record_type = 'Judgment'
                    else:
                        record_type = 'Lien'

                    # Assign creditor/debtor so that debtor = property owner in all cases.
                    #
                    # Field assignment rules:
                    #   (a) IRS Tax Liens: Grantor = IRS (filer) → creditor_val hardcoded,
                    #       debtor = Grantee (the taxpayer/property owner).
                    #   (b) Code Liens: city/county may be in either Grantor or Grantee —
                    #       whichever side contains the filer keyword is the creditor;
                    #       the other side is the debtor (property owner).
                    #   (c) All other liens: creditor = Grantee, debtor = Grantor.
                    if is_tax_lien:
                        creditor_val = 'INTERNAL REVENUE SERVICE'
                        debtor_raw = row.get('Grantee')
                    elif is_code_lien:
                        grantor_raw = row.get('Grantor')
                        grantee_raw = row.get('Grantee')
                        if self._party_is_filer(grantor_raw):
                            # Grantor = city/county (creditor); Grantee = property owner (debtor)
                            creditor_raw = grantor_raw
                            debtor_raw = grantee_raw
                        else:
                            # Grantee = city/county (creditor); Grantor = property owner (debtor)
                            creditor_raw = grantee_raw
                            debtor_raw = grantor_raw
                        creditor_val = None if pd.isna(creditor_raw) else creditor_raw
                    else:
                        creditor_raw = row.get('Grantee')
                        creditor_val = None if pd.isna(creditor_raw) else creditor_raw
                        debtor_raw = row.get('Grantor')

                    debtor_val = None if pd.isna(debtor_raw) else debtor_raw

                    book_type_val = row.get('BookType')
                    if pd.isna(book_type_val):
                        book_type_val = None

                    book_number_val = row.get('Book')
                    if pd.isna(book_number_val):
                        book_number_val = None

                    page_number_val = row.get('Page')
                    if pd.isna(page_number_val):
                        page_number_val = None

                    doc_type_val = row.get('document_type')
                    if pd.isna(doc_type_val):
                        doc_type_val = None

                    legal_desc_val = row.get('Legal')
                    if pd.isna(legal_desc_val):
                        legal_desc_val = None

                    lien_record = LegalAndLien(
                        property_id=property_record.id,
                        record_type=record_type,
                        instrument_number=instrument,
                        creditor=creditor_val,
                        debtor=debtor_val,
                        amount=self.parse_amount(row.get('Filing Amt')),
                        filing_date=self.parse_date(row.get('RecordDate')),
                        book_type=book_type_val,
                        book_number=book_number_val,
                        page_number=page_number_val,
                        document_type=doc_type_val,
                        legal_description=legal_desc_val,
                        match_confidence=match_score,
                        match_method=match_method,
                        meta_data={'match_field': match_field},
                    )

                    if self.safe_add(lien_record):
                        matched += 1
                        self.stats_by_doc_type[_doc_type_label]['matched'] += 1
                    else:
                        unmatched += 1
                        self.stats_by_doc_type[_doc_type_label]['unmatched'] += 1

                except Exception as e:
                    logger.error(f"Error building lien {instrument}: {e}")
                    unmatched += 1
                    self.stats_by_doc_type[_doc_type_label]['unmatched'] += 1
            else:
                logger.debug(
                    f"No property match for lien: {instrument} "
                    f"(Grantor: {row.get('Grantor')}, Grantee: {row.get('Grantee')}, "
                    f"doc_type: {_doc_type_label})"
                )
                self.quarantine_unmatched(
                    source_type="liens",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    county_id=self.county_id,
                    instrument_number=instrument,
                    grantor=row.get('Grantor'),
                )
                unmatched += 1
                self.stats_by_doc_type[_doc_type_label]['unmatched'] += 1

        logger.info(f"Liens/Judgments: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
