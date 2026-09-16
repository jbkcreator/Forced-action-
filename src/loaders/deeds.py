"""
Deed (property transfer) loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import (
    BaseLoader,
    MATCH_METHOD_PARCEL_ID,
    MATCH_METHOD_LEGAL_DESC,
    MATCH_METHOD_OWNER_NAME,
    MATCH_METHOD_OWNER_ZIP,
    MATCH_METHOD_OWNER_CITY,
)
from src.core.models import Deed

logger = logging.getLogger(__name__)


class DeedLoader(BaseLoader):
    """Loader for property deeds (ownership transfers)."""

    _LLM_MAX_CALLS = 30
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
        sample_mode: bool = False,
        sample_size: int = 10
    ) -> Tuple[int, int, int]:
        """
        Load deeds from DataFrame.
        
        Args:
            df: DataFrame with columns: Instrument, Grantor, Grantee, etc.
            skip_duplicates: Skip existing records
            sample_mode: If True, only load first N records for testing
            sample_size: Number of records to load when sample_mode=True
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        # Apply sampling if requested
        if sample_mode:
            original_count = len(df)
            df = df.head(sample_size)
            logger.info(f"🧪 SAMPLE MODE: Loading {len(df)} deeds (out of {original_count} total)")
        else:
            logger.info(f"Loading {len(df)} deeds")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            instrument = str(row['Instrument']).strip()
            
            # Check for duplicates
            if skip_duplicates:
                existing = self.session.query(Deed).filter(
                    Deed.instrument_number == instrument,
                    Deed.county_id == self.county_id,
                ).first()
                if existing:
                    logger.debug(f"Skipping duplicate deed: {instrument}")
                    skipped += 1
                    continue
            
            # Match property — try legal description first (most accurate),
            # then fall back to owner name (Grantor → Grantee).
            property_record = None
            match_score = 0
            match_method = None

            # Strategy 0: parcel ID extracted from Legal text — fires when the
            # county recording includes a folio/parcel number in the description.
            # Falls through silently when the Legal field has no recognisable ID.
            if pd.notna(row.get('Legal')):
                for pid in self.extract_parcel_ids_from_text(row['Legal']):
                    prop = self.find_property_by_parcel_id(pid)
                    if prop:
                        property_record = prop
                        match_score = 100
                        match_method = MATCH_METHOD_PARCEL_ID
                        logger.info(f"Matched deed by parcel ID {pid}: {instrument}")
                        break

            # Strategy A: Legal description (lot/block/subdivision → parcel)
            if not property_record and pd.notna(row.get('Legal')):
                match_result = self.find_property_by_legal_description(
                    row['Legal'], threshold=self._thresholds.legal_desc_floor,
                )
                if match_result:
                    property_record, match_score = match_result
                    match_method = MATCH_METHOD_LEGAL_DESC
                    logger.info(f"Matched deed by legal desc (score: {match_score}%): {instrument}")

            # Strategy B: Grantor (seller) name — comma-split handles multi-grantor/trust fields.
            # Deed source has no address column → cascade reduces to stage 5 (owner_name).
            # find_property_by_owner_name_multi is retained for the comma-split semantics.
            if not property_record and pd.notna(row.get('Grantor')):
                match_result = self.find_property_by_owner_name_multi(row['Grantor'], threshold=self._thresholds.owner_name_floor)
                if match_result:
                    property_record, match_score = match_result
                    match_method = MATCH_METHOD_OWNER_NAME
                    logger.info(f"Matched deed by grantor (score: {match_score}%): {instrument}")
                    property_record, llm_method = self._apply_llm_verification(
                        raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                        current_best=property_record, match_score=match_score,
                        record_type='deed', match_field='Grantor',
                    )
                    if llm_method:
                        match_method = llm_method

            # Strategy C: Grantee (buyer) name — comma-split handles multi-grantee fields
            if not property_record and pd.notna(row.get('Grantee')):
                match_result = self.find_property_by_owner_name_multi(row['Grantee'], threshold=self._thresholds.owner_name_floor)
                if match_result:
                    property_record, match_score = match_result
                    match_method = MATCH_METHOD_OWNER_NAME
                    logger.info(f"Matched deed by grantee (score: {match_score}%): {instrument}")
                    property_record, llm_method = self._apply_llm_verification(
                        raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                        current_best=property_record, match_score=match_score,
                        record_type='deed', match_field='Grantee',
                    )
                    if llm_method:
                        match_method = llm_method

            if property_record:
                tier = self._classify_match(match_score, match_method)
                if tier == "matched":
                    try:
                        # Handle NaN values
                        grantor_val = row.get('Grantor')
                        if pd.isna(grantor_val):
                            grantor_val = None

                        grantee_val = row.get('Grantee')
                        if pd.isna(grantee_val):
                            grantee_val = None

                        deed_type_val = row.get('DocType')
                        if pd.isna(deed_type_val):
                            deed_type_val = None

                        book_type_val = row.get('BookType')
                        if pd.isna(book_type_val):
                            book_type_val = None

                        book_number_val = row.get('BookNum')
                        if pd.isna(book_number_val):
                            book_number_val = None

                        page_number_val = row.get('PageNum')
                        if pd.isna(page_number_val):
                            page_number_val = None

                        legal_desc_val = row.get('Legal')
                        if pd.isna(legal_desc_val):
                            legal_desc_val = None

                        # Sprint 4.4: extract mortgage amount from Filing Amt column
                        filing_amt = self.parse_amount(row.get('Filing Amt'))
                        mortgage_amount = filing_amt
                        # Only keep mortgage_amount if this is a mortgage-type document
                        # (deed_type or DocType includes "mortgage" or "deed of trust");
                        # otherwise NULL it out — the Filing Amt on non-mortgage deeds
                        # is recording fees, not loan amounts.
                        doc_type_raw = str(deed_type_val or row.get('DocType') or '').lower()
                        if mortgage_amount is not None and not any(
                            kw in doc_type_raw for kw in ['mortgage', 'deed of trust']
                        ):
                            mortgage_amount = None

                        deed_record = Deed(
                            property_id=property_record.id,
                            instrument_number=instrument,
                            grantor=grantor_val,
                            grantee=grantee_val,
                            record_date=self.parse_date(row.get('RecordDate')),
                            sale_price=self.parse_amount(row.get('SalesPrice')),
                            deed_type=deed_type_val,
                            doc_type=None,  # Not in this CSV format
                            book_type=book_type_val,
                            book_number=book_number_val,
                            page_number=page_number_val,
                            legal_description=legal_desc_val,
                            mortgage_amount=mortgage_amount,
                            match_confidence=round(match_score / 100.0, 3),
                            match_method=match_method,
                            county_id=self.county_id,
                        )

                        if self.safe_add(deed_record):
                            matched += 1
                            try:
                                from src.services.borrower_profile_service import (
                                    schedule_profile_recompute_for_property,
                                )
                                schedule_profile_recompute_for_property(
                                    self.session, property_record.id, "new_deed"
                                )
                            except Exception:
                                pass
                        else:
                            unmatched += 1

                    except Exception as e:
                        logger.error(f"Error building deed {instrument}: {e}")
                        unmatched += 1
                else:
                    logger.debug(f"Pending review deed: {instrument} (score: {match_score}%, method: {match_method})")
                    self.quarantine_unmatched(
                        source_type="deeds",
                        raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                        county_id=self.county_id,
                        instrument_number=instrument,
                        grantor=row.get('Grantor'),
                        match_status="pending_review",
                        match_confidence=match_score / 100.0,
                        candidate_property_id=property_record.id,
                        match_method=match_method,
                    )
                    unmatched += 1
            else:
                logger.debug(f"No property match for deed: {instrument} (Grantor: {row.get('Grantor')})")
                self.quarantine_unmatched(
                    source_type="deeds",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    county_id=self.county_id,
                    instrument_number=instrument,
                    grantor=row.get('Grantor'),
                )
                unmatched += 1
        
        logger.info(f"Deeds: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
