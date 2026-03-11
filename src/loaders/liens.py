"""
Lien and judgment loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import LegalAndLien

logger = logging.getLogger(__name__)


class LienLoader(BaseLoader):
    """Loader for liens and judgments."""
    
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
            # then fall back to owner name (Grantor).
            property_record = None

            # Strategy A: Legal description (lot/block/subdivision → parcel)
            if pd.notna(row.get('Legal')):
                match_result = self.find_property_by_legal_description(row['Legal'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched lien by legal desc (score: {score}%): {instrument}")

            # Strategy B: Owner name (Grantor)
            # For IRS Tax Liens (TL), Grantor = "UNITED STATES OF AMERICA" (the filer),
            # so the property owner is in Grantee instead — try that first for TL.
            doc_type_str = str(row.get('document_type', ''))
            is_tax_lien = 'TAX LIEN' in doc_type_str.upper()

            if not property_record and is_tax_lien and pd.notna(row.get('Grantee')):
                match_result = self.find_property_by_owner_name(row['Grantee'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched tax lien by grantee/owner name (score: {score}%): {instrument}")

            if not property_record and not is_tax_lien and pd.notna(row.get('Grantor')):
                match_result = self.find_property_by_owner_name(row['Grantor'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched lien by name (score: {score}%): {instrument}")
            
            if property_record:
                try:
                    # Determine record type
                    if 'JUDGMENT' in doc_type_str.upper() or 'CERTIFIED' in doc_type_str.upper():
                        record_type = 'Judgment'
                    else:
                        record_type = 'Lien'
                    
                    # Handle NaN values - convert to None
                    # For IRS tax liens: Grantor=IRS, Grantee=property owner
                    # Swap creditor/debtor so debtor always = property owner
                    if is_tax_lien:
                        creditor_val = 'INTERNAL REVENUE SERVICE'
                        debtor_raw = row.get('Grantee')
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
                logger.debug(f"No property match for lien: {instrument} (Grantor: {row.get('Grantor')})")
                unmatched += 1
                self.stats_by_doc_type[_doc_type_label]['unmatched'] += 1
        
        logger.info(f"Liens/Judgments: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
