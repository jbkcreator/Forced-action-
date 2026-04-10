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

                    judgment_amount_val = self.parse_amount(row.get('Judgment Amount'))

                    case_status_raw = row.get('Auction Status')
                    case_status_val = str(case_status_raw).strip() if pd.notna(case_status_raw) else None

                    # ── Upsert logic ──────────────────────────────────────────
                    # Case 1: exact case_number already exists → skip (true dup)
                    existing_exact = (
                        self.session.query(Foreclosure)
                        .filter_by(case_number=case_number)
                        .first()
                    )
                    if existing_exact:
                        if skip_duplicates:
                            logger.debug(f"Skipping duplicate foreclosure: {case_number}")
                            skipped += 1
                            continue
                        # Update auction fields if not skipping
                        if auction_date_val and existing_exact.auction_date is None:
                            existing_exact.auction_date = auction_date_val
                        if judgment_amount_val and existing_exact.judgment_amount is None:
                            existing_exact.judgment_amount = judgment_amount_val
                        if case_status_val:
                            existing_exact.case_status = case_status_val
                        self.session.flush()
                        matched += 1
                        continue

                    # Case 2: LP placeholder exists for this property (created by
                    # LisPendensLoader before auction data arrived) → merge into it
                    existing_lp = (
                        self.session.query(Foreclosure)
                        .filter(
                            Foreclosure.property_id == property_record.id,
                            Foreclosure.case_number.like('LP-%'),
                        )
                        .first()
                    )
                    if existing_lp:
                        try:
                            with self.session.begin_nested():
                                # Promote synthetic case_number to the real one
                                existing_lp.case_number = case_number
                                existing_lp.auction_date = auction_date_val
                                existing_lp.judgment_amount = judgment_amount_val
                                existing_lp.case_status = case_status_val
                                # Only set plaintiff if not already captured from LP record
                                if plaintiff_val and existing_lp.plaintiff is None:
                                    existing_lp.plaintiff = plaintiff_val
                                # Never overwrite lis_pendens_date — it came from the LP loader
                                self.session.flush()
                            self._affected_property_ids.add(property_record.id)
                            logger.info(
                                f"Merged auction data into LP placeholder: {case_number} "
                                f"(property_id={property_record.id})"
                            )
                            matched += 1
                        except Exception as e:
                            logger.error(f"Error merging LP placeholder for {case_number}: {e}")
                            unmatched += 1
                        continue

                    # Case 3: No prior row — plain insert
                    foreclosure_record = Foreclosure(
                        property_id=property_record.id,
                        case_number=case_number,
                        plaintiff=plaintiff_val,
                        filing_date=None,       # Not in realforeclose CSV
                        lis_pendens_date=None,  # Will be filled by LisPendensLoader
                        judgment_amount=judgment_amount_val,
                        auction_date=auction_date_val,
                        case_status=case_status_val,
                        county_id=self.county_id,
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
                self.quarantine_unmatched(
                    source_type="foreclosures",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    county_id=self.county_id,
                    instrument_number=str(case_number),
                    grantor=str(row.get('Plaintiff', '')),
                    address_string=str(row.get('Property Address', '')),
                )
                unmatched += 1
        
        logger.info(f"Foreclosures: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
