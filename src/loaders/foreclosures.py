"""
Foreclosure loader.
"""

import logging
from typing import Optional, Tuple

import pandas as pd

from src.loaders.base import (
    BaseLoader,
    MATCH_METHOD_PARCEL_ID,
    MATCH_METHOD_NORM_ADDR,
    MATCH_METHOD_OWNER_NAME,
    MATCH_METHOD_OWNER_ZIP,
    MATCH_METHOD_OWNER_CITY,
)
from src.loaders._address_utils import split_address
from src.core.models import Foreclosure

logger = logging.getLogger(__name__)


class ForeclosureLoader(BaseLoader):
    """Loader for foreclosure records."""

    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True,
        **kwargs,
    ) -> Tuple[int, int, int]:
        """
        Load foreclosures from a CSV file, applying ColumnMapper before processing.

        The mapper looks up (or LLM-generates) an approved column mapping for this
        county's foreclosures source so arbitrary CSV column names are renamed to the
        canonical names expected by load_from_dataframe().
        """
        logger.info("[ForeclosureLoader] Loading from CSV: %s", csv_path)

        col_mapping: Optional[dict] = self._resolve_column_mapping(csv_path)

        try:
            df = pd.read_csv(csv_path, dtype=str, on_bad_lines='warn')
        except Exception:
            df = pd.read_csv(csv_path, dtype=str, engine='python', on_bad_lines='warn')

        if col_mapping:
            from src.loaders.column_mapper import ColumnMapper
            df = ColumnMapper.apply(df, col_mapping)

        return self.load_from_dataframe(df, skip_duplicates=skip_duplicates, **kwargs)

    def _resolve_column_mapping(self, csv_path: str) -> Optional[dict]:
        """
        Peek at the CSV header, look up the CountySource for this county's foreclosures
        signal, and return a mapping dict via ColumnMapper.

        Returns None if no source row exists (columns assumed already canonical) or if
        the signal type has no schema defined.
        """
        from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
        from src.core.models import CountySource

        src = (
            self.session.query(CountySource)
            .filter_by(county_id=self.county_id, signal_type="foreclosures")
            .first()
        )
        if src is None:
            logger.debug(
                "[ForeclosureLoader] No county_sources row for %s/foreclosures — "
                "skipping column mapping (columns assumed canonical)",
                self.county_id,
            )
            return None

        sample_df = pd.read_csv(csv_path, dtype=str, nrows=5)

        try:
            mapper = ColumnMapper()
            return mapper.get_or_create("foreclosures", src.id, sample_df)
        except SkipMapping:
            return None
        except NeedsMappingError as e:
            logger.error(
                "[ForeclosureLoader] Column mapping required but LLM failed — "
                "create a mapping via admin UI before loading. Error: %s", e,
            )
            raise

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

            # ── Cascade match ─────────────────────────────────────────────
            # Foreclosures have Parcel ID + Property Address. Some records may
            # also include a Defendant name (fa028) — use it if present.
            property_record = None
            match_score = 0
            match_method = None

            addr_val = row.get('Property Address')
            addr_str = str(addr_val) if pd.notna(addr_val) else None
            zip_v = city_v = None
            if addr_str:
                _, city_v, zip_v = split_address(addr_str)

            defendant_val = row.get('Defendant') if 'Defendant' in row else None
            defendant_str = str(defendant_val).strip() if pd.notna(defendant_val) else None

            prop, method, score = self.find_property_cascade(
                parcel_id=str(row.get('Parcel ID')).strip() if pd.notna(row.get('Parcel ID')) else None,
                address=addr_str,
                owner_name=defendant_str,
                zip_code=zip_v,
                city=city_v,
                addr_threshold=self._thresholds.address_floor,
                owner_threshold=self._thresholds.owner_name_floor,
            )
            if prop:
                property_record, match_method, match_score = prop, method, score
                logger.info(f"Matched foreclosure by {method} (score: {score}%): {case_number}")

            if property_record:
                tier = self._classify_match(match_score, match_method)
                if tier == "matched":
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

                        winning_bid_val = self.parse_amount(row.get('Winning Bid')) if 'Winning Bid' in row else None
                        sold_to_raw = row.get('Sold To')
                        sold_to_val = str(sold_to_raw).strip() if pd.notna(sold_to_raw) else None

                        # ── Upsert logic ──────────────────────────────────────────
                        # Case 1: exact case_number already exists → update outcome
                        # fields in place if they changed (a case scraped while
                        # "Waiting" and re-scraped once resolved is the normal
                        # lifecycle here, not a true duplicate), else skip.
                        # case_number carries its own unique constraint, so this
                        # lookup always runs regardless of skip_duplicates.
                        existing_exact = (
                            self.session.query(Foreclosure)
                            .filter_by(case_number=case_number, county_id=self.county_id)
                            .first()
                        )
                        if existing_exact:
                            changed = (
                                (auction_date_val and existing_exact.auction_date is None)
                                or (judgment_amount_val and existing_exact.judgment_amount is None)
                                or (case_status_val and case_status_val != existing_exact.case_status)
                                or (winning_bid_val and winning_bid_val != existing_exact.winning_bid)
                                or (sold_to_val and sold_to_val != existing_exact.sold_to)
                            )
                            if not changed:
                                logger.debug(f"Skipping unchanged foreclosure: {case_number}")
                                skipped += 1
                                continue
                            if auction_date_val and existing_exact.auction_date is None:
                                existing_exact.auction_date = auction_date_val
                            if judgment_amount_val and existing_exact.judgment_amount is None:
                                existing_exact.judgment_amount = judgment_amount_val
                            if case_status_val:
                                existing_exact.case_status = case_status_val
                            if winning_bid_val:
                                existing_exact.winning_bid = winning_bid_val
                            if sold_to_val:
                                existing_exact.sold_to = sold_to_val
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
                                    existing_lp.winning_bid = winning_bid_val
                                    existing_lp.sold_to = sold_to_val
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
                            winning_bid=winning_bid_val,
                            sold_to=sold_to_val,
                            match_confidence=round(match_score / 100.0, 3),
                            match_method=match_method,
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
                    logger.debug(f"Pending review foreclosure: {case_number} (score: {match_score}%, method: {match_method})")
                    self.quarantine_unmatched(
                        source_type="foreclosures",
                        raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                        county_id=self.county_id,
                        instrument_number=str(case_number),
                        grantor=str(row.get('Plaintiff', '')),
                        address_string=str(row.get('Property Address', '')),
                        match_status="pending_review",
                        match_confidence=match_score / 100.0,
                        candidate_property_id=property_record.id,
                        match_method=match_method,
                    )
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
