"""
Lis Pendens loader — routes LP doc type from Clerk Official Records CSV into the
Foreclosure table.

Lis pendens are filed at the start of foreclosure proceedings (Day ~120 after
default), giving 6-12 months of lead time over auction-stage signals from
realforeclose.com.

Upsert logic
------------
- If a Foreclosure row already exists for this property_id (auction data already
  loaded from realforeclose.com), update lis_pendens_date + plaintiff on that row
  without overwriting any auction fields.
- If no row exists, create a new one with case_number = "LP-{instrument_number}"
  as a synthetic key.  The real court case number can be updated later once it is
  extracted from the court docket PDF.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import Foreclosure

logger = logging.getLogger(__name__)

# Substrings that identify a lis pendens row in the document_type column
_LP_MARKERS = ('LIS PENDENS', '(LP)')


def _is_lp_row(doc_type_val) -> bool:
    """Return True if the document_type column value indicates a lis pendens."""
    if pd.isna(doc_type_val):
        return False
    s = str(doc_type_val).upper()
    return any(m in s for m in _LP_MARKERS)


class LisPendensLoader(BaseLoader):
    """
    Loader for lis pendens records from the Clerk of Courts Official Records CSV.

    Accepts the same full Clerk CSV used by LienLoader and DeedLoader — it
    filters internally for LP document types only.
    """

    _LLM_MAX_CALLS = 20

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        # ── Filter to LP rows only ───────────────────────────────────────────
        if 'document_type' in df.columns:
            df = df[df['document_type'].apply(_is_lp_row)].copy()
        else:
            logger.warning("LisPendensLoader: no 'document_type' column — nothing to load")
            return 0, 0, 0

        if df.empty:
            logger.info("LisPendensLoader: no LP records in this CSV")
            return 0, 0, 0

        logger.info(f"Loading {len(df)} lis pendens records")

        matched = 0
        unmatched = 0
        skipped = 0

        for _, row in df.iterrows():
            instrument = str(row.get('Instrument', '')).strip()
            synthetic_case = f"LP-{instrument}"

            # Dedup: skip if this instrument number already loaded as LP
            if skip_duplicates:
                existing_lp = (
                    self.session.query(Foreclosure)
                    .filter_by(case_number=synthetic_case)
                    .first()
                )
                if existing_lp:
                    logger.debug(f"Skipping duplicate LP: {instrument}")
                    skipped += 1
                    continue

            # ── Property matching ────────────────────────────────────────────
            # For LP records: Grantor = lender (plaintiff), Grantee = defendant
            # (property owner).  Try legal description first (most accurate),
            # then grantee name, then no address available.
            property_record = None

            if pd.notna(row.get('Legal')):
                match_result = self.find_property_by_legal_description(row['Legal'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched LP by legal desc (score: {score}%): {instrument}")

            if not property_record and pd.notna(row.get('Grantee')):
                grantee = str(row['Grantee'])
                # Skip multi-party grantee strings (defendant list with bullets/commas)
                # — they're not suitable for name matching
                if len(grantee) < 120 and grantee.count(',') <= 2:
                    match_result = self.find_property_by_owner_name(grantee, threshold=75)
                    if match_result:
                        property_record, score = match_result
                        logger.info(f"Matched LP by grantee (score: {score}%): {instrument}")
                        property_record, _ = self._apply_llm_verification(
                            raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                            current_best=property_record, match_score=score,
                            record_type='lis_pendens', match_field='Grantee',
                        )

            if property_record:
                try:
                    lis_pendens_date = self.parse_date(row.get('RecordDate'))

                    plaintiff = None
                    grantor_val = row.get('Grantor')
                    if pd.notna(grantor_val):
                        plaintiff = str(grantor_val)[:500]

                    # Check if a Foreclosure row already exists for this property
                    # (e.g. auction data loaded first from realforeclose.com)
                    existing = (
                        self.session.query(Foreclosure)
                        .filter_by(property_id=property_record.id)
                        .order_by(Foreclosure.date_added.desc())
                        .first()
                    )

                    if existing:
                        # Merge LP data into the existing row — never overwrite
                        # auction fields, never blank a field that's already set.
                        updated = False
                        if existing.lis_pendens_date is None and lis_pendens_date:
                            existing.lis_pendens_date = lis_pendens_date
                            updated = True
                        if existing.plaintiff is None and plaintiff:
                            existing.plaintiff = plaintiff
                            updated = True
                        if updated:
                            try:
                                with self.session.begin_nested():
                                    self.session.flush()
                                self._affected_property_ids.add(property_record.id)
                                logger.info(
                                    f"Updated existing foreclosure row with LP data: "
                                    f"property_id={property_record.id}"
                                )
                            except Exception as e:
                                logger.warning(f"Could not update foreclosure with LP data: {e}")
                        else:
                            logger.debug(
                                f"LP data already present on foreclosure row: "
                                f"property_id={property_record.id}"
                            )
                        matched += 1
                    else:
                        # No prior row — create new placeholder with synthetic case_number
                        record = Foreclosure(
                            property_id=property_record.id,
                            case_number=synthetic_case,
                            plaintiff=plaintiff,
                            lis_pendens_date=lis_pendens_date,
                            filing_date=lis_pendens_date,
                            county_id=self.county_id,
                        )
                        if self.safe_add(record):
                            matched += 1
                        else:
                            unmatched += 1

                except Exception as e:
                    logger.error(f"Error building LP record {instrument}: {e}")
                    unmatched += 1
            else:
                logger.debug(
                    f"No property match for LP: {instrument} "
                    f"(Grantor: {row.get('Grantor')}, Grantee: {str(row.get('Grantee', ''))[:60]})"
                )
                self.quarantine_unmatched(
                    source_type="lis_pendens",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    county_id=self.county_id,
                    instrument_number=instrument,
                    grantor=str(row.get('Grantor', '')),
                    address_string=str(row.get('Grantee', ''))[:500],
                )
                unmatched += 1

        logger.info(
            f"Lis Pendens: {matched} matched, {unmatched} unmatched, {skipped} skipped"
        )
        return matched, unmatched, skipped
