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

# Noise party substrings to exclude from grantee candidate matching.
# These appear in every LP filing but are never the property owner.
_NOISE_PARTIES = (
    'UNKNOWN SPOUSE', 'UNKNOWN TENANT', 'UNKNOWN HEIR', 'UNKNOWN PARTY',
    'UNKNOWN OCCUPANT', 'FLORIDA DEPARTMENT', 'UNITED STATES', 'SECRETARY OF',
    'DEPARTMENT OF', 'HILLSBOROUGH COUNTY', 'CITY OF ', 'INTERNAL REVENUE',
    'ANY AND ALL', 'AS TRUSTEE', 'AS NOMINEE', 'AS SUCCESSOR', 'SUCCESSOR IN',
    'MORTGAGE ELECTRONIC', 'MERS', 'FLHSMV', 'FHFC', 'FLORIDA HOUSING',
    'INDIVIDUALLY', 'THROUGH UNDER', 'CLAIMING BY',
)


def _extract_owner_candidates(grantee_raw: str) -> list:
    """
    Split a multi-party LP grantee string into individual name candidates,
    filtering out non-owner noise parties (banks, government, unknowns).
    Returns up to 3 candidates — the real property owner is almost always first.
    """
    parts = [p.strip() for p in grantee_raw.split(',') if p.strip()]
    candidates = []
    for part in parts:
        upper = part.upper()
        if any(noise in upper for noise in _NOISE_PARTIES):
            continue
        # Skip very short fragments and obvious boilerplate
        if len(part) < 4:
            continue
        candidates.append(part)
        if len(candidates) >= 3:
            break
    return candidates


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
            # Strategy priority:
            #   1. Parcel ID from Legal text (direct, 100% confidence)
            #   2. Legal description parsing (LOT/BLOCK/SUBDIVISION)
            #   3. Grantee name matching + LLM verification
            property_record = None

            # Strategy 1: Extract parcel ID from the Legal field
            if pd.notna(row.get('Legal')):
                parcel_ids = self.extract_parcel_ids_from_text(row['Legal'])
                for pid in parcel_ids:
                    prop = self.find_property_by_parcel_id(pid)
                    if prop:
                        property_record = prop
                        logger.info(f"Matched LP by parcel ID {pid}: {instrument}")
                        break

            # Strategy 2: Legal description parsing
            if not property_record and pd.notna(row.get('Legal')):
                match_result = self.find_property_by_legal_description(row['Legal'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched LP by legal desc (score: {score}%): {instrument}")

            if not property_record and pd.notna(row.get('Grantee')):
                grantee_raw = str(row['Grantee'])
                # LP grantee fields are multi-party defendant lists.
                # Split by comma, filter out non-owner noise parties, try each candidate.
                candidates = _extract_owner_candidates(grantee_raw)
                for candidate in candidates:
                    match_result = self.find_property_by_owner_name(candidate, threshold=75)
                    if match_result:
                        property_record, score = match_result
                        logger.info(f"Matched LP by grantee candidate '{candidate}' (score: {score}%): {instrument}")
                        property_record, _ = self._apply_llm_verification(
                            raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                            current_best=property_record, match_score=score,
                            record_type='lis_pendens', match_field='Grantee',
                        )
                        break

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
