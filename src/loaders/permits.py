"""
Building permit loader.
"""

import logging
import re
from datetime import date
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import text

_COMPLETION_STATUS_MAP: dict[str, str] = {
    "issued": "issued",
    "active": "active",
    "open": "active",
    "in review": "active",
    "under review": "active",
    "approved": "issued",
    "expired": "expired",
    "revoked": "expired",
    "cancelled": "expired",
    "cancel": "expired",
    "completed": "completed",
    "finaled": "completed",
    "closed": "completed",
    "pending": "pending",
    "received": "pending",
    "submitted": "pending",
}

from src.loaders.base import BaseLoader
from src.core.models import BuildingPermit, CountySource, PermitStaging

# Hillsborough County permit_type substrings that indicate an enforcement permit
_ENFORCEMENT_TYPE_KEYWORDS = frozenset({
    "code compliance case",
})
# Status values that indicate enforcement (exact match, case-insensitive)
# "awaiting client reply" = owner not responding, work stalled — strong distress signal
# "expired" / "revoked" = permit lapsed without completion — owner stalled or non-compliant
_ENFORCEMENT_STATUS_VALUES = frozenset({
    "withdrawn", "cancel", "awaiting client reply", "waiting on applicant",
    "expired", "revoked",
})

def _is_enforcement(permit_type: str | None, status: str | None, expire_date) -> bool:
    """Return True if this permit qualifies as an enforcement permit."""
    pt = (permit_type or "").lower()
    st = (status or "").lower().strip()

    if any(kw in pt for kw in _ENFORCEMENT_TYPE_KEYWORDS):
        return True
    if st in _ENFORCEMENT_STATUS_VALUES:
        return True

    return False

logger = logging.getLogger(__name__)


def _normalize_completion_status(raw: str | None) -> str | None:
    if not raw:
        return None
    return _COMPLETION_STATUS_MAP.get(raw.lower().strip())


def _parse_job_value(raw) -> float | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    try:
        cleaned = str(raw).replace("$", "").replace(",", "").strip()
        val = float(cleaned)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


class BuildingPermitLoader(BaseLoader):
    """Loader for building permits."""

    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """Load permits from CSV, applying ColumnMapper if a mapping exists."""
        col_mapping = self._resolve_column_mapping(csv_path)
        df = pd.read_csv(csv_path, dtype=str)
        if col_mapping:
            from src.loaders.column_mapper import ColumnMapper
            df = ColumnMapper.apply(df, col_mapping)
        return self.load_from_dataframe(df, skip_duplicates=skip_duplicates)

    def _resolve_column_mapping(self, csv_path: str) -> Optional[dict]:
        from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
        src = self.session.query(CountySource).filter_by(
            county_id=self.county_id, signal_type="permits"
        ).first()
        if src is None:
            return None
        sample_df = pd.read_csv(csv_path, dtype=str, nrows=5)
        try:
            mapper = ColumnMapper()
            return mapper.get_or_create("permits", src.id, sample_df)
        except SkipMapping:
            return None
        except NeedsMappingError as e:
            logger.error("[BuildingPermitLoader] Column mapping required but LLM failed: %s", e)
            raise

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load permits from DataFrame.
        
        Args:
            df: DataFrame with columns: Date, Record Number, Record Type, Address, Status, Expiration Date
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
            
        Note: BuildingPermit model has: permit_number, permit_type, issue_date, expire_date, status
        """
        logger.info(f"Loading {len(df)} building permits")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            record_number = str(row['Record Number']).strip()
            description_val = str(row.get('Description') or '').strip() or None

            # Check for duplicates; update description if previously NULL
            if skip_duplicates:
                existing_row = self.session.execute(
                    text("""
                        SELECT id, description, status, holder_name, contractor_name
                        FROM building_permits WHERE permit_number = :pnum LIMIT 1
                    """),
                    {"pnum": record_number},
                ).fetchone()
                if existing_row:
                    incoming_status = str(row.get('Status') or '').strip() or None
                    description_changed = existing_row.description is None and description_val
                    status_changed = incoming_status and incoming_status != existing_row.status
                    if description_changed or status_changed:
                        self.session.execute(
                            text("""
                                UPDATE building_permits
                                SET description       = COALESCE(description, :desc),
                                    status            = CASE WHEN :status IS NOT NULL THEN :status ELSE status END,
                                    holder_name       = COALESCE(holder_name, :holder_name),
                                    contractor_name   = COALESCE(contractor_name, :contractor_name),
                                    job_value         = COALESCE(job_value, :job_value),
                                    completion_status = COALESCE(completion_status, :completion_status)
                                WHERE id = :id
                            """),
                            {
                                "desc": description_val,
                                "status": incoming_status,
                                "holder_name": str(row.get('Holder Name') or '').strip() or None,
                                "contractor_name": str(row.get('Contractor Name') or '').strip() or None,
                                "job_value": _parse_job_value(row.get('Job Value')),
                                "completion_status": _normalize_completion_status(incoming_status),
                                "id": existing_row.id,
                            },
                        )
                        self.session.flush()
                    skipped += 1
                    continue
            

            # Match by address — extract ZIP from raw address if present
            property_record = None
            if pd.notna(row.get('Address')):
                raw_address = str(row['Address'])
                # Match zip after state abbreviation to avoid capturing 5-digit house numbers
                zip_match = re.search(r'\bFL\s+(\d{5})\b', raw_address, re.IGNORECASE)
                if not zip_match:
                    zip_match = re.search(r',\s*(\d{5})(?:-\d{4})?\s*$', raw_address)
                zip_code = zip_match.group(1) if zip_match else None
                match_result = self.find_property_by_address(
                    raw_address, zip_code=zip_code, strict_house_number=False,
                    threshold=self._thresholds.address_floor,
                )
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched permit by address (score: {score}%): {record_number}")
            
            # Enrichment fields shared by both branches
            permit_type_val = row.get('Record Type')
            if pd.isna(permit_type_val) if isinstance(permit_type_val, float) else False:
                permit_type_val = None
            status_val = row.get('Status')
            if pd.isna(status_val) if isinstance(status_val, float) else False:
                status_val = None

            raw_holder = str(row.get('Holder Name') or '').strip() or None
            raw_contractor = str(row.get('Contractor Name') or '').strip() or None
            job_value_val = _parse_job_value(row.get('Job Value'))
            completion_status_val = _normalize_completion_status(status_val)
            parsed_issue = self.parse_date(row.get('Date'))
            parsed_expire = self.parse_date(row.get('Expiration Date'))

            if property_record:
                try:
                    enforcement = _is_enforcement(permit_type_val, status_val, parsed_expire)

                    permit_record = BuildingPermit(
                        property_id=property_record.id,
                        permit_number=record_number,
                        permit_type=permit_type_val,
                        status=status_val,
                        issue_date=parsed_issue,
                        expire_date=parsed_expire,
                        is_enforcement_permit=enforcement,
                        county_id=self.county_id,
                        description=description_val,
                        holder_name=raw_holder,
                        contractor_name=raw_contractor,
                        job_value=job_value_val,
                        completion_status=completion_status_val,
                    )

                    if self.safe_add(permit_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building permit {record_number}: {e}")
                    unmatched += 1
            else:
                logger.info(f"Permit {record_number} unmatched — staging for builder engine")
                self._persist_to_staging(
                    permit_number=record_number,
                    permit_type=permit_type_val,
                    address=str(row.get('Address', '')),
                    holder_name=raw_holder,
                    contractor_name=raw_contractor,
                    job_value=job_value_val,
                    completion_status=completion_status_val,
                    status=status_val,
                    description=description_val,
                    issue_date=parsed_issue,
                    expire_date=parsed_expire,
                )
                self.quarantine_unmatched(
                    source_type="permits",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    address_string=str(row.get('Address', '')),
                    instrument_number=str(record_number),
                )
                unmatched += 1
        
        logger.info(f"Building Permits: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped

    def _persist_to_staging(
        self,
        *,
        permit_number: str,
        permit_type: str | None,
        address: str,
        holder_name: str | None,
        contractor_name: str | None,
        job_value: float | None,
        completion_status: str | None,
        status: str | None,
        description: str | None,
        issue_date,
        expire_date,
    ) -> None:
        """Upsert an unmatched permit into permit_staging."""
        self.session.execute(
            text("""
                INSERT INTO permit_staging (
                    permit_number, permit_type, county_id, address,
                    holder_name, contractor_name, job_value,
                    completion_status, status, description,
                    issue_date, expire_date, date_added, matched
                ) VALUES (
                    :permit_number, :permit_type, :county_id, :address,
                    :holder_name, :contractor_name, :job_value,
                    :completion_status, :status, :description,
                    :issue_date, :expire_date, CURRENT_DATE, FALSE
                )
                ON CONFLICT (permit_number) DO UPDATE SET
                    status            = EXCLUDED.status,
                    completion_status = EXCLUDED.completion_status,
                    holder_name       = COALESCE(EXCLUDED.holder_name, permit_staging.holder_name),
                    contractor_name   = COALESCE(EXCLUDED.contractor_name, permit_staging.contractor_name),
                    job_value         = COALESCE(EXCLUDED.job_value, permit_staging.job_value),
                    description       = COALESCE(EXCLUDED.description, permit_staging.description)
            """),
            {
                "permit_number": permit_number,
                "permit_type": permit_type,
                "county_id": self.county_id,
                "address": address,
                "holder_name": holder_name,
                "contractor_name": contractor_name,
                "job_value": job_value,
                "completion_status": completion_status,
                "status": status,
                "description": description,
                "issue_date": issue_date,
                "expire_date": expire_date,
            },
        )


PermitLoader = BuildingPermitLoader
