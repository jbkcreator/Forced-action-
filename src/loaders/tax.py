"""
Tax delinquency loader.
"""

import logging
from datetime import date
from typing import Optional, Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import TaxDelinquency, CountySource, Property

logger = logging.getLogger(__name__)


class TaxDelinquencyLoader(BaseLoader):
    """Loader for tax delinquency records."""

    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """Load tax delinquencies from CSV, applying ColumnMapper if a mapping exists."""
        col_mapping = self._resolve_column_mapping(csv_path)
        df = pd.read_csv(csv_path, dtype=str)
        if col_mapping:
            from src.loaders.column_mapper import ColumnMapper
            df = ColumnMapper.apply(df, col_mapping)
        return self.load_from_dataframe(df, skip_duplicates=skip_duplicates)

    def _resolve_column_mapping(self, csv_path: str) -> Optional[dict]:
        from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
        src = self.session.query(CountySource).filter_by(
            county_id=self.county_id, signal_type="tax_delinquency"
        ).first()
        if src is None:
            return None
        sample_df = pd.read_csv(csv_path, dtype=str, nrows=5)
        try:
            mapper = ColumnMapper()
            return mapper.get_or_create("tax_delinquency", src.id, sample_df)
        except SkipMapping:
            return None
        except NeedsMappingError as e:
            logger.error("[TaxDelinquencyLoader] Column mapping required but LLM failed: %s", e)
            raise

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """
        Load tax delinquencies from DataFrame.

        Behaviour:
        - New records (property + tax_year not seen before) → INSERT
        - Existing records → UPSERT: update total_amount_due / years_delinquent /
          certificate_data if the incoming row carries non-null values.

        Duplicate detection uses a pre-loaded (property_id, tax_year) set scoped
        to this county — one query instead of N per-row round-trips.

        Returns:
            Tuple of (matched, updated, unmatched)
        """
        logger.info("Loading %d tax delinquency rows (county=%s)", len(df), self.county_id)

        # Pre-load existing (property_id, tax_year) → TaxDelinquency.id, scoped to county
        existing_map: dict[tuple, int] = {}
        if skip_duplicates:
            rows = (
                self.session.query(
                    TaxDelinquency.id,
                    TaxDelinquency.property_id,
                    TaxDelinquency.tax_year,
                )
                .join(Property, Property.id == TaxDelinquency.property_id)
                .filter(Property.county_id == self.county_id)
                .all()
            )
            for td_id, prop_id, yr in rows:
                existing_map[(prop_id, yr)] = td_id
            logger.info("Pre-loaded %d existing tax delinquency keys", len(existing_map))

        matched = 0
        updated = 0
        unmatched = 0

        for _, row in df.iterrows():
            account_number = str(row.get('Account Number', '')).strip()
            if not account_number or account_number == 'nan':
                unmatched += 1
                continue

            parcel_id = account_number.lstrip('A')
            property_record = self.find_property_by_parcel_id(parcel_id)

            if not property_record:
                logger.warning(
                    "No property match for parcel: %s (searched as: %s)",
                    account_number, parcel_id,
                )
                self.quarantine_unmatched(
                    source_type="tax_delinquencies",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    instrument_number=str(account_number),
                )
                unmatched += 1
                continue

            tax_yr_raw = row.get('Tax Yr')
            tax_year = int(tax_yr_raw) if pd.notna(tax_yr_raw) else None

            try:
                # SNIPER enrichment columns take priority; fall back to bulk download columns
                years_raw = row.get('years_delinquent_scraped') or row.get('Years Delinquent')
                years_delinquent_val = int(years_raw) if pd.notna(years_raw) else None

                amount_raw = row.get('total_amount_due') or row.get('Total Due')
                amount_val = self.parse_amount(amount_raw)

                cert_status = row.get('Cert Status', '')
                deed_status = row.get('Deed Status', '')
                certificate_data = None
                parts = []
                if pd.notna(cert_status) and cert_status not in ('', '-- None --', 'nan'):
                    parts.append(f"Cert: {cert_status}")
                if pd.notna(deed_status) and deed_status not in ('', '-- None --', 'nan'):
                    parts.append(f"Deed: {deed_status}")
                if parts:
                    certificate_data = ", ".join(parts)

            except Exception as e:
                logger.error("Error parsing fields for %s: %s", account_number, e)
                unmatched += 1
                continue

            key = (property_record.id, tax_year)
            existing_id = existing_map.get(key) if skip_duplicates else None

            if existing_id:
                td_row = self.session.query(TaxDelinquency).get(existing_id)
                if td_row:
                    changed = False
                    if amount_val is not None and td_row.total_amount_due != amount_val:
                        td_row.total_amount_due = amount_val
                        changed = True
                    if years_delinquent_val is not None and td_row.years_delinquent != years_delinquent_val:
                        td_row.years_delinquent = years_delinquent_val
                        changed = True
                    if certificate_data and td_row.certificate_data != certificate_data:
                        td_row.certificate_data = certificate_data
                        changed = True
                    td_row.date_added = date.today()
                    self.session.flush()
                    if changed:
                        updated += 1
                        logger.debug("Updated tax record for %s year %s", account_number, tax_year)
            else:
                try:
                    tax_record = TaxDelinquency(
                        property_id=property_record.id,
                        tax_year=tax_year,
                        years_delinquent=years_delinquent_val,
                        total_amount_due=amount_val,
                        certificate_data=certificate_data,
                        deed_app_date=None,
                        county_id=self.county_id,
                    )
                    if self.safe_add(tax_record):
                        existing_map[key] = tax_record.id
                        matched += 1
                    else:
                        unmatched += 1
                except Exception as e:
                    logger.error("Error inserting tax record for %s: %s", account_number, e)
                    unmatched += 1

        logger.info(
            "Tax delinquencies: %d inserted, %d updated, %d unmatched",
            matched, updated, unmatched,
        )
        return matched, updated, unmatched
