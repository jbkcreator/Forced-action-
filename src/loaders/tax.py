"""
Tax delinquency loader.
"""

import logging
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import TaxDelinquency

logger = logging.getLogger(__name__)


class TaxDelinquencyLoader(BaseLoader):
    """Loader for tax delinquency records."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load tax delinquencies from DataFrame.

        Behaviour:
        - New records (property + tax_year not seen before) → INSERT
        - Existing records → UPSERT: update total_amount_due / years_delinquent /
          certificate_data if the incoming row carries non-null values for those fields.
          This lets an admin re-upload a refreshed county file to enrich existing rows
          without creating duplicates.

        Duplicate detection uses a pre-loaded set of (property_id, tax_year) pairs
        fetched in a single query at the start — eliminates per-row DB round-trips.

        Returns:
            Tuple of (matched, updated, unmatched)
            where matched = new inserts, updated = existing records refreshed
        """
        logger.info("Loading %d tax delinquency rows", len(df))

        # ── Pre-load existing (property_id, tax_year) → TaxDelinquency.id ──────
        # One query instead of N per-row duplicate checks.
        existing_map: dict[tuple, int] = {}  # (property_id, tax_year) → td.id
        if skip_duplicates:
            rows = self.session.query(
                TaxDelinquency.id,
                TaxDelinquency.property_id,
                TaxDelinquency.tax_year,
            ).all()
            for td_id, prop_id, yr in rows:
                existing_map[(prop_id, yr)] = td_id
            logger.info("Pre-loaded %d existing tax delinquency keys", len(existing_map))

        matched = 0
        updated = 0
        unmatched = 0

        for _, row in df.iterrows():
            account_number = str(row['Account Number']).strip()
            parcel_id = account_number.lstrip('A')

            property_record = self.find_property_by_parcel_id(parcel_id)

            if not property_record:
                logger.warning("No property match for parcel: %s (searched as: %s)", account_number, parcel_id)
                self.quarantine_unmatched(
                    source_type="tax_delinquencies",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    instrument_number=str(account_number),
                )
                unmatched += 1
                continue

            tax_year = int(row['Tax Yr']) if pd.notna(row.get('Tax Yr')) else None

            # ── Parse enrichment fields ───────────────────────────────────────
            try:
                years_delinquent_val = row.get('years_delinquent_scraped')
                years_delinquent_val = int(years_delinquent_val) if pd.notna(years_delinquent_val) else None

                amount_val = self.parse_amount(row.get('total_amount_due'))

                cert_status = row.get('Cert Status', '')
                deed_status = row.get('Deed Status', '')
                certificate_data = None
                parts = []
                if pd.notna(cert_status) and cert_status != '-- None --':
                    parts.append(f"Cert: {cert_status}")
                if pd.notna(deed_status) and deed_status != '-- None --':
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
                # UPSERT — refresh non-null enrichment fields on the existing row
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
                    if changed:
                        self.session.flush()
                        updated += 1
                        logger.debug("Updated tax record for %s year %s", account_number, tax_year)
                    # If nothing changed, count as neither matched nor updated (silent skip)
            else:
                # INSERT new record
                try:
                    tax_record = TaxDelinquency(
                        property_id=property_record.id,
                        tax_year=tax_year,
                        years_delinquent=years_delinquent_val,
                        total_amount_due=amount_val,
                        certificate_data=certificate_data,
                        deed_app_date=None,
                    )
                    if self.safe_add(tax_record):
                        existing_map[key] = tax_record.id  # keep map consistent within run
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
