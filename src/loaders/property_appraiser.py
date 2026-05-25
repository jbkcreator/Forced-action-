"""
Property Appraiser Loader — upserts HCPA enrichment data into existing property rows.

Unlike other loaders that INSERT new distress signals, this one UPDATES existing
property/owner/financial rows because we queried the properties FROM the DB first
(parcel_id is known; no matching waterfall needed).

Multi-county:
  Hillsborough → canonical pass-through (no ColumnMapper needed)
  Other counties → call ColumnMapper.get_or_create("property_appraiser", source_id, sample_df)
                   before the upsert loop (same pattern as violations.py)
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from src.loaders.base import BaseLoader
from src.core.models import Property, Owner, Financial, Deed, TaxPaymentHistory
from src.core.database import get_db_context

logger = logging.getLogger(__name__)


class PropertyAppraiserLoader(BaseLoader):
    """
    Upsert loader for HCPA property appraiser enrichment data.

    Accepts a DataFrame produced by pa_parser.to_canonical_dataframe().
    Each row must have a `parcel_id` column (and optionally `_property_id`
    for a fast direct lookup).  The special `_tax_payment_history` column
    carries a list of payment dicts that get inserted into tax_payment_history.
    """

    _LLM_MAX_CALLS: int = 0  # no LLM matching — direct parcel_id lookup

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = False,  # not used — upsert always wins
    ) -> Tuple[int, int, int]:
        """
        Upsert enrichment data for each row in df.

        Returns:
            (updated, 0, skipped)   — 0 = unmatched count (always 0 here)
        """
        # Resolve column mapping for non-Hillsborough counties
        col_mapping = self._resolve_column_mapping(df)
        if col_mapping:
            df = df.rename(columns=col_mapping)

        updated = skipped = 0

        for _, row in df.iterrows():
            parcel_id = row.get("parcel_id")
            if not parcel_id:
                skipped += 1
                continue

            try:
                did_update = self._upsert_property(row, parcel_id)
                if did_update:
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error("Error upserting parcel %s: %s", parcel_id, e)
                skipped += 1

        return updated, 0, skipped

    # ------------------------------------------------------------------
    # Core upsert logic
    # ------------------------------------------------------------------

    def _upsert_property(self, row: pd.Series, parcel_id: str) -> bool:
        """
        Upsert a single property row. Returns True if a DB row was updated.
        """
        # Fast path: _property_id was set by the engine (avoids a second query)
        prop_id = row.get("_property_id")

        prop: Optional[Property] = None
        if prop_id:
            prop = self.session.get(Property, int(prop_id))
        if prop is None:
            prop = self.session.query(Property).filter_by(parcel_id=parcel_id).first()

        if prop is None:
            logger.debug("Parcel %s not found in DB — skipping", parcel_id)
            return False

        # --- Update properties ---
        _set_if_present(prop, "year_built",             _int(row.get("year_built")))
        _set_if_present(prop, "sq_ft",                  _float(row.get("gross_sq_ft")))
        _set_if_present(prop, "beds",                   _int(row.get("beds")))
        _set_if_present(prop, "baths",                  _float(row.get("baths")))
        _set_if_present(prop, "lot_size",               _float(row.get("lot_size")))
        _set_if_present(prop, "property_use_code",      row.get("property_use_code"))
        _set_if_present(prop, "building_condition",     row.get("building_condition"))
        _set_if_present(prop, "building_class",         row.get("building_class"))
        _set_if_present(prop, "heated_sq_ft",           _float(row.get("heated_sq_ft")))
        _set_if_present(prop, "subdivision",            row.get("subdivision"))
        _set_if_present(prop, "hcpa_neighborhood_code", row.get("neighborhood_code"))
        _set_if_present(prop, "building_details",       row.get("building_details"))
        _set_if_present(prop, "legal_description",      row.get("legal_description"))
        prop.hcpa_last_refreshed = datetime.now(timezone.utc)
        # Mark for CDS rescore
        prop.sync_status = "pending_sync"

        # --- Upsert owner ---
        self._upsert_owner(prop, row)

        # --- Upsert financials ---
        self._upsert_financials(prop, row)

        # --- Insert tax payment history ---
        tax_history = row.get("_tax_payment_history")
        if isinstance(tax_history, list):
            self._insert_tax_history(prop.id, tax_history)

        return True

    def _upsert_owner(self, prop: Property, row: pd.Series) -> None:
        owner_name = row.get("owner_name")
        mailing_address = row.get("mailing_address")
        site_address = getattr(prop, "address", None)

        if not owner_name and not mailing_address:
            return

        owner: Optional[Owner] = (
            self.session.query(Owner).filter_by(property_id=prop.id).first()
        )
        if owner is None:
            owner = Owner(property_id=prop.id, county_id=self.county_id)
            self.session.add(owner)

        _set_if_present(owner, "name",           owner_name)
        _set_if_present(owner, "mailing_address", mailing_address)

        # Absentee status: matching addresses = owner-occupied, different = absentee
        if site_address and mailing_address:
            site_norm = re.sub(r"\s+", " ", site_address.upper().split(",")[0].strip())
            mail_norm = re.sub(r"\s+", " ", mailing_address.upper().split(",")[0].strip())
            owner.absentee_status = "In-County" if site_norm == mail_norm else "Out-of-County"

    def _upsert_financials(self, prop: Property, row: pd.Series) -> None:
        fin: Optional[Financial] = (
            self.session.query(Financial).filter_by(property_id=prop.id).first()
        )
        if fin is None:
            fin = Financial(property_id=prop.id, county_id=self.county_id)
            self.session.add(fin)

        # Core valuations
        _set_if_present(fin, "assessed_value_mkt",   _float(row.get("market_value")))
        _set_if_present(fin, "assessed_value_tax",   _float(row.get("county_taxable_value") or row.get("county_assessed_value")))
        _set_if_present(fin, "homestead_exempt",     bool(row.get("homestead_exempt")) if row.get("homestead_exempt") is not None else None)

        # HCPA enrichment fields
        _set_if_present(fin, "exemption_code",           row.get("exemption_code"))
        _set_if_present(fin, "soh_assessment_reduction", _float(row.get("soh_assessment_reduction")))
        _set_if_present(fin, "taxable_value_county",     _float(row.get("county_taxable_value")))
        _set_if_present(fin, "taxable_value_schools",    _float(row.get("school_taxable_value")))
        _set_if_present(fin, "prior_year_market_value",  _float(row.get("prior_year_market_value")))
        _set_if_present(fin, "proposed_next_assessed",   _float(row.get("proposed_next_assessed")))
        _set_if_present(fin, "tax_current_status",       row.get("tax_status"))
        _set_if_present(fin, "tax_last_paid_amount",     _float(row.get("tax_last_paid_amount")))
        _set_if_present(fin, "tax_last_paid_date",       row.get("tax_last_paid_date"))

        # Sales
        _set_if_present(fin, "last_sale_date",  row.get("last_sale_date"))
        _set_if_present(fin, "last_sale_price", _float(row.get("last_sale_price")))

        # Derived: value_change_yoy
        prior = _float(row.get("prior_year_market_value"))
        current = _float(row.get("market_value"))
        if prior and current and prior > 0:
            fin.value_change_yoy = round((current - prior) / prior * 100, 2)

        # price_per_sq_ft
        sq_ft = _float(row.get("heated_sq_ft") or row.get("gross_sq_ft"))
        if current and sq_ft and sq_ft > 0:
            fin.price_per_sq_ft = round(current / sq_ft, 2)

        fin.hcpa_refreshed_at = datetime.now(timezone.utc)

    def _insert_tax_history(self, property_id: int, history: list[dict]) -> None:
        """INSERT OR IGNORE tax payment rows — UniqueConstraint handles duplicates."""
        for item in history:
            try:
                # Check if row already exists (UniqueConstraint: property_id + tax_year + bill_type)
                exists = (
                    self.session.query(TaxPaymentHistory)
                    .filter_by(
                        property_id=property_id,
                        tax_year=item.get("tax_year"),
                        bill_type=item.get("bill_type"),
                    )
                    .first()
                )
                if exists:
                    continue

                row = TaxPaymentHistory(
                    property_id=property_id,
                    tax_year=item.get("tax_year"),
                    bill_type=item.get("bill_type"),
                    amount_paid=item.get("amount_paid"),
                    payment_date=item.get("payment_date"),
                    receipt_number=item.get("receipt_number"),
                    days_late=item.get("days_late"),
                    county_id=self.county_id,
                )
                self.session.add(row)
            except Exception as e:
                logger.debug("Could not insert tax history row (property %s, year %s): %s",
                             property_id, item.get("tax_year"), e)

    # ------------------------------------------------------------------
    # Column mapping (multi-county extensibility)
    # ------------------------------------------------------------------

    def _resolve_column_mapping(self, df: pd.DataFrame) -> Optional[dict]:
        """
        Return a column rename dict for non-Hillsborough counties.

        Hillsborough → None (canonical pass-through, no mapping needed).
        Other counties → look up via ColumnMapper (same pattern as violations.py).
        """
        if self.county_id == "hillsborough":
            return None

        try:
            from src.loaders.column_mapper import ColumnMapper
            from src.utils.county_config import get_county_config
            county_config = get_county_config(self.county_id)
            source_id = county_config.get("sources", {}).get("property_appraiser", {}).get("source_id")
            if not source_id:
                return None
            return ColumnMapper.get_or_create("property_appraiser", source_id, df)
        except Exception as e:
            logger.warning("Column mapping failed for county %s: %s", self.county_id, e)
            return None


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _set_if_present(obj, attr: str, value) -> None:
    """Set obj.attr = value only if value is not None."""
    if value is not None:
        setattr(obj, attr, value)


def _float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
