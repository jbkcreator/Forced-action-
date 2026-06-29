"""
Vacant Parcel Loader.

Two SQL statements, no Python loops touching the DB:
  1. unnest() INSERT — all rows in one round-trip, ON CONFLICT DO UPDATE.
  2. UPDATE JOIN    — resolves property_id for matched parcels.
     property_id stays NULL for parcels not yet in our properties table.
"""

import logging
from datetime import date
from typing import Tuple

import pandas as pd
from sqlalchemy import text as sa_text

from src.loaders.base import BaseLoader

logger = logging.getLogger(__name__)


class VacantParcelLoader(BaseLoader):

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        if df.empty:
            return 0, 0, 0

        today = date.today()

        # Build column arrays in Python — no DB calls
        county_ids, parcel_ids, use_codes, prop_uses, dor_codes, src_names, verified = (
            [], [], [], [], [], [], []
        )
        for _, row in df.iterrows():
            parcel_id = str(row.get("parcel_id", "")).strip()
            if not parcel_id:
                continue
            lv_raw = row.get("last_verified")
            if isinstance(lv_raw, str):
                try:
                    lv = date.fromisoformat(lv_raw)
                except ValueError:
                    lv = today
            elif isinstance(lv_raw, date):
                lv = lv_raw
            else:
                lv = today

            county_ids.append(self.county_id)
            parcel_ids.append(parcel_id)
            use_codes.append(str(row.get("use_code", "") or "").strip() or None)
            prop_uses.append(str(row.get("property_use", "") or "").strip() or None)
            dor_codes.append(str(row.get("dor_code", "") or "").strip() or None)
            src_names.append(str(row.get("source_name", "")).strip() or "unknown")
            verified.append(lv)

        if not parcel_ids:
            return 0, 0, 0

        existing_count = self.session.execute(
            sa_text("SELECT COUNT(*) FROM vacant_parcels WHERE county_id = :cid"),
            {"cid": self.county_id},
        ).scalar() or 0

        # Step 1: bulk upsert via unnest — one round-trip
        self.session.execute(
            sa_text("""
                INSERT INTO vacant_parcels
                    (county_id, parcel_id, use_code, property_use, dor_code,
                     source_name, last_verified, scraped_at)
                SELECT
                    unnest(:county_ids ::text[]),
                    unnest(:parcel_ids ::text[]),
                    unnest(:use_codes  ::text[]),
                    unnest(:prop_uses  ::text[]),
                    unnest(:dor_codes  ::text[]),
                    unnest(:src_names  ::text[]),
                    unnest(:verified   ::date[]),
                    NOW()
                ON CONFLICT (county_id, parcel_id) DO UPDATE SET
                    use_code      = EXCLUDED.use_code,
                    property_use  = EXCLUDED.property_use,
                    dor_code      = EXCLUDED.dor_code,
                    source_name   = EXCLUDED.source_name,
                    last_verified = EXCLUDED.last_verified,
                    scraped_at    = NOW()
            """),
            {
                "county_ids": county_ids,
                "parcel_ids": parcel_ids,
                "use_codes":  use_codes,
                "prop_uses":  prop_uses,
                "dor_codes":  dor_codes,
                "src_names":  src_names,
                "verified":   verified,
            },
        )

        # Step 2: resolve property_id via JOIN — one round-trip
        self.session.execute(
            sa_text("""
                UPDATE vacant_parcels vp
                SET property_id = p.id
                FROM properties p
                WHERE vp.county_id = :cid
                  AND p.parcel_id = replace(replace(vp.parcel_id, '-', ''), '/', '')
            """),
            {"cid": self.county_id},
        )
        self.session.flush()

        new_count = self.session.execute(
            sa_text("SELECT COUNT(*) FROM vacant_parcels WHERE county_id = :cid"),
            {"cid": self.county_id},
        ).scalar() or 0
        matched_count = self.session.execute(
            sa_text("SELECT COUNT(*) FROM vacant_parcels WHERE county_id = :cid AND property_id IS NOT NULL"),
            {"cid": self.county_id},
        ).scalar() or 0

        prop_ids = self.session.execute(
            sa_text("SELECT DISTINCT property_id FROM vacant_parcels WHERE county_id = :cid AND property_id IS NOT NULL"),
            {"cid": self.county_id},
        ).scalars().all()
        self._affected_property_ids.update(prop_ids)

        inserted = new_count - existing_count
        updated = len(parcel_ids) - inserted
        unmatched = new_count - matched_count

        logger.info(
            "[%s] VacantParcel: inserted=%d updated=%d matched=%d unmatched=%d",
            self.county_id, inserted, updated, matched_count, unmatched,
        )
        return matched_count, unmatched, updated
