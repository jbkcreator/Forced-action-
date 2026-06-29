"""
Tax Deed Auction Loader.

Matches scraped tax_deed_auctions rows to the properties table via the
parcel_id cascade (exact → normalized → address fallback) and writes
TaxDeedAuction records. Deduplicates on (county_id, auction_date, case_number).
"""

import json
import logging
from datetime import date
from typing import Tuple

import pandas as pd

from src.loaders.base import BaseLoader, MATCH_METHOD_PARCEL_ID
from src.core.models import TaxDeedAuction

logger = logging.getLogger(__name__)


class TaxDeedAuctionLoader(BaseLoader):

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        matched = unmatched = skipped = 0

        for _, row in df.iterrows():
            raw = dict(row)

            parcel_id = str(raw.get("parcel_id", "")).strip() or None
            case_number = str(raw.get("case_number", "")).strip()
            auction_date_raw = raw.get("auction_date", "")

            if not case_number:
                logger.warning("Skipping row with no case_number: %s", raw)
                unmatched += 1
                continue

            # Parse auction date
            auction_date_obj = self.parse_date(str(auction_date_raw))
            if auction_date_obj is None:
                logger.warning("Could not parse auction_date %r for case %s", auction_date_raw, case_number)
                unmatched += 1
                continue
            auction_date_val: date = auction_date_obj.date() if hasattr(auction_date_obj, "date") else auction_date_obj

            # Dedup check
            if skip_duplicates:
                from sqlalchemy import text as sa_text
                existing = self.session.execute(
                    sa_text(
                        "SELECT id FROM tax_deed_auctions "
                        "WHERE county_id = :cid AND auction_date = :dt AND case_number = :cn"
                    ),
                    {"cid": self.county_id, "dt": auction_date_val, "cn": case_number},
                ).first()
                if existing:
                    skipped += 1
                    continue

            # Property matching — parcel_id first, address fallback
            prop = self.find_property_by_parcel_id(parcel_id) if parcel_id else None
            if prop:
                match_method = MATCH_METHOD_PARCEL_ID
                match_score = 100
            else:
                prop, match_method, match_score = self.find_property_cascade(
                    parcel_id=parcel_id,
                )
                if not prop:
                    self.quarantine_unmatched(
                        source_type="tax_deed_auction",
                        raw_row=raw,
                        county_id=self.county_id,
                        instrument_number=case_number,
                        address_string=str(raw.get("parcel_id", "")),
                        match_status="unmatched",
                    )

            # Parse raw_fields JSON if stored as string
            raw_fields_val = raw.get("raw_fields")
            if isinstance(raw_fields_val, str):
                try:
                    raw_fields_val = json.loads(raw_fields_val)
                except (ValueError, TypeError):
                    raw_fields_val = None

            record = TaxDeedAuction(
                property_id=prop.id if prop else None,
                county_id=self.county_id,
                parcel_id=parcel_id,
                auction_date=auction_date_val,
                case_number=case_number,
                certificate_number=str(raw.get("certificate_number", "")).strip() or None,
                certificate_year=_to_int(raw.get("certificate_year")),
                status=str(raw.get("status", "")).strip() or None,
                auction_type=str(raw.get("auction_type", "")).strip() or None,
                opening_bid=self.parse_amount(str(raw.get("opening_bid", ""))),
                sold_amount=self.parse_amount(str(raw.get("sold_amount", ""))),
                sold_to=str(raw.get("sold_to", "")).strip() or None,
                raw_fields=raw_fields_val,
                match_method=match_method,
                match_confidence=round(match_score / 100.0, 3) if match_score is not None else None,
            )

            if self.safe_add(record):
                if prop:
                    matched += 1
                    self._affected_property_ids.add(prop.id)
                else:
                    unmatched += 1
            else:
                unmatched += 1

        logger.info(
            "[%s] TaxDeedAuction: matched=%d unmatched=%d skipped=%d",
            self.county_id, matched, unmatched, skipped,
        )
        return matched, unmatched, skipped


def _to_int(val) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
