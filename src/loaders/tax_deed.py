"""
Tax Deed Auction Loader.

Matches scraped tax_deed_auctions rows to the properties table via the
parcel_id cascade (exact → normalized → address fallback) and writes
TaxDeedAuction records. Deduplicates on (county_id, auction_date, case_number)
— a re-scrape of an existing case UPDATES status/sold_amount/sold_to/
opening_bid/certificate_number/raw_fields in place (a case scraped while
"Scheduled" and re-scraped after the auction resolves is the normal lifecycle
for this source, not a true duplicate) rather than being skipped outright.
property_id/match_method/match_confidence are left untouched on update so an
already-resolved match is never downgraded by a later pass.
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

            parcel_id = self.clean_str(raw.get("parcel_id"))
            case_number = self.clean_str(raw.get("case_number")) or ""
            auction_date_raw = raw.get("auction_date", "")

            if not case_number:
                logger.warning("Skipping row with no case_number: %s", raw)
                unmatched += 1
                continue

            # Parse auction date
            auction_date_obj = self.parse_date(auction_date_raw)
            if auction_date_obj is None:
                logger.warning("Could not parse auction_date %r for case %s", auction_date_raw, case_number)
                unmatched += 1
                continue
            auction_date_val: date = auction_date_obj.date() if hasattr(auction_date_obj, "date") else auction_date_obj

            # Parse raw_fields JSON if stored as string (needed for both the
            # insert path below and the update-on-duplicate path here).
            raw_fields_val = raw.get("raw_fields")
            if isinstance(raw_fields_val, str):
                try:
                    raw_fields_val = json.loads(raw_fields_val)
                except (ValueError, TypeError):
                    raw_fields_val = None

            new_status = self.clean_str(raw.get("status"))
            new_sold_amount = self.parse_amount(raw.get("sold_amount"))
            new_sold_to = self.clean_str(raw.get("sold_to"))
            new_opening_bid = self.parse_amount(raw.get("opening_bid"))
            new_certificate_number = self.clean_str(raw.get("certificate_number"))

            # Dedup check — update in place rather than skip, since a case's
            # status/sold_amount/sold_to are only known once the auction
            # actually resolves, which happens on a later re-scrape of the
            # same (county_id, auction_date, case_number).
            if skip_duplicates:
                from sqlalchemy import text as sa_text
                existing = self.session.execute(
                    sa_text(
                        "SELECT id, property_id, status, sold_amount, sold_to, opening_bid "
                        "FROM tax_deed_auctions "
                        "WHERE county_id = :cid AND auction_date = :dt AND case_number = :cn"
                    ),
                    {"cid": self.county_id, "dt": auction_date_val, "cn": case_number},
                ).first()
                if existing:
                    changed = (
                        new_status != existing.status
                        or new_sold_amount != existing.sold_amount
                        or new_sold_to != existing.sold_to
                        or new_opening_bid != existing.opening_bid
                    )
                    if changed:
                        self.session.execute(
                            sa_text(
                                "UPDATE tax_deed_auctions SET "
                                "status = :status, sold_amount = :sold_amount, sold_to = :sold_to, "
                                "opening_bid = :opening_bid, "
                                "certificate_number = COALESCE(:certificate_number, certificate_number), "
                                "raw_fields = COALESCE(CAST(:raw_fields AS JSONB), raw_fields) "
                                "WHERE id = :id"
                            ),
                            {
                                "status": new_status,
                                "sold_amount": new_sold_amount,
                                "sold_to": new_sold_to,
                                "opening_bid": new_opening_bid,
                                "certificate_number": new_certificate_number,
                                "raw_fields": json.dumps(raw_fields_val) if raw_fields_val is not None else None,
                                "id": existing.id,
                            },
                        )
                        if existing.property_id:
                            matched += 1
                        else:
                            unmatched += 1
                    else:
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

            record = TaxDeedAuction(
                property_id=prop.id if prop else None,
                county_id=self.county_id,
                parcel_id=parcel_id,
                auction_date=auction_date_val,
                case_number=case_number,
                certificate_number=new_certificate_number,
                certificate_year=_to_int(raw.get("certificate_year")),
                status=new_status,
                auction_type=self.clean_str(raw.get("auction_type")),
                opening_bid=new_opening_bid,
                sold_amount=new_sold_amount,
                sold_to=new_sold_to,
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
