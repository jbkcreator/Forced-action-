"""
Integration tests for the TaxDeedAuctionLoader skip-to-update fix — requires
real Postgres (fresh_db). A re-scraped case (same county_id/auction_date/
case_number) now UPDATES status/sold_amount/sold_to/opening_bid in place
instead of being silently skipped, since those fields are only known once
the auction actually resolves on a later scrape.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.core.models import Property
from src.loaders.tax_deed import TaxDeedAuctionLoader


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _row(**overrides) -> dict:
    defaults = dict(
        parcel_id="CDE09-TD-001",
        case_number="TD-2026-TEST-001",
        auction_date="01/15/2026",
        certificate_number="",
        certificate_year="",
        status="Scheduled",
        auction_type="Tax Deed",
        opening_bid="5000",
        sold_amount="",
        sold_to="",
        raw_fields="",
    )
    defaults.update(overrides)
    return defaults


class TestTaxDeedSkipToUpdate:
    def test_rescrape_updates_status_and_sold_fields_in_place(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE09-TD-001")
        loader = TaxDeedAuctionLoader(fresh_db, county_id="hillsborough")

        df1 = pd.DataFrame([_row()])
        matched1, unmatched1, skipped1 = loader.load_from_dataframe(df1)
        assert matched1 == 1
        assert skipped1 == 0

        row = fresh_db.execute(
            text(
                "SELECT id, property_id, status, sold_amount, sold_to FROM tax_deed_auctions "
                "WHERE county_id = 'hillsborough' AND case_number = 'TD-2026-TEST-001'"
            )
        ).first()
        assert row is not None
        assert row.status == "Scheduled"
        assert row.sold_amount is None
        first_id = row.id

        # Re-scrape after the auction resolves — same case, new outcome fields.
        df2 = pd.DataFrame([_row(status="Sold Third Party", sold_amount="42000", sold_to="ABC INVESTMENTS LLC")])
        matched2, unmatched2, skipped2 = loader.load_from_dataframe(df2)
        assert skipped2 == 0
        assert matched2 == 1   # updated-in-place counts as matched (existing row already had property_id)

        count = fresh_db.execute(
            text(
                "SELECT COUNT(*) FROM tax_deed_auctions "
                "WHERE county_id = 'hillsborough' AND case_number = 'TD-2026-TEST-001'"
            )
        ).scalar()
        assert count == 1   # updated in place, not duplicated

        updated = fresh_db.execute(
            text("SELECT id, property_id, status, sold_amount, sold_to FROM tax_deed_auctions WHERE id = :id"),
            {"id": first_id},
        ).first()
        assert updated.status == "Sold Third Party"
        assert float(updated.sold_amount) == 42000.0
        assert updated.sold_to == "ABC INVESTMENTS LLC"
        assert updated.property_id == prop.id   # untouched by the update

    def test_identical_rescrape_is_skipped_not_updated(self, fresh_db):
        _mk_property(fresh_db, "CDE09-TD-002")
        loader = TaxDeedAuctionLoader(fresh_db, county_id="hillsborough")

        row = _row(parcel_id="CDE09-TD-002", case_number="TD-2026-TEST-002")
        df = pd.DataFrame([row])
        loader.load_from_dataframe(df)

        # Exact same values re-scraped — nothing changed, should be a true skip.
        matched, unmatched, skipped = loader.load_from_dataframe(pd.DataFrame([row]))
        assert skipped == 1
        assert matched == 0
        assert unmatched == 0
