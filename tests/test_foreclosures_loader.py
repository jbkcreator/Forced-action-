"""
Integration tests for the ForeclosureLoader re-scrape update fix — requires
real Postgres (fresh_db). A re-scraped case (same case_number) now UPDATES
case_status/winning_bid/sold_to in place instead of being unconditionally
skipped, since those fields are only known once the auction actually
resolves on a later scrape.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.core.models import Property
from src.loaders.foreclosures import ForeclosureLoader


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _row(**overrides) -> dict:
    defaults = {
        "Case Number": "292026CA000001TEST01",
        "Parcel ID": "FCL-TEST-001",
        "Property Address": "FCL-TEST-001 TEST ST",
        "Defendant": "",
        "Plaintiff": "TEST BANK NA",
        "Auction Start Date/Time": "2026-06-15",
        "Judgment Amount": "257987.90",
        "Auction Status": "Waiting",
        "Winning Bid": "",
        "Sold To": "",
    }
    defaults.update(overrides)
    return defaults


class TestForeclosureRescrapeUpdate:
    def test_rescrape_updates_outcome_fields_in_place(self, fresh_db):
        _mk_property(fresh_db, "FCL-TEST-001")
        loader = ForeclosureLoader(fresh_db, county_id="hillsborough")

        df1 = pd.DataFrame([_row()])
        matched1, unmatched1, skipped1 = loader.load_from_dataframe(df1)
        assert matched1 == 1

        row = fresh_db.execute(
            text("SELECT id, case_status, winning_bid, sold_to FROM foreclosures WHERE case_number = :cn"),
            {"cn": "292026CA000001TEST01"},
        ).first()
        assert row is not None
        assert row.case_status == "Waiting"
        assert row.winning_bid is None
        first_id = row.id

        # Re-scrape after the auction resolves.
        df2 = pd.DataFrame([_row(**{
            "Auction Status": "06/15/2026 10:02 AM ET",
            "Winning Bid": "181100.00",
            "Sold To": "Plaintiff",
        })])
        matched2, unmatched2, skipped2 = loader.load_from_dataframe(df2)
        assert matched2 == 1
        assert skipped2 == 0

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM foreclosures WHERE case_number = :cn"),
            {"cn": "292026CA000001TEST01"},
        ).scalar()
        assert count == 1   # updated in place, not duplicated

        updated = fresh_db.execute(
            text("SELECT case_status, winning_bid, sold_to FROM foreclosures WHERE id = :id"),
            {"id": first_id},
        ).first()
        assert updated.case_status == "06/15/2026 10:02 AM ET"
        assert float(updated.winning_bid) == 181100.00
        assert updated.sold_to == "Plaintiff"

    def test_identical_rescrape_is_skipped_not_updated(self, fresh_db):
        _mk_property(fresh_db, "FCL-TEST-002")
        loader = ForeclosureLoader(fresh_db, county_id="hillsborough")

        row = _row(**{"Case Number": "292026CA000002TEST02", "Parcel ID": "FCL-TEST-002",
                       "Property Address": "FCL-TEST-002 TEST ST"})
        loader.load_from_dataframe(pd.DataFrame([row]))

        matched, unmatched, skipped = loader.load_from_dataframe(pd.DataFrame([row]))
        assert skipped == 1
        assert matched == 0
