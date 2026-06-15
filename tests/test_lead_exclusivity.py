"""
Behavioral tests for cross-trade lead exclusivity (S0c / 414 scope).

Real-Postgres tests use the `fresh_db` fixture and self-skip when DATABASE_URL
is unset or the lead_exclusivity table has not been migrated yet. Advisory
locks and ON CONFLICT upserts are Postgres-only, so SQLite is not used here.
"""
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import text

from src.services.lead_exclusivity import (
    acquire_zip_lock,
    get_exclusive_property_ids,
    record_exclusivity,
    clear_exclusivity_for_purchase,
    purge_expired,
)


def _table_exists(db) -> bool:
    return db.execute(text("SELECT to_regclass('public.lead_exclusivity')")).scalar() is not None


@pytest.fixture
def db(fresh_db):
    if not _table_exists(fresh_db):
        pytest.skip("lead_exclusivity table not migrated — run migrations/001_lead_exclusivity.sql")
    return fresh_db


ZIP = "33601"
COUNTY = "hillsborough"


def _record(db, pids, trade, source="lead_pack", source_id=1, hours=72, zip_code=ZIP):
    record_exclusivity(
        db=db, property_ids=pids, zip_code=zip_code, county_id=COUNTY,
        trade=trade, source=source, source_id=source_id,
        exclusive_until=datetime.now(timezone.utc) + timedelta(hours=hours),
    )


class TestCrossTradeExclusivity:
    def test_roundtrip_records_and_reads(self, db):
        _record(db, [101, 102], "roofing")
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP) == {101, 102}

    def test_buyer_trade_sees_own_others_hidden(self, db):
        # Sold to roofing → hidden from restoration, visible to roofing.
        _record(db, [201], "roofing")
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP, exclude_trade="restoration") == {201}
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP, exclude_trade="roofing") == set()

    def test_county_wide_spans_multiple_zips(self, db):
        _record(db, [301], "roofing", zip_code="33601")
        _record(db, [302], "roofing", source_id=2, zip_code="33602")
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now) == {301, 302}
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code="33601") == {301}

    def test_expired_rows_excluded(self, db):
        _record(db, [401], "roofing", hours=-1)  # already expired
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP) == set()


class TestUpsertReSale:
    def test_resale_same_property_source_does_not_raise(self, db):
        # The bug this guards: UNIQUE(property_id, source) used to crash a
        # legitimate re-sale after expiry. Upsert must refresh instead.
        _record(db, [501], "roofing", source_id=1, hours=-1)
        _record(db, [501], "restoration", source_id=2, hours=72)  # must not raise
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP) == {501}
        # Row was refreshed to the new trade.
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP, exclude_trade="restoration") == set()


class TestClearAndPurge:
    def test_clear_for_purchase_releases_leads(self, db):
        _record(db, [601, 602], "roofing", source_id=42)
        assert clear_exclusivity_for_purchase(db, 42, "lead_pack") == 2
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP) == set()

    def test_purge_expired_only(self, db):
        _record(db, [701], "roofing", source_id=1, hours=72)
        _record(db, [702], "roofing", source_id=2, hours=-1)
        purged = purge_expired(db)
        assert purged >= 1
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, COUNTY, now, zip_code=ZIP) == {701}


class TestAdvisoryLock:
    def test_acquire_does_not_raise(self, db):
        # Bind-param path (no SQL injection): must execute cleanly.
        acquire_zip_lock(db, ZIP, COUNTY)
        acquire_zip_lock(db, "33'; DROP TABLE x; --", COUNTY)  # malicious input is safe


class TestIsCountyLaunched:
    def test_source_county_is_launched(self, fresh_db):
        from src.utils.county_config import is_county_launched
        from config.settings import get_settings
        src = get_settings().county_launch_source_county
        if not src:
            pytest.skip("no source county configured")
        assert is_county_launched(src, fresh_db) is True

    def test_unknown_county_not_launched(self, fresh_db):
        from src.utils.county_config import is_county_launched
        assert is_county_launched("no_such_county_xyz", fresh_db) is False
