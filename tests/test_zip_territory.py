"""Tests for src.services.zip_territory.claim_zip_territory.

Covers the PR #170 review finding: claim_zip_territory previously returned
None in every case (win, lose-to-another-subscriber, and the unreachable
row-deleted case), so a losing buyer's checkout proceeded to activate a
subscription with no exclusive territory. It now returns a bool the caller
MUST check, and a real two-thread test proves the atomic INSERT genuinely
serializes two concurrent first-time claims of the same ZIP.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from config.settings import get_settings
from src.services.zip_territory import claim_zip_territory


class _FakeTerritory:
    def __init__(self, status, subscriber_id):
        self.status = status
        self.subscriber_id = subscriber_id
        self.locked_at = None
        self.grace_expires_at = None


class TestClaimZipTerritoryReturnValue:
    """Mocked-db unit tests — the return-value contract itself."""

    def test_wins_atomic_insert_returns_true(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 1  # INSERT ... RETURNING id won
        result = claim_zip_territory(
            db, zip_code="33601", vertical="roofing", county_id="hillsborough",
            subscriber_id=1, now=datetime.now(timezone.utc),
        )
        assert result is True

    def test_loses_insert_but_territory_available_returns_true(self):
        db = MagicMock()
        territory = _FakeTerritory(status="available", subscriber_id=None)
        db.execute.return_value.scalar.return_value = None  # insert lost the race
        db.execute.return_value.scalar_one_or_none.return_value = territory
        result = claim_zip_territory(
            db, zip_code="33601", vertical="roofing", county_id="hillsborough",
            subscriber_id=2, now=datetime.now(timezone.utc),
        )
        assert result is True
        assert territory.status == "locked"
        assert territory.subscriber_id == 2

    def test_already_locked_to_a_different_subscriber_returns_false(self):
        """The bug this PR closes: previously this branch returned None,
        indistinguishable from a real claim to a caller checking truthiness
        loosely, and — with the old code — not checked by the caller at all."""
        db = MagicMock()
        territory = _FakeTerritory(status="locked", subscriber_id=99)
        db.execute.return_value.scalar.return_value = None
        db.execute.return_value.scalar_one_or_none.return_value = territory
        result = claim_zip_territory(
            db, zip_code="33601", vertical="roofing", county_id="hillsborough",
            subscriber_id=2, now=datetime.now(timezone.utc),
        )
        assert result is False
        assert territory.subscriber_id == 99  # untouched — not stolen from the real holder

    def test_already_locked_to_the_same_subscriber_is_idempotent_true(self):
        db = MagicMock()
        territory = _FakeTerritory(status="locked", subscriber_id=2)
        db.execute.return_value.scalar.return_value = None
        db.execute.return_value.scalar_one_or_none.return_value = territory
        result = claim_zip_territory(
            db, zip_code="33601", vertical="roofing", county_id="hillsborough",
            subscriber_id=2, now=datetime.now(timezone.utc),
        )
        assert result is True

    def test_row_vanished_between_insert_and_reselect_returns_false(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = None
        db.execute.return_value.scalar_one_or_none.return_value = None
        result = claim_zip_territory(
            db, zip_code="33601", vertical="roofing", county_id="hillsborough",
            subscriber_id=2, now=datetime.now(timezone.utc),
        )
        assert result is False


@pytest.fixture
def e2e_engine():
    settings = get_settings()
    if not settings.database_url:
        pytest.skip("DATABASE_URL not configured")
    engine = create_engine(str(settings.database_url), pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    yield engine
    engine.dispose()


@pytest.mark.scenario_platform
class TestClaimZipTerritoryRealConcurrency:
    """
    Real two-thread test against the shared Postgres DB — the reviewer's
    exact ask: "Add a concurrent-checkout regression test asserting exactly
    one buyer is activated with the ZIP." Two real DB sessions race to claim
    a brand-new (never-before-locked) ZIP for two different subscribers;
    exactly one must win, and the loser must get False, not a silent no-op.
    """

    def test_two_concurrent_claims_exactly_one_wins(self, e2e_engine):
        from sqlalchemy.orm import sessionmaker

        Session = sessionmaker(bind=e2e_engine)
        zip_code = f"9{uuid.uuid4().int % 10000:04d}"  # fake/unused-range ZIP, unique per run
        vertical = "roofing"
        county_id = "hillsborough"
        sub_ids: list[int] = []

        def _make_subscriber(conn, tag: str) -> int:
            return conn.execute(
                text(
                    "INSERT INTO subscribers "
                    "(stripe_customer_id, tier, vertical, county_id, founding_member, status, "
                    " created_at, updated_at, has_saved_card, auto_mode_enabled) "
                    "VALUES (:cust, 'starter', :vertical, :county, false, 'active', "
                    " now(), now(), false, false) RETURNING id"
                ),
                {"cust": f"cus_test_zipclaim_{tag}", "vertical": vertical, "county": county_id},
            ).scalar()

        try:
            with e2e_engine.begin() as conn:
                sub_a = _make_subscriber(conn, uuid.uuid4().hex[:8])
                sub_b = _make_subscriber(conn, uuid.uuid4().hex[:8])
            sub_ids = [sub_a, sub_b]

            results: dict[int, bool] = {}
            barrier = threading.Barrier(2)

            def _claim(subscriber_id: int):
                session = Session()
                try:
                    barrier.wait(timeout=5)  # maximize the chance both hit the INSERT together
                    results[subscriber_id] = claim_zip_territory(
                        session, zip_code=zip_code, vertical=vertical, county_id=county_id,
                        subscriber_id=subscriber_id, now=datetime.now(timezone.utc),
                    )
                    session.commit()
                finally:
                    session.close()

            t1 = threading.Thread(target=_claim, args=(sub_a,))
            t2 = threading.Thread(target=_claim, args=(sub_b,))
            t1.start(); t2.start()
            t1.join(timeout=10); t2.join(timeout=10)

            assert set(results.values()) == {True, False}, (
                f"expected exactly one winner, got {results}"
            )

            with e2e_engine.connect() as conn:
                owner = conn.execute(
                    text(
                        "SELECT subscriber_id FROM zip_territories "
                        "WHERE zip_code = :z AND vertical = :v AND county_id = :c"
                    ),
                    {"z": zip_code, "v": vertical, "c": county_id},
                ).scalar()
            winner_id = sub_a if results[sub_a] else sub_b
            assert owner == winner_id

        finally:
            with e2e_engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM zip_territories WHERE zip_code = :z AND vertical = :v AND county_id = :c"),
                    {"z": zip_code, "v": vertical, "c": county_id},
                )
                for sid in sub_ids:
                    conn.execute(text("DELETE FROM subscribers WHERE id = :s"), {"s": sid})
