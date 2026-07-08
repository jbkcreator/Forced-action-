"""
Unit tests (pure): classify().
Integration tests (fresh_db): real Postgres, seeded Foreclosure rows ->
staged OutcomeCandidate rows.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import text

from src.connectors.foreclosure_outcomes import classify, stage_outcomes
from src.connectors.outcomes import (
    EVENT_TYPE_AUCTION_CANCELLED,
    EVENT_TYPE_AUCTION_REVERTED_TO_LENDER,
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
)
from src.core.models import Foreclosure, Property


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_foreclosure(session, property_id, case_number, **overrides) -> Foreclosure:
    defaults = dict(
        property_id=property_id,
        county_id="hillsborough",
        case_number=case_number,
        auction_date=datetime(2026, 6, 15),
        case_status=None,
        winning_bid=None,
        sold_to=None,
    )
    defaults.update(overrides)
    f = Foreclosure(**defaults)
    session.add(f)
    session.flush()
    return f


# ---------------------------------------------------------------------------
# Unit tests — classify(), no DB
# ---------------------------------------------------------------------------

class TestClassify:
    def test_third_party_bidder(self):
        assert classify("3rd Party Bidder", "01/21/2026 11:26 AM ET") == EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY

    def test_plaintiff(self):
        assert classify("Plaintiff", "06/15/2026 10:02 AM ET") == EVENT_TYPE_AUCTION_REVERTED_TO_LENDER

    def test_sold_to_wins_over_case_status(self):
        # sold_to is the confirmed-reliable signal; case_status is irrelevant once it's set.
        assert classify("3rd Party Bidder", "Canceled per County") == EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY

    def test_canceled_fallback(self):
        assert classify(None, "Canceled per County") == EVENT_TYPE_AUCTION_CANCELLED
        assert classify("", "Canceled per Bankruptcy") == EVENT_TYPE_AUCTION_CANCELLED

    def test_redeemed_not_classified_here(self):
        # No auction_* event type exists for redemption -- left unclassified by design.
        assert classify(None, "Redeemed") is None

    def test_waiting_unclassified(self):
        assert classify(None, "Waiting") is None

    def test_nothing_at_all_unclassified(self):
        assert classify(None, None) is None


# ---------------------------------------------------------------------------
# Integration tests — real Postgres (fresh_db)
# ---------------------------------------------------------------------------

class TestStageOutcomesPG:
    def test_third_party_and_plaintiff_and_cancelled_and_unclassified(self, fresh_db):
        prop = _mk_property(fresh_db, "FCO-001")

        _mk_foreclosure(fresh_db, prop.id, "FCO-CASE-SOLD", sold_to="3rd Party Bidder",
                         winning_bid=Decimal("181100.00"), case_status="06/15/2026 10:02 AM ET")
        _mk_foreclosure(fresh_db, prop.id, "FCO-CASE-REVERTED", sold_to="Plaintiff",
                         winning_bid=Decimal("257987.90"), case_status="06/15/2026 10:02 AM ET")
        _mk_foreclosure(fresh_db, prop.id, "FCO-CASE-CANCELLED", case_status="Canceled per County")
        _mk_foreclosure(fresh_db, prop.id, "FCO-CASE-WAITING", case_status="Waiting")

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        rows = fresh_db.execute(
            text("SELECT event_type, amount, counterparty FROM outcome_candidates "
                 "WHERE source_type = 'foreclosure_outcomes' AND property_id = :pid ORDER BY source_id"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 3   # sold, reverted, cancelled -- waiting stays unclassified
        event_types = {r.event_type for r in rows}
        assert event_types == {
            EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
            EVENT_TYPE_AUCTION_REVERTED_TO_LENDER,
            EVENT_TYPE_AUCTION_CANCELLED,
        }

    def test_no_auction_date_is_skipped(self, fresh_db):
        # LP-placeholder rows (created by LisPendensLoader before auction data
        # arrives) have no auction_date yet -- must not raise, must not stage.
        prop = _mk_property(fresh_db, "FCO-003")
        _mk_foreclosure(fresh_db, prop.id, "LP-FCO-CASE-003", auction_date=None, sold_to=None)

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "FCO-002")
        _mk_foreclosure(fresh_db, prop.id, "FCO-CASE-IDEMPOTENT", sold_to="Plaintiff", winning_bid=Decimal("100.00"))

        stage_outcomes(fresh_db, "hillsborough")
        stage_outcomes(fresh_db, "hillsborough")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates WHERE source_type = 'foreclosure_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
