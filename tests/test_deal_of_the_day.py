"""
Tests for T-B12-07 deal_of_the_day service.

Coverage:
  - select_deal_of_the_day: fresh pick, already-picked idempotency, no-lead case, dry-run
  - get_current_deal: live/expired/empty states
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock


def _row(**kwargs):
    r = MagicMock()
    for k, v in kwargs.items():
        setattr(r, k, v)
    return r


class TestSelectDealOfTheDay:
    def test_already_picked_is_idempotent(self):
        from src.services.deal_of_the_day import select_deal_of_the_day
        existing = _row(id=1, lead_id=42, window_start=datetime.now(timezone.utc),
                         window_end=datetime.now(timezone.utc) + timedelta(hours=24))
        db = MagicMock()
        db.execute.return_value.first.return_value = existing

        result = select_deal_of_the_day(db)
        assert result["already_picked"] is True
        assert result["lead_id"] == 42
        db.commit.assert_not_called()

    def test_no_undelivered_lead_returns_none(self):
        from src.services.deal_of_the_day import select_deal_of_the_day
        db = MagicMock()
        db.execute.return_value.first.side_effect = [None, None]  # no existing pick, no candidate lead
        result = select_deal_of_the_day(db)
        assert result is None

    def test_dry_run_does_not_insert(self):
        from src.services.deal_of_the_day import select_deal_of_the_day
        lead = _row(
            property_id=99, address="123 Main St", city="Tampa", state="FL", zip="33601",
            county_id="hillsborough", final_cds_score=91.5, lead_tier="Platinum",
            urgency_level="high", distress_types={"foreclosure": True},
        )
        db = MagicMock()
        db.execute.return_value.first.side_effect = [None, lead]
        result = select_deal_of_the_day(db, dry_run=True)
        assert result["property_id"] == 99
        assert result["already_picked"] is False
        db.commit.assert_not_called()

    def test_fresh_pick_inserts_and_commits(self):
        from src.services.deal_of_the_day import select_deal_of_the_day
        lead = _row(
            property_id=99, address="123 Main St", city="Tampa", state="FL", zip="33601",
            county_id="hillsborough", final_cds_score=91.5, lead_tier="Platinum",
            urgency_level="high", distress_types={"foreclosure": True},
        )
        db = MagicMock()
        db.execute.return_value.first.side_effect = [None, lead]
        result = select_deal_of_the_day(db, dry_run=False)
        assert result["property_id"] == 99
        db.commit.assert_called_once()
        insert_call = db.execute.call_args_list[-1]
        assert "INSERT INTO deal_of_the_day" in str(insert_call.args[0])


class TestGetCurrentDeal:
    """
    get_current_deal now issues up to two queries: (1) the live-window query
    (window_start <= now < window_end), and only if that misses, (2) a cheap
    "was there a recently-expired row" check to distinguish "expired" from
    "never had one" (PR #172 review fix — see also
    test_live_deal_spanning_midnight_is_not_hidden_by_calendar_date below).
    These tests mock db.execute directly (no real SQL filtering happens), so
    each call's return must be supplied explicitly via side_effect in the
    same order the service issues them.
    """

    def test_empty_when_no_row(self):
        from src.services.deal_of_the_day import get_current_deal
        db = MagicMock()
        db.execute.return_value.first.side_effect = [None, None]
        result = get_current_deal(db)
        assert result["status"] == "empty"
        assert result["deal"] is None

    def test_live_within_window(self):
        from src.services.deal_of_the_day import get_current_deal
        row = _row(
            date=datetime.now(timezone.utc).date(),
            window_start=datetime.now(timezone.utc) - timedelta(hours=1),
            window_end=datetime.now(timezone.utc) + timedelta(hours=23),
            property_id=7, address="1 Test Ave", city="Tampa", state="FL", zip="33601",
            county_id="hillsborough", final_cds_score=95.0, lead_tier="Ultra Platinum",
            urgency_level="high", distress_types=["foreclosure"],
        )
        db = MagicMock()
        db.execute.return_value.first.side_effect = [row]
        result = get_current_deal(db)
        assert result["status"] == "live"
        assert result["deal"]["property_id"] == 7
        assert result["deal"]["pricing"] == "standard"

    def test_expired_after_window_end(self):
        from src.services.deal_of_the_day import get_current_deal
        db = MagicMock()
        # 1st call (live-window query): no match, window already closed.
        # 2nd call (recently-expired check): finds the closed row.
        db.execute.return_value.first.side_effect = [None, (1,)]
        result = get_current_deal(db)
        assert result["status"] == "expired"
        assert result["deal"] is None

    def test_live_deal_spanning_midnight_is_not_hidden_by_calendar_date(self):
        """
        Regression (PR #172 review): the pick's window doesn't align to
        calendar-day boundaries — it runs from whenever the daily cron
        creates it (e.g. 07:15 UTC) to +24h. A deal picked yesterday whose
        24h window is still open right now must still read "live", not
        "empty"/"expired", even though its stored `date` is yesterday.
        """
        from src.services.deal_of_the_day import get_current_deal
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        row = _row(
            date=yesterday,
            window_start=datetime.now(timezone.utc) - timedelta(hours=20),
            window_end=datetime.now(timezone.utc) + timedelta(hours=4),
            property_id=9, address="2 Test Ave", city="Tampa", state="FL", zip="33602",
            county_id="hillsborough", final_cds_score=90.0, lead_tier="Platinum",
            urgency_level="high", distress_types=["storm_damage"],
        )
        db = MagicMock()
        # Only the live-window query is hit — it must match on window bounds,
        # not on `date = CURRENT_DATE`, so a single row response is enough.
        db.execute.return_value.first.side_effect = [row]
        result = get_current_deal(db)
        assert result["status"] == "live"
        assert result["deal"]["property_id"] == 9
