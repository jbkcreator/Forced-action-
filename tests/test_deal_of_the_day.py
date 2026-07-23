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
    def test_empty_when_no_row(self):
        from src.services.deal_of_the_day import get_current_deal
        db = MagicMock()
        db.execute.return_value.first.return_value = None
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
        db.execute.return_value.first.return_value = row
        result = get_current_deal(db)
        assert result["status"] == "live"
        assert result["deal"]["property_id"] == 7
        assert result["deal"]["pricing"] == "standard"

    def test_expired_after_window_end(self):
        from src.services.deal_of_the_day import get_current_deal
        row = _row(
            date=datetime.now(timezone.utc).date(),
            window_start=datetime.now(timezone.utc) - timedelta(hours=25),
            window_end=datetime.now(timezone.utc) - timedelta(hours=1),
            property_id=7, address="1 Test Ave", city="Tampa", state="FL", zip="33601",
            county_id="hillsborough", final_cds_score=95.0, lead_tier="Ultra Platinum",
            urgency_level="high", distress_types=["foreclosure"],
        )
        db = MagicMock()
        db.execute.return_value.first.return_value = row
        result = get_current_deal(db)
        assert result["status"] == "expired"
        assert result["deal"] is None
