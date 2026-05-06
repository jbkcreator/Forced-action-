"""
Unit tests for Partner tier service.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call


def _make_sub(sub_id=1, tier="annual_lock", status="active"):
    sub = MagicMock()
    sub.id = sub_id
    sub.tier = tier
    sub.status = status
    return sub


def _make_zt(zip_code, status="locked"):
    zt = MagicMock()
    zt.zip_code = zip_code
    zt.status = status
    return zt


class TestEligibility:
    def test_lock_holder_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(tier="annual_lock")) is True

    def test_ap_lite_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(tier="autopilot_lite")) is True

    def test_annual_lock_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(tier="annual_lock")) is True

    def test_wallet_user_not_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(tier="wallet")) is False

    def test_starter_not_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(tier="starter")) is False

    def test_grace_status_not_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(status="grace")) is False

    def test_paused_status_not_eligible(self):
        from src.services.partner_tier import is_eligible
        assert is_eligible(_make_sub(status="paused")) is False


class TestZipValidation:
    def _mock_db(self, locked_zips=None):
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = (
            [_make_zt(z) for z in (locked_zips or [])]
        )
        return db

    def test_available_zips_pass(self):
        from src.services.partner_tier import validate_zip_selection
        db = self._mock_db()
        result = validate_zip_selection(db, "roofing", "fl_hillsborough", ["33647", "33602"])
        assert result["ok"] is True

    def test_max_5_zips_default(self):
        from src.services.partner_tier import validate_zip_selection
        db = self._mock_db()
        result = validate_zip_selection(db, "roofing", "fl_hillsborough", ["1", "2", "3", "4", "5", "6"])
        assert result["ok"] is False
        assert result["reason"] == "max_zips_exceeded"

    def test_locked_zip_rejected(self):
        from src.services.partner_tier import validate_zip_selection
        db = self._mock_db(locked_zips=["33647"])
        result = validate_zip_selection(db, "roofing", "fl_hillsborough", ["33647", "33602"])
        assert result["ok"] is False
        assert result["reason"] == "zips_already_locked"
        assert "33647" in result["zips"]

    def test_empty_zip_list_rejected(self):
        from src.services.partner_tier import validate_zip_selection
        db = self._mock_db()
        result = validate_zip_selection(db, "roofing", "fl_hillsborough", [])
        assert result["ok"] is False

    def test_all_locked_returns_full_list(self):
        from src.services.partner_tier import validate_zip_selection
        db = self._mock_db(locked_zips=["33647", "33602"])
        result = validate_zip_selection(db, "roofing", "fl_hillsborough", ["33647", "33602"])
        assert set(result["zips"]) == {"33647", "33602"}


class TestProvisioning:
    def _make_db(self, subscriber, partner_sub=None, existing_territories=None):
        db = MagicMock()
        db.get.return_value = subscriber

        # partner_sub lookup
        # territory lookups
        results = [None] * 10  # default: no existing territory
        if existing_territories:
            results = existing_territories

        db.execute.return_value.scalar_one_or_none.side_effect = [partner_sub] + results

        return db

    def test_tier_flipped_to_partner(self):
        from src.services.partner_tier import provision_partner_access
        sub = _make_sub(tier="annual_lock")
        db = self._make_db(sub)
        provision_partner_access(db, 1, ["33647"], "roofing", "fl_hillsborough")
        assert sub.tier == "partner"

    def test_creates_partner_subscription_row(self):
        from src.services.partner_tier import provision_partner_access
        sub = _make_sub(tier="annual_lock")
        db = self._make_db(sub)
        provision_partner_access(db, 1, ["33647", "33602"], "roofing", "fl_hillsborough")
        db.add.assert_called()
        added_types = [type(c.args[0]).__name__ for c in db.add.call_args_list]
        assert "PartnerSubscription" in added_types

    def test_creates_zip_territory_if_missing(self):
        from src.services.partner_tier import provision_partner_access
        sub = _make_sub(tier="annual_lock")
        db = self._make_db(sub)
        provision_partner_access(db, 1, ["33647", "33602"], "roofing", "fl_hillsborough")
        added_types = [type(c.args[0]).__name__ for c in db.add.call_args_list]
        assert added_types.count("ZipTerritory") == 2

    def test_locks_existing_available_territory(self):
        from src.services.partner_tier import provision_partner_access
        from src.core.models import ZipTerritory
        sub = _make_sub(tier="annual_lock")
        existing_zt = _make_zt("33647", status="available")

        db = MagicMock()
        db.get.return_value = sub
        db.execute.return_value.scalar_one_or_none.side_effect = [None, existing_zt]

        provision_partner_access(db, 1, ["33647"], "roofing", "fl_hillsborough")
        assert existing_zt.status == "locked"
        assert existing_zt.subscriber_id == 1

    def test_missing_subscriber_is_no_op(self):
        from src.services.partner_tier import provision_partner_access
        db = MagicMock()
        db.get.return_value = None
        provision_partner_access(db, 99, ["33647"], "roofing", "fl_hillsborough")
        db.add.assert_not_called()
