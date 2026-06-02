"""
Unit tests for email campaign backend — Phases B2-B6.

No live DB or Instantly API calls. All external dependencies mocked.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


# ============================================================================
# B2 — email_templates service
# ============================================================================

class TestEmailTemplates:

    def test_extract_variables(self):
        from src.services.email_templates import extract_variables
        assert extract_variables("Hi {{firstName}}, your {{company}} matters") == ["firstName", "company"]

    def test_extract_variables_deduplicates(self):
        from src.services.email_templates import extract_variables
        assert extract_variables("{{firstName}} {{firstName}}") == ["firstName"]

    def test_validate_variables_passes(self):
        from src.services.email_templates import validate_variables
        steps = [{"subject": "Hi {{firstName}}", "body": "Your company {{company}} in {{city}}"}]
        assert validate_variables(steps) == []

    def test_validate_variables_rejects_unknown(self):
        from src.services.email_templates import validate_variables
        steps = [{"subject": "Hi {{firstName}}", "body": "Your {{license_expiry}} is due"}]
        unknown = validate_variables(steps)
        assert "license_expiry" in unknown

    def test_validate_variables_empty_steps(self):
        from src.services.email_templates import validate_variables
        assert validate_variables([]) == []

    def test_collect_variables_used(self):
        from src.services.email_templates import collect_variables_used
        steps = [
            {"subject": "Hi {{firstName}}", "body": "{{company}} in {{city}}"},
            {"subject": "Follow up", "body": "{{licenseType}} license"},
        ]
        result = collect_variables_used(steps)
        assert set(result) == {"firstName", "company", "city", "licenseType"}
        assert result == sorted(result)  # must be sorted

    def test_collect_variables_ignores_unknown(self):
        from src.services.email_templates import collect_variables_used
        steps = [{"subject": "{{firstName}}", "body": "{{fakeVar}}"}]
        result = collect_variables_used(steps)
        assert "fakeVar" not in result
        assert "firstName" in result

    def test_build_instantly_sequence(self):
        from src.services.email_templates import build_instantly_sequence
        steps = [
            {"step_number": 1, "delay_days": 0, "subject": "Hi", "body": "Hello"},
            {"step_number": 2, "delay_days": 4, "subject": "Follow up", "body": "Just checking"},
        ]
        result = build_instantly_sequence(steps)
        assert len(result) == 2
        assert result[0]["type"] == "email"
        assert result[0]["delay_days"] == 0
        assert result[1]["delay_days"] == 4

    def test_resolve_contact_variables_last_first_format(self):
        from src.services.email_templates import resolve_contact_variables
        contact = {
            "full_name": "SMITH, JOHN",
            "company_name": "Smith Roofing LLC",
            "city": "Tampa",
            "license_type_desc": "Roofing Contractor",
        }
        result = resolve_contact_variables("Hi {{firstName}} from {{company}} in {{city}}", contact)
        assert "JOHN" in result
        assert "Smith Roofing LLC" in result
        assert "Tampa" in result

    def test_resolve_contact_variables_fallback_company(self):
        from src.services.email_templates import resolve_contact_variables
        contact = {"full_name": "JONES, MIKE", "company_name": None, "city": None, "license_type_desc": None}
        result = resolve_contact_variables("{{company}}", contact)
        # Falls back to first name when company_name absent
        assert result in ("MIKE", "Jones")  # first token of FIRST or full_name

    def test_resolve_unknown_var_unchanged(self):
        from src.services.email_templates import resolve_contact_variables
        contact = {"full_name": "DOE, JANE"}
        result = resolve_contact_variables("{{unknownVar}}", contact)
        assert result == "{{unknownVar}}"


# ============================================================================
# B6 — campaign_attribution
# ============================================================================

class TestCampaignAttribution:

    def _with_secret(self, monkeypatch):
        monkeypatch.setenv("ADMIN_JWT_SECRET", "test-secret-for-attribution")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.campaign_attribution as attr; reload(attr)
        return attr

    def test_encode_decode_roundtrip(self, monkeypatch):
        attr = self._with_secret(monkeypatch)
        token = attr.encode_attribution_token(42)
        assert attr.decode_attribution_token(token) == 42

    def test_tampered_token_rejected(self, monkeypatch):
        attr = self._with_secret(monkeypatch)
        token = attr.encode_attribution_token(99)
        # Flip last char
        bad_token = token[:-1] + ("X" if token[-1] != "X" else "Y")
        assert attr.decode_attribution_token(bad_token) is None

    def test_expired_token_rejected(self, monkeypatch):
        attr = self._with_secret(monkeypatch)
        # Patch time to return a past timestamp inside encode, then restore
        with patch("src.services.campaign_attribution.time.time", return_value=1_000_000):
            token = attr.encode_attribution_token(7)
        # Now time is present — token is ~25 years old, well past 90-day TTL
        assert attr.decode_attribution_token(token) is None

    def test_malformed_token_returns_none(self, monkeypatch):
        attr = self._with_secret(monkeypatch)
        assert attr.decode_attribution_token("not.a.valid.token.at.all") is None
        assert attr.decode_attribution_token("") is None
        assert attr.decode_attribution_token("only.two") is None

    def test_record_conversion_stamps_cc_and_contact(self, monkeypatch):
        self._with_secret(monkeypatch)
        from src.services.campaign_attribution import record_conversion
        from src.core.models import CampaignContact, DBPRContact

        mock_cc = MagicMock(spec=CampaignContact)
        mock_cc.dbpr_contact_id = 10
        mock_dc = MagicMock(spec=DBPRContact)
        mock_dc.is_signed_up = False

        mock_db = MagicMock()
        mock_db.get.side_effect = lambda model, pk: mock_cc if model == CampaignContact else mock_dc

        result = record_conversion(mock_db, campaign_contact_id=5, subscriber_id=99, signed_up_at=None)
        assert result is True
        assert mock_dc.is_signed_up is True
        assert mock_dc.subscriber_id == 99

    def test_record_conversion_missing_cc_returns_false(self, monkeypatch):
        self._with_secret(monkeypatch)
        from src.services.campaign_attribution import record_conversion
        from src.core.models import CampaignContact

        mock_db = MagicMock()
        mock_db.get.return_value = None  # not found

        result = record_conversion(mock_db, campaign_contact_id=999, subscriber_id=1, signed_up_at=None)
        assert result is False

    def test_email_fallback_stamps_contact(self, monkeypatch):
        self._with_secret(monkeypatch)
        from src.services.campaign_attribution import try_email_fallback
        from src.core.models import DBPRContact

        mock_dc = MagicMock(spec=DBPRContact)
        mock_dc.id = 5
        mock_dc.is_signed_up = False

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.filter.return_value.first.return_value = mock_dc

        result = try_email_fallback(mock_db, email="contractor@test.com", subscriber_id=10, signed_up_at=None)
        assert result is True
        assert mock_dc.is_signed_up is True

    def test_email_fallback_empty_email_returns_false(self, monkeypatch):
        self._with_secret(monkeypatch)
        from src.services.campaign_attribution import try_email_fallback
        mock_db = MagicMock()
        assert try_email_fallback(mock_db, email="", subscriber_id=1, signed_up_at=None) is False


# ============================================================================
# B3/B4 — email_campaigns service (name helpers + eligibility)
# ============================================================================

class TestEmailCampaignHelpers:

    def test_first_name_last_first_format(self):
        from src.services.email_campaigns import _first_name
        assert _first_name("SMITH, JOHN EDWARD") == "JOHN"

    def test_first_name_simple(self):
        from src.services.email_campaigns import _first_name
        assert _first_name("JOHN SMITH") == "JOHN"

    def test_first_name_empty(self):
        from src.services.email_campaigns import _first_name
        assert _first_name("") == ""
        assert _first_name(None) == ""

    def test_last_name_last_first_format(self):
        from src.services.email_campaigns import _last_name
        assert _last_name("SMITH, JOHN") == "SMITH"

    def test_last_name_no_comma(self):
        from src.services.email_campaigns import _last_name
        assert _last_name("JOHN SMITH") == ""

    @patch("src.services.email_campaigns.get_db_context")
    @patch("src.services.email_campaigns.list_counties")
    def test_count_eligible_calls_db(self, mock_counties, mock_db_ctx):
        mock_counties.return_value = [{"county_id": "hillsborough", "status": "launched"}]
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.scalar.return_value = 5
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        from src.services.email_campaigns import count_eligible
        result = count_eligible(
            county_id="hillsborough",
            zips=[],
            vertical="roofing",
            exclude_campaign_id=None,
        )
        assert isinstance(result, int)


# ============================================================================
# B5 — Sync task (unit — no live DB)
# ============================================================================

class TestEmailCampaignSync:

    def test_run_dry_run_returns_without_hitting_db(self):
        from src.tasks.email_campaign_sync import run
        # Patch at the source module where get_db_context is imported inside functions
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = []
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_session)
        ctx.__exit__ = MagicMock(return_value=False)
        with patch("src.core.database.get_db_context", return_value=ctx):
            result = run(dry_run=True)
        assert "started_at" in result
        assert result["campaigns"] == 0


# ============================================================================
# B8 — Cron registration (smoke check)
# ============================================================================

class TestCronRegistration:

    def test_topup_task_importable(self):
        import src.tasks.email_campaign_topup as t
        assert callable(t.run)

    def test_sync_task_importable(self):
        import src.tasks.email_campaign_sync as t
        assert callable(t.run)

    def test_router_registered(self):
        from src.api.email_campaign_router import router
        paths = {r.path for r in router.routes}
        assert "/api/admin/email-templates" in paths
        assert "/api/admin/email-campaigns" in paths
        assert "/api/admin/email-inboxes" in paths
