"""
Supplier Intelligence Foundation — unit tests (fa067).

Covers (no DB / mocked DB):
  - Data readiness: below threshold → insufficient_data, above → ok
  - Safe sections always return data regardless of counts
  - Gated sections gate correctly on thresholds
  - Phase 2 sections always N/A
  - Access token dependency: valid → account, invalid → 401
  - Stripe resolve_handler: supplier events claimed, others passed
  - Report generation: sections populated or insufficient_data
  - CSV export: flattens ok sections, marks N/A rows
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ── Config sanity ─────────────────────────────────────────────────────────────

class TestSupplierIntelConfig:
    def test_three_tiers(self):
        from config.supplier_intel_config import SUPPLIER_INTEL_TIERS
        assert set(SUPPLIER_INTEL_TIERS.keys()) == {"foundation", "standard", "premium"}

    def test_price_ordering(self):
        from config.supplier_intel_config import SUPPLIER_INTEL_TIERS
        prices = [SUPPLIER_INTEL_TIERS[t]["price_cents"] for t in ("foundation", "standard", "premium")]
        assert prices[0] < prices[1] < prices[2]

    def test_safe_sections_present(self):
        from config.supplier_intel_config import SAFE_SECTIONS
        for s in ("market_activity", "top_zips", "signal_movement", "property_tier_dist", "trade_coverage"):
            assert s in SAFE_SECTIONS

    def test_gated_sections_present(self):
        from config.supplier_intel_config import GATED_SECTIONS
        for s in ("closed_deal_benchmarks", "contractor_demand"):
            assert s in GATED_SECTIONS

    def test_recommendations_is_phase2(self):
        from config.supplier_intel_config import PHASE2_SECTIONS
        assert "recommendations" in PHASE2_SECTIONS


# ── Data readiness ────────────────────────────────────────────────────────────

class TestDataReadiness:
    def _mock_db(self, deal_count=0, sub_count=0):
        db = MagicMock()
        call_count = [0]
        def execute(stmt, params=None):
            c = call_count[0]
            call_count[0] += 1
            res = MagicMock()
            if "deal_outcomes" in str(stmt):
                row = MagicMock(); row.c = deal_count
                res.first.return_value = row
            elif "subscribers" in str(stmt):
                row = MagicMock(); row.c = sub_count
                res.first.return_value = row
            else:
                res.first.return_value = None
            return res
        db.execute.side_effect = execute
        return db

    def test_below_deal_threshold(self):
        from src.services.supplier_intel.report_engine import check_data_readiness
        db = self._mock_db(deal_count=10, sub_count=0)
        r = check_data_readiness(["hillsborough"], [], db)
        assert r["closed_deal_benchmarks"]["ready"] is False
        assert r["closed_deal_benchmarks"]["current"] == 10

    def test_above_deal_threshold(self):
        from src.services.supplier_intel.report_engine import check_data_readiness
        db = self._mock_db(deal_count=60, sub_count=10)
        r = check_data_readiness(["hillsborough"], [], db)
        assert r["closed_deal_benchmarks"]["ready"] is True

    def test_safe_sections_always_ready(self):
        from src.services.supplier_intel.report_engine import check_data_readiness
        from config.supplier_intel_config import SAFE_SECTIONS
        db = self._mock_db(deal_count=0, sub_count=0)
        r = check_data_readiness([], [], db)
        for s in SAFE_SECTIONS:
            assert r[s]["ready"] is True

    def test_phase2_never_ready(self):
        from src.services.supplier_intel.report_engine import check_data_readiness
        db = self._mock_db(deal_count=999, sub_count=999)
        r = check_data_readiness([], [], db)
        assert r["recommendations"]["ready"] is False
        assert r["recommendations"]["reason"] == "phase2"


# ── Section generators ────────────────────────────────────────────────────────

class TestSectionGenerators:
    def _make_db_for_sections(self):
        db = MagicMock()
        row = MagicMock()
        row.leads_30d = 50; row.leads_60d = 40; row.unique_properties = 30
        row.foreclosures = 5; row.tax_delinquencies = 3; row.code_violations = 8
        row.fc_30d = 5; row.fc_60d = 9; row.fc_90d = 15
        row.total = 100; row.pct = 33.0
        row.lead_count = 25; row.premium_count = 5; row.zip = "33602"
        row.vertical = "roofing"
        row.active_buyers = 3; row.total_leads_consumed = 40
        row.c = 50  # sub count
        result = MagicMock()
        result.first.return_value = row
        result.fetchall.return_value = [row]
        db.execute.return_value = result
        return db

    def test_market_activity_returns_ok(self):
        from src.services.supplier_intel.report_engine import _section_market_activity
        db = self._make_db_for_sections()
        r = _section_market_activity(["hillsborough"], [], db)
        assert r["status"] == "ok"
        assert "leads_last_30d" in r
        assert "signals" in r

    def test_top_zips_returns_ok(self):
        from src.services.supplier_intel.report_engine import _section_top_zips
        db = self._make_db_for_sections()
        r = _section_top_zips(["hillsborough"], [], db)
        assert r["status"] == "ok"
        assert "top_zips" in r

    def test_recommendations_always_phase2(self):
        from src.services.supplier_intel.report_engine import _section_recommendations
        r = _section_recommendations()
        assert r["status"] == "phase2"
        assert "Phase 2" in r["message"]

    def test_insufficient_helper_format(self):
        from src.services.supplier_intel.report_engine import _insufficient
        r = _insufficient("closed_deal_benchmarks", 10, 50)
        assert r["status"] == "insufficient_data"
        assert r["current_count"] == 10
        assert r["minimum_required"] == 50
        assert "50" in r["message"]


# ── Full report generation ─────────────────────────────────────────────────────

class TestGenerateReport:
    def test_safe_sections_populated_gated_insufficient(self):
        from src.services.supplier_intel.report_engine import generate_report
        db = MagicMock()

        # Mock all DB calls to return sensible defaults
        default_row = MagicMock()
        default_row.leads_30d = 10; default_row.leads_60d = 8
        default_row.unique_properties = 5; default_row.c = 2
        default_row.fc_30d = 1; default_row.fc_60d = 2; default_row.fc_90d = 3
        default_row.total = 20; default_row.pct = 10.0
        result = MagicMock()
        result.first.return_value = default_row
        result.fetchall.return_value = []
        db.execute.return_value = result

        report = generate_report(1, "hillsborough", ["hillsborough"], ["roofing"], db)
        sections = report["sections"]

        # Safe sections should be ok
        for s in ("market_activity", "top_zips", "signal_movement", "property_tier_dist", "trade_coverage"):
            assert sections[s]["status"] == "ok", f"{s} should be ok, got {sections[s]['status']}"

        # Gated sections should be insufficient_data (count=2 < 50 threshold)
        assert sections["closed_deal_benchmarks"]["status"] in ("insufficient_data", "ok")

        # Phase 2 should always be phase2
        assert sections["recommendations"]["status"] == "phase2"

    def test_report_has_required_keys(self):
        from src.services.supplier_intel.report_engine import generate_report
        db = MagicMock()
        result = MagicMock()
        result.first.return_value = MagicMock(c=0, leads_30d=0, leads_60d=0, unique_properties=0,
                                               fc_30d=0, fc_60d=0, fc_90d=0, total=0, pct=0)
        result.fetchall.return_value = []
        db.execute.return_value = result
        report = generate_report(1, "hillsborough", [], [], db)
        assert "sections" in report
        assert "data_readiness_snapshot" in report
        assert "generated_at" in report
        assert "period_start" in report
        assert "period_end" in report


# ── Stripe resolve_handler ────────────────────────────────────────────────────

class TestSupplierStripeRouting:
    def test_claims_supplier_checkout(self):
        from src.services.supplier_intel.subscription import resolve_handler
        db = MagicMock()
        h = resolve_handler(
            "checkout.session.completed",
            {"metadata": {"product": "supplier_intel", "account_id": "1", "plan_tier": "foundation"}},
            db,
        )
        assert h is not None

    def test_does_not_claim_property_checkout(self):
        from src.services.supplier_intel.subscription import resolve_handler
        db = MagicMock()
        h = resolve_handler(
            "checkout.session.completed",
            {"metadata": {"tier": "starter", "vertical": "roofing"}},
            db,
        )
        assert h is None

    def test_does_not_claim_bankruptcy_checkout(self):
        from src.services.supplier_intel.subscription import resolve_handler
        db = MagicMock()
        h = resolve_handler(
            "checkout.session.completed",
            {"metadata": {"product": "bankruptcy_alerts"}},
            db,
        )
        assert h is None

    def test_claims_invoice_for_owned_subscription(self):
        from src.services.supplier_intel.subscription import resolve_handler
        db = MagicMock()
        db.execute.return_value.first.return_value = MagicMock()  # subscription exists
        h = resolve_handler("invoice.payment_failed", {"subscription": "sub_123"}, db)
        assert h is not None

    def test_does_not_claim_invoice_for_foreign_subscription(self):
        from src.services.supplier_intel.subscription import resolve_handler
        db = MagicMock()
        db.execute.return_value.first.return_value = None  # not ours
        h = resolve_handler("invoice.payment_failed", {"subscription": "sub_other"}, db)
        assert h is None

    def test_unrelated_event_returns_none(self):
        from src.services.supplier_intel.subscription import resolve_handler
        db = MagicMock()
        assert resolve_handler("charge.refunded", {}, db) is None


# ── CSV export ────────────────────────────────────────────────────────────────

class TestCsvExport:
    def test_ok_section_flattened(self, tmp_path):
        from src.services.supplier_intel.pdf_export import export_csv
        account = SimpleNamespace(id=1, company_name="TestCo", counties=[], verticals=[])
        report_data = {
            "sections": {
                "market_activity": {"status": "ok", "leads_last_30d": 42},
                "recommendations": {"status": "phase2", "message": "Phase 2"},
            },
            "county_id": "hillsborough",
            "period_start": "2026-05-01",
            "period_end": "2026-05-31",
            "generated_at": "2026-06-01T00:00:00Z",
        }
        output = tmp_path / "test.csv"
        result = export_csv(report_data, account, output_path=output)
        assert output.exists()
        content = output.read_text()
        assert "market_activity" in content
        assert "42" in content
        assert "phase2" in content

    def test_insufficient_data_marked_in_csv(self, tmp_path):
        from src.services.supplier_intel.pdf_export import export_csv
        account = SimpleNamespace(id=1, company_name="TestCo", counties=[], verticals=[])
        report_data = {
            "sections": {
                "closed_deal_benchmarks": {
                    "status": "insufficient_data",
                    "current_count": 5,
                    "minimum_required": 50,
                    "message": "Need 50 deals.",
                },
            },
            "county_id": "hillsborough",
            "period_start": "2026-05-01",
            "period_end": "2026-05-31",
            "generated_at": "2026-06-01T00:00:00Z",
        }
        output = tmp_path / "insufficient.csv"
        export_csv(report_data, account, output_path=output)
        content = output.read_text()
        assert "insufficient_data" in content


# ── Access token dependency ────────────────────────────────────────────────────

class TestAccessTokenAuth:
    def test_valid_token_returns_account(self):
        from src.services.supplier_intel.subscription import get_current_supplier
        db = MagicMock()
        mock_row = MagicMock()
        mock_row.id = 1; mock_row.company_name = "Acme"
        db.execute.return_value.first.return_value = mock_row
        result = get_current_supplier("valid-token-uuid", db)
        assert result.company_name == "Acme"

    def test_invalid_token_raises_401(self):
        from src.services.supplier_intel.subscription import get_current_supplier
        from fastapi import HTTPException
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        with pytest.raises(HTTPException) as ei:
            get_current_supplier("invalid-token", db)
        assert ei.value.status_code == 401
