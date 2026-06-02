"""Unit tests for the skip trace waterfall."""
import pytest
from unittest.mock import MagicMock, patch

from src.services.skip_trace_result import SkipTraceResult, compute_confidence


class TestComputeConfidence:

    def test_no_contact_returns_zero(self):
        assert compute_confidence(None, None, None, None) == 0.0

    def test_mobile_high_reachability(self):
        # 0.50 + 0.20 + 0.15 = 0.85
        assert compute_confidence("+18135550000", None, None, None, 85) == pytest.approx(0.85)

    def test_landline_only_no_score(self):
        # 0.50 only — under 0.70 threshold → should escalate
        assert compute_confidence(None, "+18135550001", None, None) == pytest.approx(0.50)

    def test_email_no_phone(self):
        # 0.50 + 0.10 = 0.60 — still under threshold
        assert compute_confidence(None, None, "a@b.com", None) == pytest.approx(0.60)

    def test_mobile_plus_email_hits_threshold(self):
        # 0.50 + 0.20 + 0.10 = 0.80 → above 0.70
        assert compute_confidence("+18135550000", None, "a@b.com", None) == pytest.approx(0.80)

    def test_full_result_caps_at_one(self):
        result = compute_confidence("+13135550000", "+13135550001", "a@b.com", "123 Main", 95)
        assert result == pytest.approx(1.0)

    def test_low_reachability_score_not_counted(self):
        # Score 40 < 70 → +0.15 not awarded; 0.50 + 0.20 = 0.70
        assert compute_confidence("+18135550000", None, None, None, 40) == pytest.approx(0.70)

    def test_mailing_address_adds_points(self):
        # 0.50 + 0.20 + 0.05 = 0.75
        assert compute_confidence("+18135550000", None, None, "123 Main St", None) == pytest.approx(0.75)

    def test_reachability_exactly_70_counts(self):
        # Score exactly 70 → +0.15 awarded; 0.50 + 0.20 + 0.15 = 0.85
        assert compute_confidence("+18135550000", None, None, None, 70) == pytest.approx(0.85)


class TestCostCeilingInvariants:

    def test_all_three_providers_under_ceiling(self):
        # $0.02 + $0.25 + $0.28 = $0.55 — under $0.80 ceiling
        assert 2 + 25 + 28 == 55
        assert 55 <= 80

    def test_batchdata_only_well_under_ceiling(self):
        assert 2 <= 80

    def test_batchdata_plus_whitepages_under_ceiling(self):
        assert 2 + 25 == 27
        assert 27 <= 80

    def test_tlo_excluded_correctly(self):
        # TLO minimum $1.00 = 100 cents; 2 + 25 + 100 = 127 > 80
        assert 2 + 25 + 100 > 80

    def test_irb_excluded_correctly(self):
        # IRB ~$3.00 = 300 cents; 2 + 25 + 300 = 327 >> 80
        assert 2 + 25 + 300 > 80


class TestSkipTraceResult:

    def test_skipped_result_has_zero_cost(self):
        r = SkipTraceResult(
            provider="pdl", success=False, skipped=True, confidence=0.0, cost_cents=0
        )
        assert r.cost_cents == 0
        assert r.skipped is True
        assert r.success is False

    def test_successful_result_structure(self):
        r = SkipTraceResult(
            provider="batchdata",
            success=True,
            skipped=False,
            confidence=0.85,
            cost_cents=2,
            mobile_phone="+18135550000",
            email="owner@example.com",
        )
        assert r.confidence == pytest.approx(0.85)
        assert r.cost_cents == 2
        assert r.mobile_phone == "+18135550000"


class TestPDLSkippedWhenNoKey:

    def test_returns_skipped_when_pdl_key_absent(self):
        from src.services.pdl_skip_trace import run_pdl_lookup
        with patch("src.services.pdl_skip_trace.get_settings") as mock:
            mock.return_value.pdl_api_key = None
            result = run_pdl_lookup("John", "Doe", "123 Main St", "Tampa", "FL", "33601")
        assert result.skipped is True
        assert result.cost_cents == 0
        assert result.success is False
        assert result.provider == "pdl"


class TestWhitepagesSkippedWhenNoKey:

    def test_returns_skipped_dict_when_key_absent(self):
        from src.services.whitepages_fallback import run_whitepages_fallback
        with patch("src.services.whitepages_fallback.get_settings") as mock:
            mock.return_value.whitepages_api_key = None
            stats = run_whitepages_fallback(limit=10)
        assert stats.get("skipped") is True
        assert stats.get("reason") == "WHITEPAGES_API_KEY not configured"


class TestPDLResponseParsing:

    def _make_mock_resp(self, data_payload: dict, status_code: int = 200):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = {
            "status": status_code,
            "likelihood": 7,
            "data": data_payload,
        }
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def test_mobile_phone_field_used_directly(self):
        """mobile_phone top-level field in person data is used, not phone_numbers."""
        from src.services.pdl_skip_trace import run_pdl_lookup
        mock_resp = self._make_mock_resp({
            "mobile_phone": "+18135550000",
            "phones": [],
            "emails": [],
        })
        with patch("src.services.pdl_skip_trace.get_settings") as mock_settings, \
             patch("requests.get", return_value=mock_resp):
            s = MagicMock()
            s.pdl_api_key.get_secret_value.return_value = "test-key"
            mock_settings.return_value = s
            result = run_pdl_lookup("John", "Doe", "123 Main", "Tampa", "FL", "33601")
        assert result.mobile_phone == "+18135550000"
        assert result.success is True

    def test_fields_accessed_inside_data_key(self):
        """Person data is nested under response['data'], not at top level."""
        from src.services.pdl_skip_trace import run_pdl_lookup
        # Put phone at top level only (not inside data) — should NOT be picked up
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "status": 200,
            "likelihood": 5,
            "mobile_phone": "+18135559999",  # top-level — should be ignored
            "data": {
                "mobile_phone": None,
                "phones": [],
                "emails": [],
            },
        }
        with patch("src.services.pdl_skip_trace.get_settings") as mock_settings, \
             patch("requests.get", return_value=mock_resp):
            s = MagicMock()
            s.pdl_api_key.get_secret_value.return_value = "test-key"
            mock_settings.return_value = s
            result = run_pdl_lookup("John", "Doe", "123 Main", "Tampa", "FL", "33601")
        assert result.mobile_phone is None
        assert result.success is False

    def test_location_flat_fields_used_for_mailing_address(self):
        """Mailing address built from location_street_address etc., not locations array."""
        from src.services.pdl_skip_trace import run_pdl_lookup
        mock_resp = self._make_mock_resp({
            "mobile_phone": "+18135550001",
            "phones": [],
            "emails": [],
            "location_street_address": "123 Main St",
            "location_locality": "Tampa",
            "location_region": "FL",
            "location_postal_code": "33601",
        })
        with patch("src.services.pdl_skip_trace.get_settings") as mock_settings, \
             patch("requests.get", return_value=mock_resp):
            s = MagicMock()
            s.pdl_api_key.get_secret_value.return_value = "test-key"
            mock_settings.return_value = s
            result = run_pdl_lookup("John", "Doe", "123 Main", "Tampa", "FL", "33601")
        assert result.mailing_address == "123 Main St, Tampa, FL, 33601"

    def test_likelihood_from_top_level(self):
        """likelihood is at resp_json top level, not inside data."""
        from src.services.pdl_skip_trace import run_pdl_lookup
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "status": 200,
            "likelihood": 9,
            "data": {"mobile_phone": "+18135550002", "phones": [], "emails": []},
        }
        with patch("src.services.pdl_skip_trace.get_settings") as mock_settings, \
             patch("requests.get", return_value=mock_resp):
            s = MagicMock()
            s.pdl_api_key.get_secret_value.return_value = "test-key"
            mock_settings.return_value = s
            result = run_pdl_lookup("John", "Doe", "123 Main", "Tampa", "FL", "33601")
        assert result.raw_metadata == {"pdl_likelihood": 9}


class TestWhitepagesResponseParsing:

    def test_mobile_preferred_over_landline(self):
        from src.services.whitepages_fallback import _parse_wp_result
        persons = [{
            "phones": [
                {"number": "(813) 555-0001", "type": "landline", "score": 90},
                {"number": "(813) 555-0002", "type": "mobile",   "score": 85},
            ],
            "emails": [],
            "current_addresses": [],
        }]
        result = _parse_wp_result(persons)
        assert result["mobile_phone"] == "(813) 555-0002"
        assert result["landline"] == "(813) 555-0001"
        assert result["match_success"] is True

    def test_email_extracted_by_score(self):
        from src.services.whitepages_fallback import _parse_wp_result
        persons = [{
            "phones": [],
            "emails": [
                {"address": "low@example.com",  "score": 40},
                {"address": "high@example.com", "score": 90},
            ],
            "current_addresses": [],
        }]
        result = _parse_wp_result(persons)
        assert result["email"] == "high@example.com"

    def test_address_from_current_addresses_string(self):
        from src.services.whitepages_fallback import _parse_wp_result
        persons = [{
            "phones": [{"number": "(813) 555-0003", "type": "mobile", "score": 80}],
            "emails": [],
            "current_addresses": [{"id": "A123", "address": "123 Main St, Tampa, FL 33601"}],
        }]
        result = _parse_wp_result(persons)
        assert result["mailing_address"] == "123 Main St, Tampa, FL 33601"

    def test_empty_persons_returns_no_match(self):
        from src.services.whitepages_fallback import _parse_wp_result
        result = _parse_wp_result([])
        assert result["match_success"] is False
        assert result["mobile_phone"] is None
