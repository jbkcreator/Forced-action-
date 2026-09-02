"""
Unit tests for the /health/detailed severity classification and the
health_check.py alert-dispatch logic — no DB required.

Covers the redesign that separates real scraper errors (actionable —
escalates to "degraded") from data-availability gaps (informational only —
escalates to "warning" at most, never mixed into the urgent alert as if it
were a real problem).
"""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.api.main import _classify_scraper_issues, _escalate
import src.tasks.health_check as hc


# ── _escalate ────────────────────────────────────────────────────────────────

class TestEscalate:
    def test_escalates_upward(self):
        assert _escalate("ok", "warning") == "warning"
        assert _escalate("warning", "degraded") == "degraded"
        assert _escalate("degraded", "critical") == "critical"

    def test_never_downgrades(self):
        assert _escalate("degraded", "warning") == "degraded"
        assert _escalate("critical", "degraded") == "critical"
        assert _escalate("warning", "ok") == "warning"

    def test_order_independent_across_multiple_checks(self):
        # A later "warning" must not undo an earlier "degraded", and a later
        # "degraded" must still win over an earlier "warning".
        overall = "ok"
        overall = _escalate(overall, "degraded")
        overall = _escalate(overall, "warning")
        assert overall == "degraded"

        overall = "ok"
        overall = _escalate(overall, "warning")
        overall = _escalate(overall, "degraded")
        assert overall == "degraded"


# ── _classify_scraper_issues ─────────────────────────────────────────────────

def _row(source_type, error_type, error_message, run_success, d=date(2026, 7, 8),
         outcome_category=None):
    return SimpleNamespace(
        source_type=source_type, error_type=error_type,
        error_message=error_message, run_date=d, run_success=run_success,
        outcome_category=outcome_category,
    )


def _legacy_row(source_type, error_type, error_message, run_success, d=date(2026, 7, 8)):
    """A row shaped like it predates outcome_category entirely (no attribute
    at all, not just None) — exercises the getattr(...) default fallback."""
    return SimpleNamespace(
        source_type=source_type, error_type=error_type,
        error_message=error_message, run_date=d, run_success=run_success,
    )


class TestClassifyScraperIssues:
    def test_confirmed_no_data_is_never_a_real_error(self):
        """error_type='no_data' lands in data_unavailable even when a call
        site defensively marked it run_success=False (e.g. the
        ScraperNoDataError handler in scraper_db_helper.py)."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("evictions", "no_data", None, True),
            _row("probate", "no_data", None, False),
        ])
        assert errors == []
        assert {d["source"] for d in data_unavailable} == {"evictions", "probate"}
        assert all(d["reason"] == "no_data" for d in data_unavailable)

    def test_rate_limited_is_always_informational_even_when_run_success_true(self):
        """error_type='rate_limited' (sunbiz/pa_engine circuit breaker tripped
        after repeated hard timeouts) is a throttled site, not a code bug —
        never a real error regardless of run_success."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("sunbiz", "rate_limited", "aborted with 42 unprocessed", True),
        ])
        assert errors == []
        assert len(data_unavailable) == 1
        assert data_unavailable[0]["reason"] == "rate_limited"

    def test_export_unavailable_is_a_real_error(self):
        """Post the docket-detail reconstruction fallback, export_unavailable
        only fires when the site was genuinely unreachable this run — treated
        as a real, actionable error, not a data gap."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("evictions", "export_unavailable", "export timeout", False),
        ])
        assert data_unavailable == []
        assert len(errors) == 1
        assert errors[0]["error_type"] == "export_unavailable"

    def test_genuine_crash_is_a_real_error(self):
        errors, _ = _classify_scraper_issues([
            _row("bankruptcy", "scraper_error", "Read timed out", False),
        ])
        assert len(errors) == 1
        assert errors[0]["message"] == "Read timed out"

    def test_scraper_error_is_a_real_error_even_when_run_success_true(self):
        """storm/flood zone-fetch helpers mark a partial-zone outage as
        error_type='scraper_error' with run_success=True (another zone/NFIP
        still produced usable data) — this must never be downgraded to the
        informational zero_rows_unclassified bucket."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("flood_damage", "scraper_error", "FLZ151: connection reset", True),
        ])
        assert data_unavailable == []
        assert len(errors) == 1
        assert errors[0]["error_type"] == "scraper_error"
        assert errors[0]["message"] == "FLZ151: connection reset"

    def test_null_error_type_failure_defaults_to_scraper_error_label(self):
        errors, _ = _classify_scraper_issues([
            _row("sunbiz", None, "unhandled exception", False),
        ])
        assert len(errors) == 1
        assert errors[0]["error_type"] == "scraper_error"

    def test_outcome_category_no_data_wins_even_with_mismatched_error_type(self):
        """outcome_category is the enforced, authoritative signal for migrated
        sources — checked before error_type, not just alongside it."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("flood_damage", "scraper_error", "stale legacy label", False,
                 outcome_category="NO_DATA"),
        ])
        assert errors == []
        assert len(data_unavailable) == 1
        assert data_unavailable[0]["reason"] == "no_data"

    def test_outcome_category_timeout_is_a_real_error(self):
        """A migrated source's TIMEOUT/SOURCE_ERROR/INTERNAL_ERROR/UNKNOWN
        outcome forces run_success=False by construction, so it already lands
        in real_errors via the existing `not r.run_success` branch — no
        special-casing needed beyond the NO_DATA carve-out."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("flood_damage", "scraper_error", "FEMA read timed out", False,
                 outcome_category="TIMEOUT"),
        ])
        assert data_unavailable == []
        assert len(errors) == 1
        assert errors[0]["message"] == "FEMA read timed out"

    def test_unmigrated_source_falls_back_to_legacy_error_type_when_no_attribute(self):
        """A row from before outcome_category existed (no attribute at all,
        not just None) must not crash — falls back to the legacy error_type
        string check exactly as before."""
        errors, data_unavailable = _classify_scraper_issues([
            _legacy_row("evictions", "no_data", None, True),
        ])
        assert errors == []
        assert len(data_unavailable) == 1
        assert data_unavailable[0]["reason"] == "no_data"

    def test_unclassified_zero_rows_is_data_unavailable_not_an_error(self):
        """run_success=True + 0 rows + no explicit no_data marker (e.g. a
        scraper that doesn't yet distinguish 'nothing to scan' from 'scanned
        fine') is informational, not a confirmed crash."""
        errors, data_unavailable = _classify_scraper_issues([
            _row("dbpr_company", "none", None, True),
        ])
        assert errors == []
        assert len(data_unavailable) == 1
        assert data_unavailable[0]["reason"] == "zero_rows_unclassified"

    def test_same_source_can_appear_in_both_buckets_on_different_dates(self):
        errors, data_unavailable = _classify_scraper_issues([
            _row("evictions", "scraper_error", "boom", False, d=date(2026, 7, 7)),
            _row("evictions", "no_data", None, True, d=date(2026, 7, 8)),
        ])
        assert len(errors) == 1 and len(data_unavailable) == 1
        assert errors[0]["source"] == data_unavailable[0]["source"] == "evictions"


# ── health_check.py alert dispatch ───────────────────────────────────────────

def _run_with(status: str, checks: dict):
    """Call run_health_check with a mocked /health/detailed response and
    return the (subject, body) args of the send_alert call, or None if no
    alert was sent."""
    fake_resp = MagicMock()
    fake_resp.json.return_value = {
        "status": status, "checks": checks, "checked_at": "2026-07-08T09:00:00Z",
    }
    with patch.object(hc.requests, "get", return_value=fake_resp), \
         patch("src.services.email.send_alert") as mock_alert:
        hc.run_health_check()
        return mock_alert.call_args


class TestHealthCheckAlertDispatch:
    def test_ok_sends_no_email(self):
        assert _run_with("ok", {"scrapers": {"status": "ok"}}) is None

    def test_warning_sends_calm_notice_not_urgent_alert(self):
        call = _run_with("warning", {"scrapers": {
            "status": "data_unavailable",
            "data_unavailable": [{"source": "evictions", "date": "2026-07-08", "reason": "no_data"}],
        }})
        subject, body = call.args
        assert subject == "[FA] Data availability notice"
        assert "no action is expected" in body.lower()
        assert "evictions" in body

    def test_degraded_sends_urgent_alert_with_error_detail(self):
        call = _run_with("degraded", {"scrapers": {
            "status": "errors",
            "errors": [{"source": "bankruptcy", "date": "2026-07-08",
                        "error_type": "scraper_error", "message": "Read timed out"}],
        }})
        subject, body = call.args
        assert subject == "[FA] System health DEGRADED"
        assert "bankruptcy" in body and "Read timed out" in body

    def test_mixed_degraded_and_data_unavailable_demotes_gap_to_footer(self):
        """A real error elsewhere must not get its urgency diluted by an
        unrelated data-availability gap, and the gap must not read as a
        second alarming issue."""
        call = _run_with("degraded", {"scrapers": {
            "status": "errors",
            "errors": [{"source": "bankruptcy", "date": "2026-07-08",
                        "error_type": "scraper_error", "message": "Read timed out"}],
            "data_unavailable": [{"source": "evictions", "date": "2026-07-08", "reason": "no_data"}],
        }})
        subject, body = call.args
        assert subject == "[FA] System health DEGRADED"
        assert "Also: 1 scraper(s) had no data available today (informational" in body

    def test_critical_sends_urgent_alert(self):
        call = _run_with("critical", {"database": {"status": "error"}})
        subject, _ = call.args
        assert subject == "[FA] System health CRITICAL"
