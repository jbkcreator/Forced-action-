"""
Stage 12 — Unit tests: contractor_benchmark.py + contractor_benchmark_report.py

Covers:
  - Benchmark calculation: group averages, above/at/below classification
  - AP upsell and AP Pro flag logic
  - Edge cases: no leads, single contractor (no peers), zero group avg
  - CSV row formatting
  - Idempotency: compute_benchmark_report is pure (no side effects)
  - End-to-end simulation with injected mock data
"""

from __future__ import annotations

import csv
import io
import math
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest

from src.services.contractor_benchmark import (
    ABOVE_BENCHMARK_THRESHOLD,
    BELOW_BENCHMARK_THRESHOLD,
    MIN_GROUP_SIZE,
    BenchmarkReport,
    ContractorMetrics,
    GroupBenchmark,
    _apply_benchmark_flags,
    _compute_group_benchmarks,
    compute_benchmark_report,
)
from src.tasks.contractor_benchmark_report import (
    CSV_FIELDNAMES,
    _contractor_to_csv_row,
    write_csv,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _row(
    subscriber_id: int = 1,
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    tier: str = "starter",
    total_leads: int = 100,
    closed_deals: int = 20,
    avg_days_to_close: float = 14.0,
    avg_deal_size: float = 8000.0,
    total_revenue: float = 160000.0,
    sms_reply_rate: float = 0.08,
    close_rate: float | None = None,
    plan_price: float = 99.0,
    revenue_signal_score: int = 60,
    name: str = "Test Contractor",
    email: str = "test@example.com",
    total_messages_sent: int = 50,
) -> dict:
    if close_rate is None:
        close_rate = closed_deals / total_leads if total_leads > 0 else 0.0
    return {
        "subscriber_id": subscriber_id,
        "name": name,
        "email": email,
        "vertical": vertical,
        "county_id": county_id,
        "tier": tier,
        "plan_price": plan_price,
        "revenue_signal_score": revenue_signal_score,
        "total_leads": total_leads,
        "closed_deals": closed_deals,
        "close_rate": close_rate,
        "avg_days_to_close": avg_days_to_close,
        "avg_deal_size": avg_deal_size,
        "total_revenue": total_revenue,
        "sms_reply_rate": sms_reply_rate,
        "total_messages_sent": total_messages_sent,
        "replies": int(sms_reply_rate * total_messages_sent),
    }


def _make_contractor(**kwargs) -> ContractorMetrics:
    r = _row(**kwargs)
    return ContractorMetrics(
        subscriber_id=r["subscriber_id"],
        name=r["name"],
        email=r["email"],
        vertical=r["vertical"],
        county_id=r["county_id"],
        tier=r["tier"],
        plan_price=r["plan_price"],
        revenue_signal_score=r["revenue_signal_score"],
        total_leads=r["total_leads"],
        closed_deals=r["closed_deals"],
        close_rate=r["close_rate"],
        avg_days_to_close=r["avg_days_to_close"],
        avg_deal_size=r["avg_deal_size"],
        total_revenue=r["total_revenue"],
        sms_reply_rate=r["sms_reply_rate"],
        total_messages_sent=r["total_messages_sent"],
    )


# ── _compute_group_benchmarks ─────────────────────────────────────────────────

class TestComputeGroupBenchmarks:
    def test_single_group_two_contractors(self):
        rows = [
            _row(subscriber_id=1, close_rate=0.20),
            _row(subscriber_id=2, close_rate=0.10),
        ]
        groups = _compute_group_benchmarks(rows)
        assert len(groups) == 1
        key = ("roofing", "hillsborough")
        g = groups[key]
        assert g.contractor_count == 2
        assert g.sufficient_peers is True
        assert g.avg_close_rate == pytest.approx(0.15)

    def test_group_with_one_contractor_is_insufficient(self):
        rows = [_row(subscriber_id=1, close_rate=0.20)]
        groups = _compute_group_benchmarks(rows)
        key = ("roofing", "hillsborough")
        assert groups[key].sufficient_peers is False

    def test_separate_groups_for_different_verticals(self):
        rows = [
            _row(subscriber_id=1, vertical="roofing",     close_rate=0.20),
            _row(subscriber_id=2, vertical="roofing",     close_rate=0.10),
            _row(subscriber_id=3, vertical="restoration", close_rate=0.15),
            _row(subscriber_id=4, vertical="restoration", close_rate=0.25),
        ]
        groups = _compute_group_benchmarks(rows)
        assert len(groups) == 2
        assert groups[("roofing", "hillsborough")].avg_close_rate == pytest.approx(0.15)
        assert groups[("restoration", "hillsborough")].avg_close_rate == pytest.approx(0.20)

    def test_separate_groups_for_different_counties(self):
        rows = [
            _row(subscriber_id=1, county_id="hillsborough", close_rate=0.20),
            _row(subscriber_id=2, county_id="hillsborough", close_rate=0.10),
            _row(subscriber_id=3, county_id="pinellas",     close_rate=0.30),
            _row(subscriber_id=4, county_id="pinellas",     close_rate=0.20),
        ]
        groups = _compute_group_benchmarks(rows)
        assert len(groups) == 2
        assert groups[("roofing", "hillsborough")].avg_close_rate == pytest.approx(0.15)
        assert groups[("roofing", "pinellas")].avg_close_rate == pytest.approx(0.25)

    def test_totals_aggregated_correctly(self):
        rows = [
            _row(subscriber_id=1, total_leads=100, closed_deals=20, total_revenue=50000),
            _row(subscriber_id=2, total_leads=80,  closed_deals=10, total_revenue=30000),
        ]
        g = _compute_group_benchmarks(rows)[("roofing", "hillsborough")]
        assert g.total_leads == 180
        assert g.total_closed_deals == 30
        assert g.total_revenue == pytest.approx(80000)

    def test_min_group_size_constant_is_respected(self):
        assert MIN_GROUP_SIZE == 2
        rows = [_row()] * (MIN_GROUP_SIZE - 1)
        # Assign unique subscriber_ids
        for i, r in enumerate(rows):
            r["subscriber_id"] = i
        g = _compute_group_benchmarks(rows)[("roofing", "hillsborough")]
        assert g.sufficient_peers is False


# ── _apply_benchmark_flags ────────────────────────────────────────────────────

class TestApplyBenchmarkFlags:
    def _group(self, avg_close_rate: float, n: int = 3) -> GroupBenchmark:
        return GroupBenchmark(
            vertical="roofing",
            county_id="hillsborough",
            contractor_count=n,
            avg_close_rate=avg_close_rate,
            sufficient_peers=n >= MIN_GROUP_SIZE,
        )

    def test_above_benchmark_when_20_percent_over(self):
        c = _make_contractor(close_rate=0.24)
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.benchmark_status == "above"
        assert c.close_rate_vs_benchmark == pytest.approx(1.2)

    def test_at_benchmark_when_within_band(self):
        c = _make_contractor(close_rate=0.20)
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.benchmark_status == "at"
        assert c.close_rate_vs_benchmark == pytest.approx(1.0)

    def test_below_benchmark_when_20_percent_under(self):
        c = _make_contractor(close_rate=0.15)
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.benchmark_status == "below"
        assert c.close_rate_vs_benchmark == pytest.approx(0.75)

    def test_no_peers_when_insufficient_group(self):
        c = _make_contractor(close_rate=0.20)
        g = self._group(avg_close_rate=0.20, n=1)
        _apply_benchmark_flags(c, g)
        assert c.benchmark_status == "no_peers"
        assert c.benchmark_close_rate == 0.0

    def test_ap_upsell_candidate_set_for_eligible_below(self):
        c = _make_contractor(close_rate=0.10, tier="starter")
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.ap_upsell_candidate is True
        assert c.ap_pro_candidate is False

    def test_ap_upsell_not_set_for_autopilot_tier(self):
        c = _make_contractor(close_rate=0.10, tier="autopilot_lite")
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.ap_upsell_candidate is False

    def test_ap_pro_candidate_set_for_above_on_starter(self):
        c = _make_contractor(close_rate=0.30, tier="starter")
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.benchmark_status == "above"
        assert c.ap_pro_candidate is True

    def test_ap_pro_not_set_when_already_on_autopilot_pro(self):
        c = _make_contractor(close_rate=0.30, tier="autopilot_pro")
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        assert c.ap_pro_candidate is False

    def test_zero_group_avg_treated_as_at_benchmark(self):
        c = _make_contractor(close_rate=0.0)
        g = self._group(avg_close_rate=0.0)
        _apply_benchmark_flags(c, g)
        assert c.benchmark_status == "at"

    def test_zero_leads_contractor_counted_correctly(self):
        c = _make_contractor(total_leads=0, closed_deals=0, close_rate=0.0, tier="starter")
        g = self._group(avg_close_rate=0.20)
        _apply_benchmark_flags(c, g)
        # 0 / 0.20 = 0.0 < 0.80 threshold → below
        assert c.benchmark_status == "below"
        assert c.ap_upsell_candidate is True


# ── compute_benchmark_report (integration with mocked DB) ────────────────────

class TestComputeBenchmarkReport:
    def _mock_db(self, rows: list[dict]):
        db = MagicMock()
        mapping_results = MagicMock()
        mapping_results.mappings.return_value.fetchall.return_value = rows
        db.execute.return_value = mapping_results
        return db

    def test_returns_empty_report_when_no_rows(self):
        db = self._mock_db([])
        report = compute_benchmark_report(db, window_days=90)
        assert len(report.contractors) == 0
        assert len(report.benchmarks) == 0

    def test_full_pipeline_three_contractors(self):
        rows = [
            _row(subscriber_id=1, close_rate=0.30, tier="starter"),       # above
            _row(subscriber_id=2, close_rate=0.20, tier="pro"),            # at
            _row(subscriber_id=3, close_rate=0.10, tier="dominator"),      # below
        ]
        db = self._mock_db(rows)
        report = compute_benchmark_report(db, window_days=90)

        assert len(report.contractors) == 3
        assert len(report.benchmarks) == 1

        statuses = {c.subscriber_id: c.benchmark_status for c in report.contractors}
        assert statuses[1] == "above"
        assert statuses[2] == "at"
        assert statuses[3] == "below"

    def test_below_benchmark_property(self):
        rows = [
            _row(subscriber_id=1, close_rate=0.30, tier="starter"),
            _row(subscriber_id=2, close_rate=0.10, tier="starter"),
        ]
        db = self._mock_db(rows)
        report = compute_benchmark_report(db, window_days=90)
        assert len(report.below_benchmark) == 1
        assert report.below_benchmark[0].subscriber_id == 2

    def test_ap_upsell_candidates_property(self):
        rows = [
            _row(subscriber_id=1, close_rate=0.30),
            _row(subscriber_id=2, close_rate=0.10, tier="starter"),
            _row(subscriber_id=3, close_rate=0.12, tier="autopilot_lite"),  # below but wrong tier
        ]
        db = self._mock_db(rows)
        report = compute_benchmark_report(db, window_days=90)
        upsell = {c.subscriber_id for c in report.ap_upsell_candidates}
        assert 2 in upsell
        assert 3 not in upsell  # autopilot_lite excluded

    def test_report_is_pure_no_db_writes(self):
        rows = [_row(subscriber_id=1), _row(subscriber_id=2)]
        db = self._mock_db(rows)
        compute_benchmark_report(db, window_days=90)
        db.add.assert_not_called()
        db.flush.assert_not_called()
        db.commit.assert_not_called()

    def test_generated_at_is_recent(self):
        from datetime import datetime, timezone
        db = self._mock_db([])
        report = compute_benchmark_report(db)
        diff = (datetime.now(timezone.utc) - report.generated_at).total_seconds()
        assert diff < 5

    def test_window_days_passed_to_query(self):
        db = MagicMock()
        db.execute.return_value.mappings.return_value.fetchall.return_value = []
        compute_benchmark_report(db, window_days=30)
        call_params = db.execute.call_args[0][1]
        # 'since' should be 30 days ago (approximately)
        from datetime import datetime, timedelta, timezone
        expected_since = datetime.now(timezone.utc) - timedelta(days=30)
        diff = abs((call_params["since"] - expected_since).total_seconds())
        assert diff < 5


# ── CSV formatting ────────────────────────────────────────────────────────────

class TestCSVFormatting:
    def test_all_fieldnames_present(self):
        c = _make_contractor()
        c.benchmark_status = "below"
        c.benchmark_close_rate = 0.20
        c.close_rate_vs_benchmark = 0.75
        c.ap_upsell_candidate = True
        c.ap_pro_candidate = False
        row = _contractor_to_csv_row(c)
        for field in CSV_FIELDNAMES:
            assert field in row, f"Missing field: {field}"

    def test_close_rate_formatted_as_percentage_string(self):
        c = _make_contractor(close_rate=0.1234)
        c.benchmark_status = "at"
        c.benchmark_close_rate = 0.20
        c.close_rate_vs_benchmark = 0.617
        c.ap_upsell_candidate = False
        c.ap_pro_candidate = False
        row = _contractor_to_csv_row(c)
        assert row["close_rate_pct"] == "12.3"

    def test_ap_upsell_candidate_is_yes_string(self):
        c = _make_contractor(tier="starter")
        c.benchmark_status = "below"
        c.benchmark_close_rate = 0.20
        c.close_rate_vs_benchmark = 0.5
        c.ap_upsell_candidate = True
        c.ap_pro_candidate = False
        row = _contractor_to_csv_row(c)
        assert row["ap_upsell_candidate"] == "yes"
        assert row["ap_pro_candidate"] == "no"

    def test_write_csv_creates_valid_file(self, tmp_path):
        rows = [
            _row(subscriber_id=1, close_rate=0.30, tier="starter"),
            _row(subscriber_id=2, close_rate=0.10, tier="dominator"),
        ]
        db = MagicMock()
        db.execute.return_value.mappings.return_value.fetchall.return_value = rows
        report = compute_benchmark_report(db)

        csv_path = tmp_path / "test_benchmark.csv"
        write_csv(report, csv_path)

        assert csv_path.exists()
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            written_rows = list(reader)

        assert len(written_rows) == 2
        assert written_rows[0]["vertical"] == "roofing"

    def test_write_csv_headers_match_fieldnames(self, tmp_path):
        db = MagicMock()
        db.execute.return_value.mappings.return_value.fetchall.return_value = []
        report = compute_benchmark_report(db)
        csv_path = tmp_path / "empty.csv"
        write_csv(report, csv_path)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == CSV_FIELDNAMES


# ── End-to-end simulation ─────────────────────────────────────────────────────

class TestEndToEndSimulation:
    """Inject a full synthetic dataset and verify the complete pipeline."""

    SYNTHETIC_ROWS = [
        # Roofing / hillsborough — 4 contractors
        _row(subscriber_id=101, vertical="roofing", county_id="hillsborough",
             tier="starter",       close_rate=0.05, total_leads=100, closed_deals=5),   # below
        _row(subscriber_id=102, vertical="roofing", county_id="hillsborough",
             tier="pro",           close_rate=0.20, total_leads=80,  closed_deals=16),  # at
        _row(subscriber_id=103, vertical="roofing", county_id="hillsborough",
             tier="dominator",     close_rate=0.25, total_leads=120, closed_deals=30),  # at/above
        _row(subscriber_id=104, vertical="roofing", county_id="hillsborough",
             tier="autopilot_lite",close_rate=0.35, total_leads=90,  closed_deals=31),  # above

        # Restoration / hillsborough — 2 contractors
        _row(subscriber_id=201, vertical="restoration", county_id="hillsborough",
             tier="starter",       close_rate=0.10, total_leads=60, closed_deals=6),    # below
        _row(subscriber_id=202, vertical="restoration", county_id="hillsborough",
             tier="starter",       close_rate=0.30, total_leads=70, closed_deals=21),   # above

        # Fix & Flip / pinellas — 1 contractor (no peers)
        _row(subscriber_id=301, vertical="fix_flip", county_id="pinellas",
             tier="pro",           close_rate=0.40, total_leads=50, closed_deals=20),   # no_peers
    ]

    def _run(self):
        db = MagicMock()
        db.execute.return_value.mappings.return_value.fetchall.return_value = self.SYNTHETIC_ROWS
        return compute_benchmark_report(db, window_days=90)

    def test_correct_contractor_count(self):
        report = self._run()
        assert len(report.contractors) == 7

    def test_correct_group_count(self):
        report = self._run()
        assert len(report.benchmarks) == 3

    def test_roofing_group_has_sufficient_peers(self):
        report = self._run()
        rh_group = next(b for b in report.benchmarks
                        if b.vertical == "roofing" and b.county_id == "hillsborough")
        assert rh_group.sufficient_peers is True
        assert rh_group.contractor_count == 4

    def test_fix_flip_group_has_no_peers(self):
        report = self._run()
        ff_group = next(b for b in report.benchmarks
                        if b.vertical == "fix_flip")
        assert ff_group.sufficient_peers is False

    def test_no_peers_contractor_has_correct_status(self):
        report = self._run()
        c301 = next(c for c in report.contractors if c.subscriber_id == 301)
        assert c301.benchmark_status == "no_peers"
        assert c301.ap_upsell_candidate is False

    def test_subscriber_101_is_below_and_upsell_candidate(self):
        report = self._run()
        c = next(c for c in report.contractors if c.subscriber_id == 101)
        assert c.benchmark_status == "below"
        assert c.ap_upsell_candidate is True  # tier=starter qualifies

    def test_subscriber_104_above_and_ap_pro_candidate(self):
        report = self._run()
        c = next(c for c in report.contractors if c.subscriber_id == 104)
        assert c.benchmark_status == "above"
        assert c.ap_pro_candidate is True   # autopilot_lite → eligible for pro upgrade

    def test_benchmark_close_rate_is_group_average(self):
        report = self._run()
        roofing_contractors = [c for c in report.contractors
                               if c.vertical == "roofing" and c.county_id == "hillsborough"]
        expected_avg = sum(c.close_rate for c in roofing_contractors) / len(roofing_contractors)
        for c in roofing_contractors:
            if c.benchmark_status != "no_peers":
                assert c.benchmark_close_rate == pytest.approx(expected_avg, rel=1e-6)

    def test_total_upsell_candidates(self):
        report = self._run()
        # Sub 101 (roofing, starter, below) + sub 201 (restoration, starter, below)
        upsell_ids = {c.subscriber_id for c in report.ap_upsell_candidates}
        assert 101 in upsell_ids
        assert 201 in upsell_ids

    def test_csv_output_has_correct_row_count(self, tmp_path):
        report = self._run()
        path = tmp_path / "sim.csv"
        write_csv(report, path)
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 7

    def test_csv_upsell_column_correct_for_each_contractor(self, tmp_path):
        report = self._run()
        path = tmp_path / "sim_upsell.csv"
        write_csv(report, path)
        with open(path, newline="", encoding="utf-8") as f:
            rows = {int(r["subscriber_id"]): r for r in csv.DictReader(f)}
        assert rows[101]["ap_upsell_candidate"] == "yes"
        assert rows[104]["ap_pro_candidate"] == "yes"
        assert rows[301]["ap_upsell_candidate"] == "no"  # no peers → no flag
