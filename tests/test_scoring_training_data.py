"""
Stage B — CDS scoring training-dataset builder.

The builder issues three SQL statements (score events, signal evidence,
outcomes) and composes per-vertical rows in Python. These tests stub the
session so we can exercise the composition layer without a live DB.
"""

import csv
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.services.scoring_training_data import (
    BuilderConfig,
    DEFAULT_OUTCOME_WINDOW_DAYS,
    SIGNAL_TYPES,
    VERTICALS,
    _equity_bucket,
    _row_columns,
    build_training_dataset,
    write_csv,
)


# ---------------------------------------------------------------------------
# Session stub — returns canned rows per SQL call in execution order.
# ---------------------------------------------------------------------------

class _FakeMappings:
    """Mimic the .mappings() interface used in the builder."""
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _FakeMappings(self._rows)


class _FakeSession:
    """Returns canned rows for each execute() call in declaration order.

    The builder makes exactly three calls — base score events, signal
    evidence, outcomes — and the tests load those three result sets.
    """
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append({"statement": str(statement), "params": params})
        return _FakeResult(self._results.pop(0))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TODAY = date.today()
SCORE_DATE = TODAY - timedelta(days=120)  # fully observed past 90d window


def _score_event(**overrides):
    """Default score-event row matching _BASE_SCORE_EVENTS_SQL output."""
    row = {
        "score_id":          1,
        "property_id":       100,
        "score_date":        SCORE_DATE,
        "final_cds_score":   95.0,
        "vertical_scores":   {v: 50.0 + i for i, v in enumerate(VERTICALS)},
        "lead_tier":         "Ultra Platinum",
        "urgency_level":     "immediate",
        "qualified":         True,
        "county_id":         "hillsborough",
        "year_built":        1985,
        "parcel_id":         "U-100",
        "absentee_status":   "In-County",
        "has_phone":         True,
        "has_email":         False,
        "equity_pct":        45.0,
        "last_sale_date":    TODAY - timedelta(days=365 * 12),  # 12 yrs ago
        "value_change_yoy":  -0.03,
    }
    row.update(overrides)
    return row


def _signal_row(property_id, score_date, sig_type, days_before):
    return {
        "property_id":      property_id,
        "score_date":       score_date,
        "sig_type":         sig_type,
        "latest_sig_date":  score_date - timedelta(days=days_before),
    }


def _outcome_row(property_id, score_date, *, deed_days=None, fc_days=None, deal_days=None,
                 deal_tier=None):
    deed = score_date + timedelta(days=deed_days) if deed_days is not None else None
    fc   = score_date + timedelta(days=fc_days)   if fc_days   is not None else None
    deal = score_date + timedelta(days=deal_days) if deal_days is not None else None
    event = None
    if deed and fc:
        event = min(deed, fc)
    elif deed:
        event = deed
    elif fc:
        event = fc
    return {
        "property_id":     property_id,
        "score_date":      score_date,
        "event_date":      event,
        "deed_event_date": deed,
        "fc_event_date":   fc,
        "deal_event_date": deal,
        "deal_confidence_tier": deal_tier,
    }


def _cfg(**overrides):
    base = dict(
        since=TODAY - timedelta(days=365),
        outcome_window_days=DEFAULT_OUTCOME_WINDOW_DAYS,
        county_id=None,
        output_dir=Path("/tmp/never_written"),
        run_id="test",
    )
    base.update(overrides)
    return BuilderConfig(**base)


# ---------------------------------------------------------------------------
# Composition behaviour
# ---------------------------------------------------------------------------

class TestComposition:

    def test_one_row_per_vertical_per_score_event(self):
        session = _FakeSession([
            [_score_event()],
            [_signal_row(100, SCORE_DATE, "foreclosures", 30)],
            [_outcome_row(100, SCORE_DATE, deed_days=45)],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert len(rows) == len(VERTICALS)
        assert {r["vertical"] for r in rows} == set(VERTICALS)
        # Each row carries the same score_id but a vertical-specific score.
        score_ids = {r["score_id"] for r in rows}
        assert score_ids == {1}
        v_scores = {r["vertical"]: r["vertical_score"] for r in rows}
        assert all(s is not None for s in v_scores.values())

    def test_signal_presence_and_recency_populated(self):
        session = _FakeSession([
            [_score_event()],
            [
                _signal_row(100, SCORE_DATE, "foreclosures", 30),
                _signal_row(100, SCORE_DATE, "judgment_liens", 10),
            ],
            [],   # no outcome
        ])
        rows = list(build_training_dataset(session, _cfg()))
        r = rows[0]
        assert r["has_foreclosures"] == 1
        assert r["recency_foreclosures_days"] == 30
        assert r["has_judgment_liens"] == 1
        assert r["recency_judgment_liens_days"] == 10
        # An unsignalled axis is 0/None, NOT missing (Hillsborough has all signals).
        assert r["has_code_violations"] == 0
        assert r["recency_code_violations_days"] is None

    def test_outcome_label_zero_when_no_outcome_row(self):
        session = _FakeSession([
            [_score_event()],
            [],   # no signals
            [],   # no outcomes
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert all(r["outcome_event"] == 0 for r in rows)
        assert all(r["outcome_event_date"] is None for r in rows)
        assert all(r["outcome_deal"] == 0 for r in rows)

    def test_outcome_label_one_when_event_in_window(self):
        session = _FakeSession([
            [_score_event()],
            [],
            [_outcome_row(100, SCORE_DATE, fc_days=60)],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        # Every per-vertical row carries the same outcome label — that's fine,
        # the fit groups by vertical.
        assert all(r["outcome_event"] == 1 for r in rows)
        expected = (SCORE_DATE + timedelta(days=60)).isoformat()
        assert all(r["outcome_event_date"] == expected for r in rows)

    def test_deal_outcome_column_distinct_from_event(self):
        session = _FakeSession([
            [_score_event()],
            [],
            [_outcome_row(100, SCORE_DATE, deal_days=30)],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        # No deed/foreclosure → outcome_event=0 but outcome_deal=1.
        assert all(r["outcome_event"] == 0 for r in rows)
        assert all(r["outcome_deal"] == 1 for r in rows)

    def test_deal_outcome_confidence_tier_surfaced(self):
        """CDE-11 — a deal outcome's confidence tier reaches the training row."""
        session = _FakeSession([
            [_score_event()],
            [],
            [_outcome_row(100, SCORE_DATE, deal_days=30, deal_tier="public_record_inferred")],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert all(r["outcome_deal_confidence_tier"] == "public_record_inferred" for r in rows)

    def test_deal_confidence_tier_none_when_no_deal(self):
        session = _FakeSession([
            [_score_event()],
            [],
            [_outcome_row(100, SCORE_DATE, fc_days=60)],  # foreclosure, no deal
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert all(r["outcome_deal_confidence_tier"] is None for r in rows)

    def test_stacking_count_only_within_stacking_window(self):
        # foreclosures 30 days back, judgment_liens 200 days back (outside 180d window).
        session = _FakeSession([
            [_score_event()],
            [
                _signal_row(100, SCORE_DATE, "foreclosures", 30),
                _signal_row(100, SCORE_DATE, "judgment_liens", 200),
            ],
            [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        # Only foreclosures counts toward the stacking window.
        assert all(r["stacking_count"] == 1 for r in rows)
        # But both signals still show their recency.
        assert rows[0]["has_judgment_liens"] == 1
        assert rows[0]["recency_judgment_liens_days"] == 200


# ---------------------------------------------------------------------------
# Per-county feature mask
# ---------------------------------------------------------------------------

class TestPerCountyMask:

    def test_pinellas_missing_signals_emit_null_not_zero(self):
        # Pinellas missing_signals (per COUNTY_OVERRIDES) includes
        # code_violations, code_lien, enforcement_permit, tax_delinquencies,
        # bankruptcy, evictions.
        session = _FakeSession([
            [_score_event(county_id="pinellas", property_id=200, parcel_id="P-200")],
            [_signal_row(200, SCORE_DATE, "foreclosures", 5)],  # has-signal axis
            [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        r = rows[0]
        # Available signal: emitted normally.
        assert r["has_foreclosures"] == 1
        assert r["recency_foreclosures_days"] == 5
        # Missing-by-county signals: NULL, not 0 (so the fit treats them as unobserved).
        for sig in ("code_violations", "code_lien", "enforcement_permit",
                    "tax_delinquencies", "bankruptcy", "evictions"):
            assert r[f"has_{sig}"] is None, f"{sig} should be NULL for Pinellas"
            assert r[f"recency_{sig}_days"] is None

    def test_hillsborough_unsignalled_axis_is_zero_not_null(self):
        session = _FakeSession([
            [_score_event(county_id="hillsborough")],
            [],   # no signals at all
            [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        r = rows[0]
        # Hillsborough has full coverage — absence means "not present", emit 0.
        assert r["has_code_violations"] == 0
        assert r["recency_code_violations_days"] is None


# ---------------------------------------------------------------------------
# HCPA passives
# ---------------------------------------------------------------------------

class TestHCPAPassives:

    def test_property_age_30plus_set_when_old_enough(self):
        session = _FakeSession([
            [_score_event(year_built=1980)],  # > 30 years before SCORE_DATE
            [], [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert rows[0]["property_age_30plus"] == 1
        assert rows[0]["property_age_years"] is not None

    def test_property_age_30plus_zero_when_new_build(self):
        session = _FakeSession([
            [_score_event(year_built=TODAY.year - 5)],
            [], [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert rows[0]["property_age_30plus"] == 0

    def test_long_term_owner_set_when_sale_old(self):
        session = _FakeSession([
            [_score_event(last_sale_date=TODAY - timedelta(days=365 * 15))],
            [], [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert rows[0]["long_term_owner"] == 1

    def test_long_term_owner_zero_when_recent_sale(self):
        session = _FakeSession([
            [_score_event(last_sale_date=TODAY - timedelta(days=365 * 2))],
            [], [],
        ])
        rows = list(build_training_dataset(session, _cfg()))
        assert rows[0]["long_term_owner"] == 0


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

class TestSmallHelpers:

    @pytest.mark.parametrize("pct,expected", [
        (None, None),
        (10.0, "low"),
        (29.9, "low"),
        (30.0, "mid"),
        (50.0, "mid"),
        (50.1, "high"),
        (75.0, "high"),
    ])
    def test_equity_bucket_thresholds(self, pct, expected):
        assert _equity_bucket(pct) == expected

    def test_fully_observed_cutoff_respects_window(self):
        cfg = _cfg(outcome_window_days=60)
        assert cfg.fully_observed_cutoff == TODAY - timedelta(days=60)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

class TestCsvWriter:

    def test_write_csv_emits_stable_column_order(self, tmp_path):
        session = _FakeSession([
            [_score_event()],
            [_signal_row(100, SCORE_DATE, "foreclosures", 30)],
            [_outcome_row(100, SCORE_DATE, fc_days=10)],
        ])
        rows = list(build_training_dataset(session, _cfg()))

        out = tmp_path / "run.csv"
        n = write_csv(rows, out)
        assert n == len(VERTICALS)

        with out.open() as fh:
            reader = csv.reader(fh)
            header = next(reader)
            assert header == _row_columns()

            data_rows = list(reader)
            assert len(data_rows) == len(VERTICALS)
            # Every row carries the outcome and a vertical.
            v_idx = header.index("vertical")
            evt_idx = header.index("outcome_event")
            assert {r[v_idx] for r in data_rows} == set(VERTICALS)
            assert all(r[evt_idx] == "1" for r in data_rows)

    def test_write_csv_creates_parent_dir(self, tmp_path):
        session = _FakeSession([[_score_event()], [], []])
        rows = list(build_training_dataset(session, _cfg()))

        out = tmp_path / "deeply" / "nested" / "dir" / "run.csv"
        write_csv(rows, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# Time-leakage guard — verify the SQL constants encode the > score_date guard
# ---------------------------------------------------------------------------

class TestTimeLeakageGuards:

    def test_outcome_sql_uses_strict_greater_than_score_date(self):
        from src.services.scoring_training_data import _OUTCOMES_SQL
        # Outcome event must be STRICTLY after score_date.
        assert "d.record_date >  se.score_date" in _OUTCOMES_SQL
        assert "f.filing_date >  se.score_date" in _OUTCOMES_SQL
        assert "dlo.deal_date >  se.score_date" in _OUTCOMES_SQL

    def test_outcome_sql_surfaces_deal_confidence_tier_by_trust(self):
        from src.services.scoring_training_data import _OUTCOMES_SQL
        # CDE-11 — the deal outcome's confidence_tier is aggregated highest-trust
        # first (founder_verified ranks above subscriber_reported above inferred).
        assert "dlo.confidence_tier" in _OUTCOMES_SQL
        assert "deal_confidence_tier" in _OUTCOMES_SQL
        assert "'founder_verified'    THEN 1" in _OUTCOMES_SQL
        assert "'subscriber_reported' THEN 2" in _OUTCOMES_SQL

    def test_signal_sql_uses_leq_score_date(self):
        from src.services.scoring_training_data import _SIGNAL_EVIDENCE_SQL
        # Signals must be AT-OR-BEFORE score_date — they're features visible
        # to the engine at that point in time.
        assert "sig.sig_date <= se.score_date" in _SIGNAL_EVIDENCE_SQL

    def test_arms_length_filter_includes_all_intra_family_patterns(self):
        from src.services.scoring_training_data import _ARMS_LENGTH_DEED_SQL
        from src.tasks.conversion_report import _INTRA_FAMILY_DEED_PATTERNS
        for pat in _INTRA_FAMILY_DEED_PATTERNS:
            assert pat in _ARMS_LENGTH_DEED_SQL
