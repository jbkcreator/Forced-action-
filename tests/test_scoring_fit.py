"""
Stage C — per-vertical CDS scoring fit.

Pure-function tests (logit/sigmoid math, coefficient → integer-knob mapping,
feature-matrix prep) run unconditionally. End-to-end fit tests that need
scikit-learn use pytest.importorskip so the suite stays green on machines
where sklearn isn't installed yet.
"""

import json
import math
from pathlib import Path

import pandas as pd
import pytest

from src.services.scoring_fit import (
    DEFAULT_L2_REG,
    MIN_EVENTS_PER_VERTICAL,
    FitConfig,
    KnobProposal,
    _coef_to_weight,
    _logit,
    _sigmoid,
    categorical_dummy_names,
    coefs_to_knobs,
    derive_lead_tier_thresholds,
    feature_names,
    numeric_features,
    prepare_feature_matrix,
)
from src.services.scoring_training_data import SIGNAL_TYPES, VERTICALS


# ---------------------------------------------------------------------------
# Pure-function math
# ---------------------------------------------------------------------------

class TestLogitSigmoid:

    @pytest.mark.parametrize("p", [0.01, 0.1, 0.3, 0.5, 0.7, 0.9, 0.99])
    def test_sigmoid_inverts_logit(self, p):
        assert _sigmoid(_logit(p)) == pytest.approx(p, rel=1e-6)

    def test_logit_clipping_at_bounds(self):
        # 0 and 1 would blow up — function must clip rather than raise.
        _logit(0.0)
        _logit(1.0)


class TestCoefToWeight:

    def test_negative_coef_clips_to_zero(self):
        assert _coef_to_weight(-3.5, max_coef=4.0, score_budget=75) == 0

    def test_zero_coef_is_zero(self):
        assert _coef_to_weight(0.0, max_coef=4.0, score_budget=75) == 0

    def test_max_coef_takes_full_budget(self):
        assert _coef_to_weight(4.0, max_coef=4.0, score_budget=75) == 75

    def test_proportional_scaling(self):
        # Half the max → half the budget.
        assert _coef_to_weight(2.0, max_coef=4.0, score_budget=80) == 40

    def test_zero_max_coef_returns_zero(self):
        # All coefficients are zero → no weight given out.
        assert _coef_to_weight(0.0, max_coef=0.0, score_budget=75) == 0


class TestCoefsToKnobs:

    def test_emits_weight_per_signal_type(self):
        coefs = {f"has_{sig}": 1.0 for sig in SIGNAL_TYPES}
        knobs = coefs_to_knobs("wholesalers", coefs, intercept=0.0)
        assert set(knobs.keys()) == set(SIGNAL_TYPES)

    def test_strongest_signal_anchors_score_budget(self):
        coefs = {f"has_{sig}": 0.0 for sig in SIGNAL_TYPES}
        coefs["has_foreclosures"] = 2.0
        coefs["has_judgment_liens"] = 1.0
        knobs = coefs_to_knobs("wholesalers", coefs, intercept=0.0, score_budget=80)
        assert knobs["has_foreclosures" if "has_foreclosures" in knobs else "foreclosures"] in (80,)
        # Signal-key normalisation: knobs are keyed by the bare signal name.
        assert knobs["foreclosures"] == 80
        assert knobs["judgment_liens"] == 40   # half the strongest
        # Untouched signals should be zero.
        assert knobs["evictions"] == 0
        assert knobs["bankruptcy"] == 0

    def test_all_zero_coefs_returns_zero_knobs(self):
        coefs = {f"has_{sig}": 0.0 for sig in SIGNAL_TYPES}
        knobs = coefs_to_knobs("attorneys", coefs, intercept=0.0)
        assert all(v == 0 for v in knobs.values())

    def test_non_signal_coefs_are_ignored(self):
        # Stacking / equity / contact coefficients shouldn't bleed into the
        # VERTICAL_WEIGHTS map — they map to universal multipliers separately.
        coefs = {
            "has_foreclosures": 2.0,
            "stacking_count":   5.0,
            "equity_pct":       3.0,
            "has_phone":        1.0,
        }
        knobs = coefs_to_knobs("wholesalers", coefs, intercept=0.0)
        assert knobs["foreclosures"] > 0
        # Make sure we didn't accidentally produce a "stacking_count" key.
        assert "stacking_count" not in knobs
        assert "equity_pct" not in knobs


class TestDeriveTierThresholds:

    def test_returns_existing_thresholds_in_v1(self):
        from config.scoring import LEAD_TIER_THRESHOLDS
        derived = derive_lead_tier_thresholds(intercept=0.0)
        assert derived == list(LEAD_TIER_THRESHOLDS)


# ---------------------------------------------------------------------------
# Feature shape
# ---------------------------------------------------------------------------

class TestFeatureNames:

    def test_numeric_features_include_per_signal_has_indicators(self):
        cols = numeric_features()
        for sig in SIGNAL_TYPES:
            assert f"has_{sig}" in cols

    def test_numeric_features_exclude_recency(self):
        # Recency isn't a per-signal knob in the engine — see numeric_features() docstring.
        cols = numeric_features()
        assert not any(c.startswith("recency_") for c in cols)

    def test_categoricals_one_hot(self):
        dummies = categorical_dummy_names()
        # Three absentee statuses + three equity buckets = 6 dummies.
        assert "absentee_status__Out-of-State" in dummies
        assert "absentee_status__In-County" in dummies
        assert "equity_bucket__high" in dummies
        assert "equity_bucket__low" in dummies

    def test_feature_names_is_stable_order(self):
        # Call twice — must be deterministic.
        assert feature_names() == feature_names()


# ---------------------------------------------------------------------------
# Feature-matrix prep
# ---------------------------------------------------------------------------

def _toy_training_frame(n_per_vertical=20, county_id="hillsborough"):
    """Synthetic CSV-like frame with one row per (vertical, idx)."""
    rows = []
    for v in VERTICALS:
        for i in range(n_per_vertical):
            row = {
                "property_id":         i,
                "parcel_id":           f"U-{i}",
                "county_id":           county_id,
                "score_id":            i,
                "score_date":          "2025-01-01",
                "vertical":            v,
                "vertical_score":      60.0,
                "final_cds_score":     70.0,
                "lead_tier":           "Gold",
                "urgency_level":       "daily",
                "qualified":           True,
                "absentee_status":     "In-County" if i % 2 == 0 else "Out-of-State",
                "has_phone":           1,
                "has_email":           i % 3 == 0,
                "equity_pct":          45.0,
                "equity_bucket":       "mid",
                "years_since_sale":    5.0,
                "long_term_owner":     0,
                "value_change_yoy":    -0.02,
                "property_age_years":  40,
                "property_age_30plus": 1,
                "stacking_count":      1,
                "outcome_event":       1 if i % 5 == 0 else 0,
                "outcome_event_date":  None,
                "outcome_deal":        0,
            }
            for sig in SIGNAL_TYPES:
                # foreclosures is the only "real" signal for this synthetic
                # dataset — it perfectly predicts the outcome (every 5th row).
                row[f"has_{sig}"] = (1 if sig == "foreclosures" and i % 5 == 0 else 0)
                row[f"recency_{sig}_days"] = None if row[f"has_{sig}"] == 0 else 7
            rows.append(row)
    return pd.DataFrame(rows)


class TestPrepareFeatureMatrix:

    def test_filters_to_one_vertical(self):
        df = _toy_training_frame(n_per_vertical=10)
        X, y, names = prepare_feature_matrix(df, "wholesalers")
        # Only one vertical's rows survive.
        assert X.shape[0] == 10
        assert len(y) == 10

    def test_raises_when_vertical_absent(self):
        df = _toy_training_frame()
        with pytest.raises(ValueError, match="no training rows"):
            prepare_feature_matrix(df, "not_a_vertical")

    def test_raises_when_outcome_missing(self):
        df = _toy_training_frame()
        df = df.drop(columns=["outcome_event"])
        with pytest.raises(ValueError, match="outcome_event"):
            prepare_feature_matrix(df, "wholesalers")

    def test_drops_rows_with_nan_features(self):
        """Pinellas-style rows (NaN per-signal mask) drop out of the fit set."""
        import numpy as np
        df = _toy_training_frame(n_per_vertical=10, county_id="pinellas")
        # Simulate per-county NaN on a missing signal axis (code_violations).
        df.loc[df["vertical"] == "wholesalers", "has_code_violations"] = np.nan
        df.loc[df["vertical"] == "wholesalers", "recency_code_violations_days"] = np.nan
        with pytest.raises(ValueError, match="no rows survive NaN drop"):
            prepare_feature_matrix(df, "wholesalers")

    def test_one_hot_expansion_produces_dummy_columns(self):
        df = _toy_training_frame(n_per_vertical=10)
        X, y, names = prepare_feature_matrix(df, "wholesalers")
        # Last 6 columns are the one-hot dummies.
        assert "absentee_status__Out-of-State" in names
        assert "equity_bucket__mid" in names


# ---------------------------------------------------------------------------
# End-to-end fit (requires scikit-learn)
# ---------------------------------------------------------------------------

class TestFitVerticalE2E:
    """Integration tests gated on scikit-learn being installed."""

    @pytest.fixture(autouse=True)
    def _require_sklearn(self):
        pytest.importorskip("sklearn")

    def test_returns_knob_proposal(self):
        from src.services.scoring_fit import fit_vertical
        df = _toy_training_frame(n_per_vertical=50)
        prop = fit_vertical(df, "wholesalers")
        assert isinstance(prop, KnobProposal)
        assert prop.vertical == "wholesalers"
        assert set(prop.vertical_weights.keys()) == set(SIGNAL_TYPES)

    def test_signal_with_perfect_predictive_power_gets_max_weight(self):
        from src.services.scoring_fit import fit_vertical
        df = _toy_training_frame(n_per_vertical=100)
        prop = fit_vertical(df, "wholesalers")
        # In the synthetic data, only foreclosures predicts the outcome.
        # Its weight should dominate the table.
        weights = prop.vertical_weights
        assert weights["foreclosures"] == max(weights.values())
        assert weights["foreclosures"] > 0

    def test_auc_reported_and_above_random_for_predictive_dataset(self):
        from src.services.scoring_fit import fit_vertical
        df = _toy_training_frame(n_per_vertical=200)
        prop = fit_vertical(df, "wholesalers")
        # With a perfectly-predictive single signal, AUC should be near 1.0.
        assert prop.auc is not None
        assert prop.auc > 0.9

    def test_constant_outcome_returns_zero_knobs_with_warning(self):
        from src.services.scoring_fit import fit_vertical
        df = _toy_training_frame(n_per_vertical=50)
        df.loc[df["vertical"] == "wholesalers", "outcome_event"] = 0
        prop = fit_vertical(df, "wholesalers")
        assert all(v == 0 for v in prop.vertical_weights.values())
        assert prop.coverage_warning is not None

    def test_sparse_events_emits_coverage_warning(self):
        from src.services.scoring_fit import fit_vertical
        # Trim so positives are below MIN_EVENTS_PER_VERTICAL.
        df = _toy_training_frame(n_per_vertical=20)  # ~ 4 positives per vertical
        prop = fit_vertical(df, "wholesalers")
        # 4 events << MIN_EVENTS_PER_VERTICAL (30) — warning expected.
        assert prop.coverage_warning is not None
        assert "weak" in prop.coverage_warning.lower() or "events" in prop.coverage_warning.lower()


# ---------------------------------------------------------------------------
# Orchestrator (requires scikit-learn for the fit; pure CSV I/O otherwise)
# ---------------------------------------------------------------------------

class TestRunFit:

    @pytest.fixture(autouse=True)
    def _require_sklearn(self):
        pytest.importorskip("sklearn")

    def test_writes_json_artifact_with_proposals(self, tmp_path):
        from src.services.scoring_fit import run_fit

        df = _toy_training_frame(n_per_vertical=100)
        csv_path = tmp_path / "training.csv"
        df.to_csv(csv_path, index=False)

        cfg = FitConfig(
            training_csv=csv_path,
            output_dir=tmp_path / "fit_out",
            run_id="t1",
        )
        out = run_fit(cfg)
        assert out.exists()

        artifact = json.loads(out.read_text())
        assert artifact["schema_version"] == 1
        assert len(artifact["proposals"]) == len(VERTICALS)
        proposed_verticals = {p["vertical"] for p in artifact["proposals"]}
        assert proposed_verticals == set(VERTICALS)
        # Every proposal must carry a vertical_weights map.
        for p in artifact["proposals"]:
            assert set(p["vertical_weights"].keys()) == set(SIGNAL_TYPES)
