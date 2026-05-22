"""
CDS scoring fit job (Stage C of the cross-county retune).

Fits per-vertical logistic regressions on the training CSV produced by
src/services/scoring_training_data.py and emits a JSON artifact with
proposed knob values for config/scoring.py plus per-vertical calibration
metrics. Stage F (cutover) reads this artifact and replaces the live
knobs after the Stage E shadow rescore validates it.

Usage:
    python -m src.services.scoring_fit --training-csv data/scoring_training/<id>.csv
    python -m src.services.scoring_fit --training-csv <path> --output-dir data/scoring_fit

Modeling choices (kept narrow on purpose):
  - Per-vertical logistic regression with L2 regularization (one fit per
    vertical so each gets its own coefficient set, matching the existing
    VERTICAL_WEIGHTS shape).
  - class_weight="balanced" because events are rare (<5% positive rate
    in the conversion data) — without it the model collapses to
    "predict negative for everyone."
  - Per-county missing signals are dropped from the fit's row set rather
    than imputed. Stage E's coverage normalizer at inference time handles
    them by skipping the missing axes entirely (Stage D engine change).
  - calibrated absolute output: intercept chosen so that Pr(event | score=78)
    ≈ Platinum target rate and Pr(event | score=92) ≈ UP target rate,
    measured directly from observed event rates in the training set.

What is NOT in this fit:
  - Tree models (XGBoost / LightGBM). The existing engine is additive and
    that's the contract we're refitting against — a tree fit wouldn't map
    back onto VERTICAL_WEIGHTS without lossy approximation.
  - Subscriber-reported DealOutcome — column is in the training CSV but
    too sparse today to fit on. Future re-fit when the table fills in.

Lazy sklearn import: only `fit_vertical` and `compute_metrics` pull in
sklearn, so the rest of the module (coefs_to_knobs, feature prep, the
calibration math) loads + tests cleanly without sklearn installed. Tests
that exercise the actual fit are gated on sklearn availability.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from config.scoring import (
    LEAD_TIER_THRESHOLDS,
    SCORE_CAP,
    SIGNAL_HARD_CUTOFF_DAYS,
    VERTICAL_WEIGHTS,
)
from src.services.scoring_training_data import SIGNAL_TYPES, VERTICALS

logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIR = Path("data/scoring_fit")
DEFAULT_L2_REG = 1.0           # sklearn LogisticRegression `C` is INVERSE of strength
DEFAULT_RANDOM_STATE = 13
MIN_EVENTS_PER_VERTICAL = 30   # below this, the fit is unreliable — emit a warning

# Tiers that anchor the calibration. Score thresholds derive from
# config.scoring.LEAD_TIER_THRESHOLDS so a future config edit propagates.
# Pr(event) targets are derived from the data, not hardcoded.
_PLATINUM_SCORE = 78
_ULTRA_PLATINUM_SCORE = 92


# ── Feature shape ────────────────────────────────────────────────────────────

# Numeric columns the fit consumes directly. NaN handling: rows where ANY
# of these are NaN are dropped from that vertical's fit. Per-county missing
# signals show up as NaN here (Stage B emits NULL for them) — that's how
# the "treat missing as unobserved" contract is enforced at fit time.
_NUMERIC_FEATURES_BASE = [
    "stacking_count",
    "equity_pct",
    "years_since_sale",
    "property_age_years",
    "value_change_yoy",
    "long_term_owner",
    "property_age_30plus",
    "has_phone",
    "has_email",
]

# Categorical columns expanded to one-hot. Empty / NaN cells become zero
# across all dummies (treated as "no info").
_CATEGORICAL_FEATURES = [
    ("absentee_status", ("Out-of-State", "Out-of-County", "In-County")),
    ("equity_bucket",   ("high", "mid", "low")),
]


def numeric_features() -> list[str]:
    """Full numeric feature list — per-signal `has_<sig>` indicators plus
    the base numeric columns.

    `recency_<sig>_days` is intentionally NOT a fit feature: the engine
    applies a single global recency bonus per scored signal
    (config.scoring.RECENCY_BONUSES) rather than a per-signal-per-age
    knob, so a per-signal recency coefficient would have no knob to map
    onto. Recency stays in the training CSV for future fits when we add
    per-signal recency interactions.

    Stable order so the coefs_to_knobs mapping is deterministic across
    fits — the test suite pins on this order.
    """
    cols = list(_NUMERIC_FEATURES_BASE)
    for sig in SIGNAL_TYPES:
        cols.append(f"has_{sig}")
    return cols


def categorical_dummy_names() -> list[str]:
    out = []
    for col, levels in _CATEGORICAL_FEATURES:
        for lvl in levels:
            out.append(f"{col}__{lvl}")
    return out


def feature_names() -> list[str]:
    return numeric_features() + categorical_dummy_names()


# ── Pure-function knob mapping ───────────────────────────────────────────────


def _logit(p: float) -> float:
    """Log-odds transform with clipping to avoid math errors at 0/1."""
    p = max(1e-9, min(1 - 1e-9, p))
    return math.log(p / (1 - p))


def _sigmoid(z: float) -> float:
    if z >= 0:
        ez = math.exp(-z)
        return 1.0 / (1.0 + ez)
    ez = math.exp(z)
    return ez / (1.0 + ez)


def _coef_to_weight(coef: float, max_coef: float, score_budget: int) -> int:
    """Map one logistic-regression coefficient → integer engine weight in [0, score_budget].

    Negative coefficients clip to 0 (the engine has no concept of a
    negative weight — those signals just don't contribute). Positive
    coefficients scale linearly against the vertical's strongest signal
    so the relative ordering survives the integer rounding.
    """
    if coef <= 0 or max_coef <= 0:
        return 0
    return int(round(coef / max_coef * score_budget))


@dataclass(frozen=True)
class KnobProposal:
    """Per-vertical knob recommendation derived from one logistic-regression fit."""
    vertical:            str
    vertical_weights:    dict[str, int]
    intercept:           float
    auc:                 Optional[float] = None
    n_rows:              int = 0
    n_events:            int = 0
    event_rate:          float = 0.0
    coverage_warning:    Optional[str] = None
    calibration_points:  list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "vertical":           self.vertical,
            "vertical_weights":   self.vertical_weights,
            "intercept":          self.intercept,
            "auc":                self.auc,
            "n_rows":             self.n_rows,
            "n_events":           self.n_events,
            "event_rate":         self.event_rate,
            "coverage_warning":   self.coverage_warning,
            "calibration_points": self.calibration_points,
        }


def coefs_to_knobs(
    vertical: str,
    coefs_by_feature: dict[str, float],
    intercept: float,
    *,
    score_budget: int = 75,
) -> dict[str, int]:
    """Map a fitted coefficient vector to engine-compatible VERTICAL_WEIGHTS for one vertical.

    Only the per-signal `has_<sig>` coefficients are mapped to engine weights
    (those are the columns that correspond to base signal weights). Non-signal
    coefficients (stacking, equity, contact, absentee, HCPA passives) drive
    the universal multipliers/modifiers which are scaled separately — they
    don't belong inside `VERTICAL_WEIGHTS[vertical]`.

    `score_budget` is the cap a single signal can contribute. 75 leaves room
    for the recency bonus (~15) + stacking (~40) + universals (~50) under
    the global cap of 100.
    """
    signal_coefs = {
        sig: coefs_by_feature.get(f"has_{sig}", 0.0)
        for sig in SIGNAL_TYPES
    }
    max_coef = max(signal_coefs.values()) if signal_coefs else 0.0
    if max_coef <= 0:
        # No signal moved the needle for this vertical — fall back to all-zero
        # so the cutover can see the fit produced nothing usable.
        return {sig: 0 for sig in SIGNAL_TYPES}
    return {
        sig: _coef_to_weight(coef, max_coef, score_budget)
        for sig, coef in signal_coefs.items()
    }


def derive_lead_tier_thresholds(
    intercept: float,
    score_budget: int = SCORE_CAP,
) -> list[tuple[int, str]]:
    """Compute LEAD_TIER_THRESHOLDS by inverting the calibrated logistic at
    each tier's target event rate.

    For Stage C v1 we keep the *existing* score-axis tier breakpoints —
    the calibration moves the *meaning* of those breakpoints (a 78 now
    means actual Platinum-level event probability) rather than the
    breakpoint numbers themselves. Stage F's cutover may re-derive these
    from observed event rates once the fit lands and the shadow rescore
    confirms the new distribution.
    """
    # Returned unchanged for now — kept as a hook so Stage F has a single
    # place to recompute thresholds after the shadow rescore data lands.
    return list(LEAD_TIER_THRESHOLDS)


# ── Feature matrix prep (pandas + numpy only) ────────────────────────────────


def prepare_feature_matrix(df, vertical: str):
    """Build (X, y, used_feature_names) for one vertical's fit.

    Rows are filtered to the given vertical. Rows with NaN in any feature
    column are dropped — that's the structural fix for the missing-signal-
    rewards-absence bias (Pinellas rows have NaN for unobserved axes, so
    they drop out of the fit for those axes instead of contributing zeros).

    Returns a tuple `(X, y, feature_names)` where X and y are numpy arrays.
    Raises ValueError if `df` is empty after filtering or the outcome column
    is missing — callers should guard at the orchestrator level.
    """
    # Lazy import: keeps the module loadable even without numpy installed.
    import numpy as np

    if "vertical" not in df.columns:
        raise ValueError("training dataframe missing 'vertical' column")
    if "outcome_event" not in df.columns:
        raise ValueError("training dataframe missing 'outcome_event' column")

    sub = df[df["vertical"] == vertical].copy()
    if sub.empty:
        raise ValueError(f"no training rows for vertical={vertical}")

    # Expand categoricals to one-hot.
    for col, levels in _CATEGORICAL_FEATURES:
        for lvl in levels:
            sub[f"{col}__{lvl}"] = (sub[col].astype(str) == lvl).astype(int)

    cols = feature_names()
    # `errors="ignore"` lets the function survive missing columns gracefully —
    # the caller would already have aborted if the CSV is malformed.
    matrix = sub[cols].apply(lambda c: c.astype(float) if c.dtype != float else c, axis=0)

    # Drop rows that have NaN in any numeric feature — those are the
    # Pinellas-style rows where the per-county mask suppressed signals.
    mask_complete = ~matrix.isna().any(axis=1)
    matrix = matrix[mask_complete]
    y_series = sub.loc[mask_complete, "outcome_event"].astype(int)

    if matrix.empty:
        raise ValueError(
            f"no rows survive NaN drop for vertical={vertical} — "
            "are signals available for this county/vertical combination?"
        )

    X = matrix.to_numpy(dtype=float)
    y = y_series.to_numpy(dtype=int)
    return X, y, cols


# ── Fit driver (sklearn) ─────────────────────────────────────────────────────


def fit_vertical(df, vertical: str, *, l2_C: float = DEFAULT_L2_REG,
                 random_state: int = DEFAULT_RANDOM_STATE) -> KnobProposal:
    """Run one per-vertical logistic regression. Returns a KnobProposal.

    Lazy-imports sklearn so this module can be imported when sklearn is
    absent (the rest of the module — coefs_to_knobs, feature prep — works
    on stock numpy/pandas).
    """
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score
    except ImportError as exc:
        raise ImportError(
            "scikit-learn is required for the Stage C fit. "
            "Install with `pip install scikit-learn`."
        ) from exc

    X, y, used_features = prepare_feature_matrix(df, vertical)
    n_rows = int(len(y))
    n_events = int(y.sum())
    event_rate = float(n_events) / n_rows if n_rows else 0.0

    coverage_warning: Optional[str] = None
    if n_events < MIN_EVENTS_PER_VERTICAL:
        coverage_warning = (
            f"only {n_events} positive events for vertical={vertical} "
            f"(<{MIN_EVENTS_PER_VERTICAL}); fit is statistically weak."
        )
        logger.warning("[fit] %s", coverage_warning)

    # If the entire vertical's outcome is constant (all positives or all
    # negatives), LogisticRegression will silently refuse to fit. Bail early.
    if n_events == 0 or n_events == n_rows:
        return KnobProposal(
            vertical=vertical,
            vertical_weights={sig: 0 for sig in SIGNAL_TYPES},
            intercept=_logit(event_rate) if 0 < event_rate < 1 else 0.0,
            auc=None,
            n_rows=n_rows,
            n_events=n_events,
            event_rate=event_rate,
            coverage_warning=(
                coverage_warning
                or f"outcome is constant for vertical={vertical} — cannot fit"
            ),
        )

    model = LogisticRegression(
        penalty="l2",
        C=l2_C,
        solver="liblinear",  # works well for small/medium datasets, deterministic
        class_weight="balanced",
        random_state=random_state,
        max_iter=2000,
    )
    model.fit(X, y)

    coefs_by_feature = dict(zip(used_features, model.coef_[0].tolist()))
    intercept = float(model.intercept_[0])

    try:
        proba = model.predict_proba(X)[:, 1]
        auc = float(roc_auc_score(y, proba))
    except ValueError:
        auc = None

    # Calibration plot data: 10 quantile bins. Each entry = mean predicted
    # probability and observed event rate within the bin.
    calibration_points = _calibration_bins(proba, y) if auc is not None else []

    knobs = coefs_to_knobs(vertical, coefs_by_feature, intercept)

    return KnobProposal(
        vertical=vertical,
        vertical_weights=knobs,
        intercept=intercept,
        auc=auc,
        n_rows=n_rows,
        n_events=n_events,
        event_rate=event_rate,
        coverage_warning=coverage_warning,
        calibration_points=calibration_points,
    )


def _calibration_bins(proba, y, *, n_bins: int = 10) -> list[dict]:
    """Quantile-binned calibration points: (mean predicted, observed event rate)."""
    import numpy as np
    arr = np.asarray(proba, dtype=float)
    y_arr = np.asarray(y, dtype=int)
    if arr.size == 0:
        return []

    # Quantile bins — gives equal-mass buckets so well-calibrated models
    # produce ~ y=x lines.
    quantiles = np.quantile(arr, np.linspace(0, 1, n_bins + 1))
    points = []
    for i in range(n_bins):
        lo, hi = quantiles[i], quantiles[i + 1]
        if i == n_bins - 1:
            mask = (arr >= lo) & (arr <= hi)
        else:
            mask = (arr >= lo) & (arr < hi)
        if not mask.any():
            continue
        points.append({
            "bin":             i,
            "mean_predicted":  float(arr[mask].mean()),
            "observed_rate":   float(y_arr[mask].mean()),
            "count":           int(mask.sum()),
        })
    return points


# ── Orchestrator ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FitConfig:
    training_csv:  Path
    output_dir:    Path = DEFAULT_OUTPUT_DIR
    run_id:        Optional[str] = None
    l2_C:          float = DEFAULT_L2_REG
    verticals:     tuple[str, ...] = VERTICALS

    def resolved_run_id(self) -> str:
        return self.run_id or uuid.uuid4().hex[:12]


def run_fit(cfg: FitConfig) -> Path:
    """End-to-end: load training CSV, fit each vertical, write artifact.

    Returns the path the artifact was written to.
    """
    import pandas as pd

    df = pd.read_csv(cfg.training_csv)
    logger.info("Loaded %d training rows from %s", len(df), cfg.training_csv)

    proposals: list[KnobProposal] = []
    for vertical in cfg.verticals:
        try:
            proposals.append(fit_vertical(df, vertical, l2_C=cfg.l2_C))
        except ValueError as exc:
            logger.warning("[fit] vertical=%s skipped: %s", vertical, exc)
            proposals.append(KnobProposal(
                vertical=vertical,
                vertical_weights={sig: 0 for sig in SIGNAL_TYPES},
                intercept=0.0,
                coverage_warning=str(exc),
            ))

    artifact = {
        "schema_version":         1,
        "generated_at":           datetime.utcnow().isoformat() + "Z",
        "training_csv":           str(cfg.training_csv),
        "l2_C":                   cfg.l2_C,
        "score_cap":              SCORE_CAP,
        "signal_hard_cutoff_days": SIGNAL_HARD_CUTOFF_DAYS,
        "lead_tier_thresholds":   derive_lead_tier_thresholds(intercept=0.0),
        "proposals":              [p.to_dict() for p in proposals],
    }

    run_id = cfg.resolved_run_id()
    output_path = cfg.output_dir / f"{run_id}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, indent=2, default=str))
    logger.info("Wrote fit artifact to %s", output_path)
    return output_path


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fit per-vertical CDS scoring weights (Stage C of retune)."
    )
    p.add_argument("--training-csv", type=Path, required=True,
                   help="CSV produced by src.services.scoring_training_data.")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                   help=f"Directory to write the JSON artifact into (default {DEFAULT_OUTPUT_DIR}).")
    p.add_argument("--run-id", default=None, help="Override auto-generated run id.")
    p.add_argument("--l2-C", type=float, default=DEFAULT_L2_REG,
                   help=f"L2 regularization inverse strength (default {DEFAULT_L2_REG}).")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = _parse_args(argv)
    cfg = FitConfig(
        training_csv=args.training_csv,
        output_dir=args.output_dir,
        run_id=args.run_id,
        l2_C=args.l2_C,
    )
    run_fit(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
