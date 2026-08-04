"""
LEARN-v2.2 T-LEARN-06 — feature-to-revenue learning.

Spec §9.4: "which target characteristics correlate with replies, purchases,
retention; feeds targeting weights."

What this does today:
  - Feature source: ExperimentDecisionSnapshot.target_characteristics — the
    frozen-at-decision-time JSONB feature copy. NULL-tolerant: these are NULL
    until Hunter's buyer-profiling branch merges, so when none are populated we
    return an empty "insufficient evidence" report rather than raising.
  - Outcome label: reply ONLY for now. A thread "replied" if an
    experiment_attributions row with event_type='reply.received' exists for it.
    purchase / retention are deferred.
  - Min-N floor: 30. Below MIN_N observations for a (feature_key, feature_value)
    we report "insufficient evidence (N=X)" and no rate.

Weights are PROPOSE-ONLY: this reports correlations. It never writes into any
ranking. The target ranking is NBRA (src/services/nbra_engine.py); it is named
in the proposal text but never called.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

MIN_N = 30


@dataclass
class FeatureCorrelation:
    feature_key: str
    feature_value: str
    n: int
    replies: int
    reply_rate_pct: Optional[float]  # None when n < MIN_N
    sufficient: bool


@dataclass
class FeatureRevenueReport:
    snapshots_scanned: int = 0
    features: list[FeatureCorrelation] = field(default_factory=list)      # sufficient (n >= MIN_N)
    insufficient: list[FeatureCorrelation] = field(default_factory=list)  # n < MIN_N


def _aggregate(observations: list[tuple[str, str, bool]]) -> list[FeatureCorrelation]:
    """Pure aggregation: (feature_key, feature_value, replied) tuples ->
    per-(key, value) FeatureCorrelation with the MIN_N floor applied.

    No DB access — unit-testable in isolation.
    """
    counts: dict[tuple[str, str], list[int]] = {}  # (key, value) -> [n, replies]
    for key, value, replied in observations:
        bucket = counts.setdefault((key, value), [0, 0])
        bucket[0] += 1
        if replied:
            bucket[1] += 1

    out: list[FeatureCorrelation] = []
    for (key, value), (n, replies) in counts.items():
        sufficient = n >= MIN_N
        reply_rate_pct = round(100.0 * replies / n, 2) if sufficient else None
        out.append(FeatureCorrelation(
            feature_key=key,
            feature_value=value,
            n=n,
            replies=replies,
            reply_rate_pct=reply_rate_pct,
            sufficient=sufficient,
        ))
    return out


def run_feature_revenue_analysis(db: Session) -> FeatureRevenueReport:
    """Correlate frozen target_characteristics with reply outcomes. Never raises.

    Bulk-fetches snapshots and replied threads once, then aggregates in Python
    (no query-in-loop).
    """
    try:
        snapshot_rows = db.execute(text("""
            SELECT opportunity_thread_id, target_characteristics
            FROM experiment_decision_snapshots
            WHERE target_characteristics IS NOT NULL
        """)).fetchall()
    except Exception as exc:
        logger.warning("feature_revenue: snapshot fetch failed: %s", exc)
        return FeatureRevenueReport()

    if not snapshot_rows:
        # NULL-tolerant "insufficient evidence" case — Hunter's profiling has
        # not populated target_characteristics yet.
        return FeatureRevenueReport(snapshots_scanned=0)

    thread_ids = list({r.opportunity_thread_id for r in snapshot_rows})

    try:
        replied_rows = db.execute(text("""
            SELECT DISTINCT opportunity_thread_id
            FROM experiment_attributions
            WHERE event_type = 'reply.received'
              AND opportunity_thread_id = ANY(:thread_ids)
        """), {"thread_ids": thread_ids}).fetchall()
    except Exception as exc:
        logger.warning("feature_revenue: reply fetch failed: %s", exc)
        return FeatureRevenueReport(snapshots_scanned=len(snapshot_rows))

    replied_threads = {r.opportunity_thread_id for r in replied_rows}

    observations: list[tuple[str, str, bool]] = []
    for row in snapshot_rows:
        characteristics = row.target_characteristics or {}
        if not isinstance(characteristics, dict):
            continue
        replied = row.opportunity_thread_id in replied_threads
        for key, value in characteristics.items():
            observations.append((str(key), str(value), replied))

    correlations = _aggregate(observations)

    report = FeatureRevenueReport(snapshots_scanned=len(snapshot_rows))
    for corr in correlations:
        if corr.sufficient:
            report.features.append(corr)
        else:
            report.insufficient.append(corr)

    report.features.sort(key=lambda c: c.reply_rate_pct or 0.0, reverse=True)
    return report
