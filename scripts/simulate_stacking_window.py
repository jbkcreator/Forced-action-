"""
Stacking Window Simulation — Before vs After
=============================================
Shows how many of the 2,000 sampled properties changed tier or score
when the stacking window moved from 90 → 180 days.

Usage
-----
    python scripts/simulate_stacking_window.py
    python scripts/simulate_stacking_window.py --limit 5000
"""

import argparse
import logging
from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional

logging.basicConfig(level=logging.WARNING)

import config.scoring as _scoring_cfg
import src.services.cds_engine as _engine_module

from src.core.database import Database
from src.core.models import (
    Property, DistressScore,
)
from config.scoring import LEAD_TIER_THRESHOLDS, VERTICAL_WEIGHTS
from sqlalchemy.orm import joinedload


def _tier(score: float) -> str:
    for threshold, name in sorted(LEAD_TIER_THRESHOLDS, key=lambda x: x[0], reverse=True):
        if score >= threshold:
            return name
    return "Below Threshold"


_SIMULATED_SIGNAL_TYPES = [
    "judgment_liens",
    "code_violations",
    "tax_delinquencies",
    "probate",
    "building_permits",
    "evictions",
]


def _score_all(session, properties, window_days: int, inject_historical: bool = False) -> Dict[int, dict]:
    """
    Score every property in-memory.

    inject_historical=True  →  for each property that already has signals,
    inject one additional signal of a DIFFERENT type dated 120 days ago.
    This simulates a second scrape cycle landing in the 91-180 day range.

    With window=90  → injected signal is outside the window, no stacking bonus.
    With window=180 → injected signal is inside the window, stacking bonus fires.
    That delta is exactly what the window change will unlock over 6 months.
    """
    _scoring_cfg.STACKING_WINDOW_DAYS = window_days
    _engine_module.STACKING_WINDOW_DAYS = window_days

    scorer  = _engine_module.MultiVerticalScorer(session)
    today   = date.today()
    hist_date = today - timedelta(days=120)
    results = {}

    for prop in properties:
        signals = scorer._collect_signals(prop)
        if not signals:
            continue

        if inject_historical:
            existing_types = {s["type"] for s in signals}
            # Only simulate historical depth for properties that already have
            # 2+ distinct signal types — these are the ones that will naturally
            # accumulate signals across scrape cycles over 6 months.
            # Single-signal properties are unlikely to suddenly get a second type.
            if len(existing_types) >= 2:
                for candidate in _SIMULATED_SIGNAL_TYPES:
                    if candidate not in existing_types:
                        signals = signals + [{"type": candidate, "date": hist_date, "amount": None}]
                        break

        best = 0.0
        viol_count = sum(1 for s in signals if s["type"] == "code_violations")
        for vertical in VERTICAL_WEIGHTS:
            r = scorer._score_vertical(
                vertical=vertical,
                signals=signals,
                owner=prop.owner,
                financial=prop.financial,
                violation_count=viol_count,
            )
            if r["score"] > best:
                best = r["score"]

        results[prop.id] = {"score": round(best, 2), "tier": _tier(best)}

    return results


def main(limit: int) -> None:
    db = Database()

    print(f"\nLoading {limit:,} properties...", flush=True)
    with db.session_scope() as session:
        properties = (
            session.query(Property)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .options(
                joinedload(Property.owner),
                joinedload(Property.financial),
                joinedload(Property.code_violations),
                joinedload(Property.legal_and_liens),
                joinedload(Property.deeds),
                joinedload(Property.legal_proceedings),
                joinedload(Property.tax_delinquencies),
                joinedload(Property.foreclosures),
                joinedload(Property.building_permits),
                joinedload(Property.incidents),
            )
            .distinct()
            .limit(limit)
            .all()
        )

        total = len(properties)
        print(f"Loaded {total:,}. Scoring...", flush=True)

        # BEFORE: 90-day window, current signals only (no historical depth)
        before = _score_all(session, properties, window_days=90,  inject_historical=False)
        # AFTER:  180-day window + simulated second signal at 120 days old
        #         represents what happens once scrapers have 6 months of history
        after  = _score_all(session, properties, window_days=180, inject_historical=True)

        # Restore
        _scoring_cfg.STACKING_WINDOW_DAYS = 180
        _engine_module.STACKING_WINDOW_DAYS = 180

    # ── Compare ──────────────────────────────────────────────────────────────
    scored_ids   = set(before) & set(after)
    changed      = [(pid, before[pid], after[pid]) for pid in scored_ids
                    if before[pid]["tier"] != after[pid]["tier"] or before[pid]["score"] != after[pid]["score"]]

    tier_upgrades   = [(pid, b, a) for pid, b, a in changed if b["tier"] != a["tier"]]
    score_only      = [(pid, b, a) for pid, b, a in changed
                       if b["tier"] == a["tier"] and b["score"] != a["score"]]

    # Tier movement matrix
    tier_order = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
    moves: Counter = Counter()
    for _, b, a in tier_upgrades:
        moves[(b["tier"], a["tier"])] += 1

    score_deltas = [a["score"] - b["score"] for _, b, a in score_only]
    avg_delta    = sum(score_deltas) / len(score_deltas) if score_deltas else 0

    # ── Print ─────────────────────────────────────────────────────────────────
    W = 60
    print()
    print("═" * W)
    print("  SIMULATION: 90-day window (now)  →  180-day window (6 months)")
    print(f"  Sample size: {total:,} properties")
    print("═" * W)
    print(f"  Properties scored:          {len(scored_ids):,}")
    print(f"  Properties changed:         {len(changed):,}  ({len(changed)/len(scored_ids)*100:.1f}%)")
    print(f"    — Tier upgraded:          {len(tier_upgrades):,}")
    print(f"    — Score improved (same tier): {len(score_only):,}  (avg +{avg_delta:.1f} pts)")
    print()

    if tier_upgrades:
        print("  TIER UPGRADES")
        print("─" * W)
        for (from_tier, to_tier), count in sorted(moves.items(), key=lambda x: -x[1]):
            print(f"  {from_tier:<20} → {to_tier:<20}  {count:>5} properties")
        print()

    # Before/after tier totals
    before_tiers: Counter = Counter(v["tier"] for v in before.values())
    after_tiers:  Counter = Counter(v["tier"] for v in after.values())

    print("  TIER DISTRIBUTION")
    print("─" * W)
    print(f"  {'Tier':<22} {'Before (90d)':>14} {'After (180d)':>14} {'Delta':>8}")
    print("─" * W)
    for tier in tier_order:
        b = before_tiers.get(tier, 0)
        a = after_tiers.get(tier, 0)
        d = a - b
        sign = f"+{d}" if d > 0 else str(d)
        print(f"  {tier:<22} {b:>14,} {a:>14,} {sign:>8}")

    gold_plus = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
    gb = sum(before_tiers.get(t, 0) for t in gold_plus)
    ga = sum(after_tiers.get(t, 0)  for t in gold_plus)
    gd = ga - gb
    sign = f"+{gd}" if gd > 0 else str(gd)
    print("─" * W)
    print(f"  {'Gold+ Total':<22} {gb:>14,} {ga:>14,} {sign:>8}")
    print("═" * W)
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000)
    args = parser.parse_args()
    main(limit=args.limit)
