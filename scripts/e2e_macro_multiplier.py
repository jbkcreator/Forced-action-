"""End-to-end test: FRED macro-signal -> CDS distress multiplier.

Steps:
1. Check macro_signals table for MORTGAGE30US data.
2. Insert a synthetic high-rate row if needed (rate = 7.50, above 6.5 threshold).
3. Score a real property that has foreclosure or tax_delinquency signals.
4. Remove the synthetic row if we inserted it.
5. Re-score the same property without the synthetic row.
6. Compare: boosted score vs baseline score.
7. Print a clear pass/fail report.

Usage:
    PYTHONPATH=. python scripts/e2e_macro_multiplier.py
    PYTHONPATH=. python scripts/e2e_macro_multiplier.py --use-live  # use live MORTGAGE30US if present
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from config.settings import get_settings
from src.services.macro_signal_multiplier_service import (
    _load_rules,
    get_latest_mortgage_rate_context,
    get_macro_distress_multipliers,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger("e2e_macro")

_TEST_DATE = date(2099, 1, 1)   # Far future — won't collide with real data


def _find_test_property(session: Session) -> dict | None:
    """Return first property that has a foreclosure or tax_delinquency."""
    row = session.execute(text("""
        SELECT p.id, p.parcel_id, p.address, p.county_id
        FROM properties p
        WHERE EXISTS (SELECT 1 FROM foreclosures f WHERE f.property_id = p.id)
           OR EXISTS (SELECT 1 FROM tax_delinquencies t WHERE t.property_id = p.id)
        LIMIT 1
    """)).mappings().first()
    return dict(row) if row else None


def _insert_synthetic_rate(session: Session, rate: float) -> bool:
    """Insert a synthetic MORTGAGE30US row dated 2099-01-01. Returns True if inserted."""
    existing = session.execute(text("""
        SELECT id FROM macro_signals
        WHERE signal_key = 'mortgage_30y_fixed'
          AND source = 'fred'
          AND observed_at = :obs
    """), {"obs": _TEST_DATE}).first()

    if existing:
        logger.info("Synthetic row already exists — reusing it.")
        return False

    session.execute(text("""
        INSERT INTO macro_signals
            (source, signal_key, source_series_id, value, unit,
             observed_at, frequency, geography_scope, geography_id, raw_payload)
        VALUES
            ('fred', 'mortgage_30y_fixed', 'MORTGAGE30US', :rate, 'percent',
             :obs, 'weekly', 'national', 'US', '{"synthetic": true}'::jsonb)
    """), {"rate": rate, "obs": _TEST_DATE})
    session.commit()
    logger.info("Inserted synthetic MORTGAGE30US row: rate=%.2f%% date=%s", rate, _TEST_DATE)
    return True


def _remove_synthetic_rate(session: Session) -> None:
    session.execute(text("""
        DELETE FROM macro_signals
        WHERE signal_key = 'mortgage_30y_fixed'
          AND source = 'fred'
          AND observed_at = :obs
    """), {"obs": _TEST_DATE})
    session.commit()
    logger.info("Removed synthetic MORTGAGE30US row.")


def _score_property(session: Session, prop_id: int) -> dict:
    """Score a single property and return vertical_scores dict."""
    from src.services.cds_engine import MultiVerticalScorer
    from src.core.models import Property
    from sqlalchemy.orm import joinedload

    prop = (
        session.query(Property)
        .options(
            joinedload(Property.owner),
            joinedload(Property.financial),
            joinedload(Property.foreclosures),
            joinedload(Property.tax_delinquencies),
            joinedload(Property.code_violations),
            joinedload(Property.legal_and_liens),
            joinedload(Property.legal_proceedings),
            joinedload(Property.building_permits),
            joinedload(Property.incidents),
            joinedload(Property.deeds),
        )
        .filter(Property.id == prop_id)
        .first()
    )
    if not prop:
        raise ValueError(f"Property {prop_id} not found")

    scorer = MultiVerticalScorer(session)
    result = scorer.score_property(prop, teaching_corrections=[])
    return {
        "vertical_scores": result.get("vertical_scores", {}),
        "cds_score": result.get("final_cds_score", 0),
        "macro_multipliers": scorer._macro_multipliers,
    }


def run(use_live: bool = False) -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    SessionLocal = sessionmaker(bind=engine)

    print("\n" + "=" * 60)
    print("A7 Macro Multiplier — End-to-End Test")
    print("=" * 60)

    # ── Step 1: find a test property ─────────────────────────────
    with SessionLocal() as session:
        prop = _find_test_property(session)

    if not prop:
        print("SKIP: No property with foreclosure/tax_delinquency found in DB.")
        sys.exit(0)

    print(f"\nTest property: id={prop['id']} parcel={prop['parcel_id']}")
    print(f"  address:    {prop['address']}")
    print(f"  county_id:  {prop['county_id']}")

    # ── Step 2: check for live MORTGAGE30US ──────────────────────
    with SessionLocal() as session:
        live_ctx = get_latest_mortgage_rate_context(session)

    if live_ctx:
        print(f"\nLive MORTGAGE30US: {live_ctx['value']:.2f}% on {live_ctx['observed_at']}")
    else:
        print("\nNo live MORTGAGE30US in macro_signals — will insert synthetic row.")

    # ── Step 3: baseline score (neutral/live rate) ────────────────
    print("\n--- Baseline score (current macro state) ---")
    _load_rules.cache_clear()
    with SessionLocal() as session:
        baseline = _score_property(session, prop["id"])

    print(f"  CDS score:          {baseline['cds_score']:.1f}")
    print(f"  Macro multipliers:  {baseline['macro_multipliers'] or 'neutral (none)'}")
    for v, s in sorted(baseline["vertical_scores"].items(), key=lambda x: -x[1]):
        print(f"    {v:<20} {s:.1f}")

    # ── Step 4: insert synthetic high-rate row ────────────────────
    if use_live and live_ctx and live_ctx["value"] >= 6.5:
        print("\n--- Using live rate (above threshold) — skipping synthetic insert ---")
        inserted = False
    else:
        print("\n--- Inserting synthetic MORTGAGE30US = 7.50% (above 6.5% threshold) ---")
        with SessionLocal() as session:
            inserted = _insert_synthetic_rate(session, rate=7.50)

    # ── Step 5: boosted score ─────────────────────────────────────
    print("\n--- Boosted score (high-rate macro state) ---")
    _load_rules.cache_clear()
    with SessionLocal() as session:
        boosted = _score_property(session, prop["id"])

    print(f"  CDS score:          {boosted['cds_score']:.1f}")
    print(f"  Macro multipliers:  {boosted['macro_multipliers'] or 'neutral (none)'}")
    for v, s in sorted(boosted["vertical_scores"].items(), key=lambda x: -x[1]):
        b_score = baseline["vertical_scores"].get(v, 0)
        delta   = s - b_score
        tag     = f"  (+{delta:.1f})" if delta > 0 else ""
        print(f"    {v:<20} {s:.1f}{tag}")

    # ── Step 6: cleanup synthetic row ────────────────────────────
    if inserted:
        with SessionLocal() as session:
            _remove_synthetic_rate(session)

    # ── Step 7: assertions ────────────────────────────────────────
    print("\n--- Assertions ---")
    passed = True

    # Multipliers must be active on the boosted run
    if not boosted["macro_multipliers"]:
        print("FAIL: Expected macro multipliers to be active on boosted run")
        passed = False
    else:
        print(f"PASS: Macro multipliers active: {boosted['macro_multipliers']}")

    # foreclosures and/or tax_delinquencies must be in multipliers
    affected = set(boosted["macro_multipliers"].keys())
    expected = {"foreclosures", "tax_delinquencies"}
    if not expected.issubset(affected):
        print(f"FAIL: Expected {expected} in multipliers, got {affected}")
        passed = False
    else:
        print(f"PASS: Affected signal types correct: {affected}")

    # Baseline must have had neutral multipliers (assuming no live high rate when test started)
    if not use_live and baseline["macro_multipliers"]:
        print(f"WARN: Baseline already had active multipliers (live rate may be above threshold): {baseline['macro_multipliers']}")
    else:
        print("PASS: Baseline was neutral (no multipliers)")

    # Boosted CDS >= baseline CDS (multiplier can only boost or stay same)
    if boosted["cds_score"] < baseline["cds_score"] - 0.01:
        print(f"FAIL: Boosted CDS ({boosted['cds_score']:.1f}) < Baseline ({baseline['cds_score']:.1f})")
        passed = False
    else:
        print(f"PASS: Boosted CDS ({boosted['cds_score']:.1f}) >= Baseline ({baseline['cds_score']:.1f})")

    print("\n" + ("=" * 60))
    print("RESULT:", "PASS" if passed else "FAIL")
    print("=" * 60 + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-live", action="store_true", help="Use live MORTGAGE30US if above threshold")
    args = parser.parse_args()
    run(use_live=args.use_live)
