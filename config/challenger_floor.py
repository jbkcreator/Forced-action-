"""
LEARN-v2.2 Layer 3 — challenger protected-floor monitor (PROPOSE-ONLY).

Spec §9.4 line 458: "challengers keep a protected floor (the existing 30/20/10
rule, now executed automatically)." Line 238: "~30% capacity reserved for
challengers until a winner, then 20% adjacent + 10% wild-card; formal challenge
when a challenger projects >=20% better retained gross profit." Line 259:
"Price variants may be PROPOSED per cell... never set."

WHY THIS IS A MEASURE-AND-PROPOSE LAYER, NOT FULL AUTO-EXECUTION.
Line 458 says the floor is "executed automatically," but full auto-execution is
not buildable now, for reasons that are structural, not a matter of effort:

1. NO DIVISIBLE POOL TO RESERVE 30% OF.
   L3's production model is per-cell (`limit x multiplier`, see
   config/cell_allocation.py:THROTTLE_FLOOR_PCT and the auto-double cell knob),
   not a single divisible capacity budget. "Reserve 30% of total capacity for
   challengers" presumes a total pool to carve up; that primitive does not
   exist. Auto-enforcement is DEFERRED pending a production-budget primitive.

2. NO RETAINED-GROSS-PROFIT PER CELL, AND NO DECLARED WINNER.
   The ">=20% better retained gross profit" formal-challenge trigger and the
   "20% adjacent + 10% wild-card" winner split both need retained-GP-per-cell
   and a declared per-cell winner. Pre-launch there are no sales, so neither
   exists (same gap cell_allocation.py documents for booking/payment columns).
   The GP challenge trigger and the 20/10 winner split are DEFERRED.

So this module implements the faithful MEASURE + PROPOSE half: it measures the
challenger cohort's send share against the 30% floor and PROPOSES a reserve to
Slack when the cohort is under it. It never reallocates capacity and never sets
a price — respecting line 259 ("never set").

LOCKED DECISIONS (grilled, spec-checked — do not re-open):
  A1  Propose-only, via Slack (no mutation).
  A2  A challenger is a DERIVED status: an eligible cell with NO L3 verdict yet.
  A3  Capacity is measured as SEND share (the only per-cell volume signal L3 has).
  A4  Cohort reserve, NOT a per-cell guarantee — L3 kill/throttle still governs
      individual challenger cells; the floor protects the cohort, not any one cell.
  A5  Winner 20/10 split + >=20% GP formal-challenge trigger DEFERRED (see above).

Usage:
    from config.challenger_floor import (
        CHALLENGER_FLOOR_PCT, WINDOW_DAYS, VERDICT_SAMPLE, ELIGIBLE_STAGES,
        MIN_TOTAL_SENDS, validate_challenger_config,
    )
"""
from __future__ import annotations

from config.venture_ladder import (
    AUTO_DOUBLE_ELIGIBLE_STAGES,
    AUTO_DOUBLE_MIN_SAMPLE,
    AUTO_DOUBLE_WINDOW_DAYS,
)

# Spec line 238 "~30%". The share of send capacity the challenger cohort should
# hold until a winner exists.
CHALLENGER_FLOOR_PCT = 30

# Reuse L3's 14-day trailing window so the two engines never disagree about the
# same cell's volume on the same day.
WINDOW_DAYS = AUTO_DOUBLE_WINDOW_DAYS

# A cell that has accumulated at least this many sends has effectively been
# judged by L3 (this is CL4/L3's decision sample), so it is no longer a
# challenger even if no verdict row has been written yet.
VERDICT_SAMPLE = AUTO_DOUBLE_MIN_SAMPLE

# Venture stage/active gating is L3's, unchanged: a venture that may not
# auto-double may not have its cohort share evaluated either.
ELIGIBLE_STAGES = AUTO_DOUBLE_ELIGIBLE_STAGES

# Below this venture-total send count the cohort share is statistical noise —
# skip proposing rather than fire on a handful of sends.
MIN_TOTAL_SENDS = 30


def validate_challenger_config() -> list[str]:
    """Return a list of configuration errors; empty means valid.

    Same spirit as config/cell_allocation.py:validate_allocation_config() — a
    floor that cannot express a coherent share should fail loudly, not silently
    propose nonsense.
    """
    errors: list[str] = []

    if not 0 < CHALLENGER_FLOOR_PCT < 100:
        errors.append(
            f"CHALLENGER_FLOOR_PCT must be strictly between 0 and 100, got "
            f"{CHALLENGER_FLOOR_PCT} (0 reserves nothing; >=100 reserves everything)"
        )

    if MIN_TOTAL_SENDS < 1:
        errors.append(
            f"MIN_TOTAL_SENDS must be at least 1, got {MIN_TOTAL_SENDS} — "
            "a zero floor would divide by zero on an empty venture"
        )

    if VERDICT_SAMPLE < 1:
        errors.append(
            f"VERDICT_SAMPLE must be positive, got {VERDICT_SAMPLE}"
        )

    if WINDOW_DAYS < 1:
        errors.append(
            f"WINDOW_DAYS must be positive, got {WINDOW_DAYS}"
        )

    return errors
