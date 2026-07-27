"""
A/B testing engine — deterministic assignment, outcome recording, auto-rollback.

Two assignment functions exist for two different test shapes:
  assign_variant      — message-swap a/b tests; out-of-test traffic → None (unrecorded).
  assign_rollout_arm  — rollout tests (e.g. lifecycle_attribution_v1); records BOTH arms
                        ('variant' / 'control') so control conversion rate is measurable.
"""

import hashlib
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.lifecycle_guardrails import get_guardrail, is_within_guardrail
from src.core.models import AbAssignment, AbTest

logger = logging.getLogger(__name__)


def get_or_create_test(
    test_name: str,
    segment: str,
    variant_a: dict,
    variant_b: dict,
    traffic_pct: int,
    db: Session,
) -> AbTest:
    cap = get_guardrail("ab_test_traffic_cap")["max_pct"]
    capped_pct = min(traffic_pct, cap)

    existing = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if existing:
        # Sync traffic_pct from YAML config (source of truth) to DB.
        if existing.traffic_pct != capped_pct:
            logger.info(
                "ab_engine: syncing traffic_pct for %s: %s → %s",
                test_name, existing.traffic_pct, capped_pct,
            )
            existing.traffic_pct = capped_pct
            db.flush()
        return existing
    test = AbTest(
        test_name=test_name,
        segment=segment,
        variant_a=variant_a,
        variant_b=variant_b,
        traffic_pct=capped_pct,
        status="active",
    )
    db.add(test)
    db.flush()
    return test


def assign_variant(subscriber_id: int, test_name: str, db: Session) -> Optional[str]:
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name, AbTest.status == "active")
    ).scalar_one_or_none()
    if not test:
        return None

    existing = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.subscriber_id == subscriber_id,
        )
    ).scalar_one_or_none()
    if existing:
        return existing.variant

    # Deterministic hash — same subscriber always gets same variant
    h = int(hashlib.md5(f"{test_name}{subscriber_id}".encode()).hexdigest(), 16) % 100
    if h >= test.traffic_pct:
        return None

    variant = "a" if h % 2 == 0 else "b"
    assignment = AbAssignment(
        test_id=test.id,
        subscriber_id=subscriber_id,
        variant=variant,
    )
    db.add(assignment)
    db.flush()
    return variant


ATTRIBUTION_ROLLOUT_TEST_NAME = "lifecycle_attribution_v1"


def ensure_attribution_rollout_test(db: Session) -> AbTest:
    """Idempotently register the lifecycle_attribution_v1 rollout test.

    Called lazily from decision_hierarchy so the test row exists before
    assign_rollout_arm tries to look it up.
    """
    return get_or_create_test(
        test_name=ATTRIBUTION_ROLLOUT_TEST_NAME,
        segment="attribution_eligible",
        variant_a={"path": "control"},
        variant_b={"path": "attribution_driven"},
        traffic_pct=10,
        db=db,
    )


def get_or_create_holdout_test(
    test_name: str,
    segment: str,
    traffic_pct: int,
    db: Session,
    baseline_fingerprint: Optional[str] = None,
) -> AbTest:
    """Idempotently register a Task 4.1 control-holdout rollout test.

    Distinct from get_or_create_test: it does NOT apply the
    ab_test_traffic_cap guardrail. That cap limits AI-copy EXPOSURE for
    message-swap tests (keep experimental copy under X% of a segment); a
    holdout's traffic_pct is the opposite — the treatment MAJORITY
    (100 - control_pct, e.g. 90), with the small remainder held out as a
    frozen control. Capping it would invert the split (90% control, 10%
    treatment). Syncs traffic_pct on every call so a control_pct change in
    lifecycle_holdout_tests.yaml takes effect on restart.

    baseline_fingerprint: content hash of the graph's base prompt at creation
    (loader.base_prompt_fingerprint). Stored in variant_b and re-checked by
    lifecycle_holdout_check so a verdict never promotes on a baseline that drifted
    mid-experiment. Recorded once at creation and NOT re-synced — that's the
    point: it captures the baseline the control arm was measured against.
    """
    existing = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if existing:
        if existing.traffic_pct != traffic_pct:
            existing.traffic_pct = traffic_pct
            db.flush()
        return existing
    test = AbTest(
        test_name=test_name,
        segment=segment,
        variant_a={"path": "variant"},
        variant_b={"path": "control", "baseline_fingerprint": baseline_fingerprint or ""},
        traffic_pct=traffic_pct,
        status="active",
    )
    db.add(test)
    db.flush()
    return test


def assign_rollout_arm(
    subscriber_id: int,
    test_name: str,
    db: Session,
) -> Optional[str]:
    """Assign a subscriber to 'variant' or 'control' for a rollout-type test.

    Unlike assign_variant, BOTH arms are recorded as AbAssignment rows so that
    control conversion rate is measurable alongside the variant's. Returns None
    only when the test doesn't exist or is not active.

    Deterministic: same subscriber always gets the same arm for the life of the test.
    """
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name, AbTest.status == "active")
    ).scalar_one_or_none()
    if not test:
        return None

    existing = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.subscriber_id == subscriber_id,
        )
    ).scalar_one_or_none()
    if existing:
        return existing.variant

    h = int(hashlib.md5(f"{test_name}{subscriber_id}".encode()).hexdigest(), 16) % 100
    arm = "variant" if h < test.traffic_pct else "control"
    db.add(AbAssignment(
        test_id=test.id,
        subscriber_id=subscriber_id,
        variant=arm,
    ))
    db.flush()
    return arm


ANNUAL_SIGNUP_TEST_NAME = "annual_at_signup_v1"


def ensure_annual_signup_test(db: Session) -> AbTest:
    """Idempotently register the annual_at_signup_v1 rollout test.

    Called lazily from signup_engine so the test row exists before
    record_pregenerated_arm tries to look it up. traffic_pct is read from
    settings.annual_signup_test_traffic_pct (ANNUAL_SIGNUP_TEST_TRAFFIC_PCT
    env var) — toggleable without a code change — and is still capped to the
    shared ab_test_traffic_cap guardrail by get_or_create_test, same as every
    other test registered this way. get_or_create_test re-syncs the DB row's
    traffic_pct on every call, so changing the env var and restarting is
    enough to take effect.
    """
    from config.settings import settings
    return get_or_create_test(
        test_name=ANNUAL_SIGNUP_TEST_NAME,
        segment="new_signups",
        variant_a={"path": "control"},
        variant_b={"path": "annual_offer_shown"},
        traffic_pct=settings.annual_signup_test_traffic_pct,
        db=db,
    )


FOLLOWUP_CADENCE_TEST_NAME = "followup_cadence_v1"

# arm -> (second_touch_days, third_touch_days). control is today's literal
# requirement; variant tests a later cadence. assign_rollout_arm() only ever
# returns the arm label, not this dict, so callers map the label themselves.
FOLLOWUP_CADENCE_ARMS = {
    "control": (2, 5),
    "variant": (3, 6),
}


def ensure_followup_cadence_test(db: Session) -> AbTest:
    """Idempotently register the followup_cadence_v1 rollout test.

    Called lazily from the auto_mode_followup_sms sweep so the test row
    exists before assign_rollout_arm looks it up. 10% traffic_pct only
    controls the variant/control split (both arms get 100% coverage) —
    see FOLLOWUP_CADENCE_ARMS for the actual day thresholds per arm.
    """
    return get_or_create_test(
        test_name=FOLLOWUP_CADENCE_TEST_NAME,
        segment="auto_mode_first_touch_no_reply",
        variant_a={"path": "control", "second_touch_days": 2, "third_touch_days": 5},
        variant_b={"path": "variant", "second_touch_days": 3, "third_touch_days": 6},
        traffic_pct=10,
        db=db,
    )


def record_pregenerated_arm(
    subscriber_id: int,
    test_name: str,
    arm: str,
    db: Session,
) -> Optional[str]:
    """Persist a client-precomputed rollout arm for a subscriber.

    Unlike assign_rollout_arm, this does not derive the arm from
    subscriber_id — it trusts a value the caller already decided before a
    subscriber_id existed (e.g. an anonymous landing-page visitor bucketed
    client-side before signing up). Only "variant"/"control" are accepted;
    anything else is a no-op. Idempotent: an existing assignment for this
    subscriber is returned unchanged rather than overwritten.
    """
    if arm not in ("variant", "control"):
        return None

    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name, AbTest.status == "active")
    ).scalar_one_or_none()
    if not test:
        return None

    existing = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.subscriber_id == subscriber_id,
        )
    ).scalar_one_or_none()
    if existing:
        return existing.variant

    db.add(AbAssignment(
        test_id=test.id,
        subscriber_id=subscriber_id,
        variant=arm,
    ))
    db.flush()
    return arm


def should_rollback_rollout(
    test_name: str,
    db: Session,
    *,
    window_hours: int = 48,
    min_per_arm: int = 30,
) -> bool:
    """Return True when the 'variant' arm is losing by >2σ vs 'control' in the
    rolling window, with ≥min_per_arm assignments per arm.

    Fail-safe hold: returns False (no rollback) when the floor isn't met or
    when the z-test is indeterminate (p_pool ∈ {0,1}, se==0).
    """
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name, AbTest.status == "active")
    ).scalar_one_or_none()
    if not test:
        return False

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    assignments = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.created_at >= cutoff,
        )
    ).scalars().all()

    ctrl = [a for a in assignments if a.variant == "control"]
    var = [a for a in assignments if a.variant == "variant"]

    if len(ctrl) < min_per_arm or len(var) < min_per_arm:
        return False

    ctrl_conv = sum(1 for a in ctrl if a.outcome == "converted")
    var_conv = sum(1 for a in var if a.outcome == "converted")
    n_ctrl, n_var = len(ctrl), len(var)

    p_ctrl = ctrl_conv / n_ctrl
    p_var = var_conv / n_var
    p_pool = (ctrl_conv + var_conv) / (n_ctrl + n_var)

    if p_pool == 0 or p_pool == 1:
        return False

    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n_ctrl + 1 / n_var))
    if se == 0:
        return False

    # One-sided: variant is the suspect — rollback only if variant < control by >2σ
    z = (p_var - p_ctrl) / se
    return z < -2.0


def rollback_rollout(test_name: str, db: Session) -> None:
    """Flip AbTest.status to 'rolled_back' (terminal). assign_rollout_arm will
    return None for this test going forward, routing all traffic to control."""
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test:
        return
    test.status = "rolled_back"
    test.ended_at = datetime.now(timezone.utc)
    db.flush()


def holdout_verdict(
    test_name: str,
    db: Session,
    *,
    window_hours: int = 168,
    min_per_arm: int = 30,
    conversion_window_days: Optional[int] = None,
) -> dict:
    """Positive-direction verdict for a control-holdout rollout test (ADR 0031/Task 4.1).

    Mirrors should_rollback_rollout's z-test shape but asks the opposite
    question: does 'variant' beat 'control' by >2σ? Never mutates the test —
    pure read, called by a scheduled surfacing job, not by any promotion path
    (promotion stays human-adopted via lifecycle_playbook, per fa036).

    conversion_window_days: when set, an assignment only counts toward the
    conversion numerator if outcome_at - created_at falls within this many
    days (e.g. retention_v1's "any paid action within 7 days" definition).
    The assignment still counts toward n_ctrl/n_var either way — only
    whether it counts as a WIN is time-boxed. None (default) preserves the
    original behavior: any recorded 'converted' outcome counts, unbounded —
    correct for sequences with a single, immediate conversion event
    (wallet activation, lead unlock, lock purchase) rather than a rolling
    "did anything happen after this" window.
    """
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name, AbTest.status == "active")
    ).scalar_one_or_none()
    if not test:
        return {"status": "no_test"}

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    assignments = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.created_at >= cutoff,
        )
    ).scalars().all()

    ctrl = [a for a in assignments if a.variant == "control"]
    var = [a for a in assignments if a.variant == "variant"]

    # For a time-windowed conversion definition, only assignments whose full
    # observation window has elapsed can be judged — a subscriber assigned
    # yesterday hasn't had 7 days to convert. Counting them dilutes the
    # denominator (and can dilute arms unequally by timing rather than copy),
    # so exclude immature assignments from BOTH numerator and denominator.
    if conversion_window_days is not None:
        mature_before = (
            datetime.now(timezone.utc) - timedelta(days=conversion_window_days)
        ).replace(tzinfo=None)  # created_at is a naive (UTC) column
        ctrl = [a for a in ctrl if a.created_at <= mature_before]
        var = [a for a in var if a.created_at <= mature_before]

    n_ctrl, n_var = len(ctrl), len(var)

    if n_ctrl < min_per_arm or n_var < min_per_arm:
        return {"status": "insufficient_data", "n_ctrl": n_ctrl, "n_var": n_var, "needed": min_per_arm}

    def _converted(a) -> bool:
        if a.outcome != "converted":
            return False
        if conversion_window_days is None:
            return True
        if a.outcome_at is None:
            return False
        # created_at is a naive DateTime column (legacy); outcome_at is
        # DateTime(timezone=True). Both represent UTC — strip tzinfo before
        # subtracting so naive/aware datetimes don't raise a TypeError.
        outcome_at = a.outcome_at.replace(tzinfo=None) if a.outcome_at.tzinfo else a.outcome_at
        return (outcome_at - a.created_at) <= timedelta(days=conversion_window_days)

    ctrl_conv = sum(1 for a in ctrl if _converted(a))
    var_conv = sum(1 for a in var if _converted(a))
    p_ctrl = ctrl_conv / n_ctrl
    p_var = var_conv / n_var
    p_pool = (ctrl_conv + var_conv) / (n_ctrl + n_var)

    base = {
        "n_ctrl": n_ctrl, "n_var": n_var,
        "control_rate_pct": round(p_ctrl * 100, 2),
        "variant_rate_pct": round(p_var * 100, 2),
    }

    if p_pool in (0.0, 1.0):
        return {**base, "status": "not_significant", "z_score": 0.0}

    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n_ctrl + 1 / n_var))
    if se == 0:
        return {**base, "status": "not_significant", "z_score": 0.0}

    z = (p_var - p_ctrl) / se
    status = "proven" if z > 2.0 else "not_significant"
    return {**base, "status": status, "z_score": round(z, 3)}


def record_outcome(subscriber_id: int, test_name: str, outcome: str, db: Session) -> None:
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test:
        return
    assignment = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.subscriber_id == subscriber_id,
        )
    ).scalar_one_or_none()
    if assignment:
        # Preserve the FIRST conversion and its timestamp. record_revenue
        # fires on every paid action, so without this a repeat purchaser's
        # outcome_at would advance past a valid early conversion and a
        # time-windowed verdict (conversion_window_days) would wrongly
        # exclude them. Once converted, the assignment is terminal here.
        if assignment.outcome == "converted":
            return
        assignment.outcome = outcome
        assignment.outcome_at = datetime.now(timezone.utc)
        db.flush()


def record_holdout_conversion(subscriber_id: int, test_name: str, db: Session) -> None:
    """Best-effort 'converted' outcome for a Task 4.1 control-holdout test.

    Wraps record_outcome with the swallow-and-log guard every conversion
    site needs, so the four call sites (wallet activation, lead unlock,
    checkout completion, any-paid-action) don't each re-implement the same
    try/except. No-op for any subscriber without an assignment on this test
    (record_outcome's own guard), so it fires safely regardless of whether
    the subscriber was ever placed in the holdout.
    """
    try:
        record_outcome(subscriber_id, test_name, "converted", db)
    except Exception:
        logger.warning(
            "holdout record_outcome failed test=%s sub=%s", test_name, subscriber_id,
            exc_info=True,
        )


def should_rollback(test_name: str, db: Session) -> bool:
    import math
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test or test.status != "active":
        return False

    guardrail = get_guardrail("message_variant_swap")
    retire_after = guardrail.get("retire_after_sends", 200)

    assignments = db.execute(
        select(AbAssignment).where(AbAssignment.test_id == test.id)
    ).scalars().all()

    if len(assignments) < retire_after:
        return False

    a_total = sum(1 for a in assignments if a.variant == "a")
    b_total = sum(1 for a in assignments if a.variant == "b")
    a_conv = sum(1 for a in assignments if a.variant == "a" and a.outcome == "converted")
    b_conv = sum(1 for a in assignments if a.variant == "b" and a.outcome == "converted")

    if a_total < 10 or b_total < 10:
        return False

    p_a = a_conv / a_total
    p_b = b_conv / b_total
    p_pool = (a_conv + b_conv) / (a_total + b_total)

    if p_pool == 0 or p_pool == 1:
        return False

    se = math.sqrt(p_pool * (1 - p_pool) * (1 / a_total + 1 / b_total))
    if se == 0:
        return False

    z = (p_a - p_b) / se
    return abs(z) > 2.0 and (p_a < p_b)  # variant A is losing by >2 std devs


def complete_test(
    test_name: str,
    winner: str,
    db: Session,
    *,
    source_actor: str = "lifecycle",
) -> None:
    """Close out an A/B test by recording the winner and writing a
    `lifecycle_playbook` recommendation row.

    The `source_actor` kwarg attributes who decided the test was over:
      - 'lifecycle' (default) — called automatically by `ab_rollback_check`
                           when the Z-test triggers. Drives Metric 5
                           "net new playbooks Lifecycle authored."
      - <operator handle> — called manually from an admin endpoint or
                            an operator script. Attributes the playbook
                            to the real human actor so Metric 5 doesn't
                            double-count human decisions as Lifecycle's.

    The playbook row writes through `playbook_writer.upsert_recommendation`,
    which dedupes by source_key (so re-running ab_rollback_check on a
    test that already has a recommendation silently skips).
    """
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test:
        return
    test.status = "completed"
    test.winner = winner
    test.ended_at = datetime.now(timezone.utc)
    db.flush()

    # fa036 — write a `lifecycle_playbook` recommendation row for the winning
    # variant. Status stays 'recommended' until a human adopts via the
    # admin endpoint (no auto-promote — pinned decision #1).
    from src.services.playbook_writer import upsert_recommendation
    upsert_recommendation(
        db,
        name=f"ab_winner:{test_name}",
        description=(
            f"A/B test {test_name} winner '{winner}' — recommend promoting "
            f"variant_{winner} to default"
        ),
        pattern={
            "test_name": test_name,
            "winner": winner,
            "variant": test.variant_b if winner == "b" else test.variant_a,
            "segment": test.segment,
        },
        source_type="ab_test",
        source_id=test_name,
        authored_by=source_actor,
    )
