"""
Agent Lane's own experiment engine — deterministic assignment, price-band
arm selection, for cold/pre-customer opportunities (Cora, REVINT, Hunter's
vertical autopilot, LEARN).

Deliberately not a thin wrapper around src/services/ab_engine.py. That
module is Lifecycle's (see AbTest's own docstring: "Lifecycle creates and
manages tests within guardrail bounds") — Agent Lane and Lifecycle are two
different engines (pre- vs post-customer outreach). This file duplicates
the small amount of well-understood math (deterministic hash + traffic
cap, in `_deterministic_variant`) rather than importing from ab_engine, so
neither engine's code or DB writes ever depend on the other's tables.

Originally this logic lived inline on AbTest/AbAssignment
(migrations/apply_price_assignment.py, apply_ab_assignment_thread_id.py).
It now targets AgentLaneExperiment/AgentLaneExperimentAssignment instead —
see migrations/apply_agent_lane_experiments.py and
migrations/apply_agent_lane_experiment_separation_cleanup.py.
"""

import hashlib
import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.agent_lane_guardrails import get_guardrail
from src.core.models import AgentLaneExperiment, AgentLaneExperimentAssignment

logger = logging.getLogger(__name__)


def get_or_create_experiment(
    test_name: str,
    variant_a: dict,
    variant_b: dict,
    traffic_pct: int,
    db: Session,
    *,
    hypothesis: Optional[str] = None,
    audience: Optional[str] = None,
    offer: Optional[str] = None,
    control_price_cents: Optional[int] = None,
    test_price_cents: Optional[int] = None,
    min_sample: Optional[int] = None,
    success_metric: Optional[str] = None,
) -> AgentLaneExperiment:
    cap = get_guardrail("agent_lane_experiment_traffic_cap")["max_pct"]
    capped_pct = min(traffic_pct, cap)

    existing = db.execute(
        select(AgentLaneExperiment).where(AgentLaneExperiment.test_name == test_name)
    ).scalar_one_or_none()
    if existing:
        if existing.traffic_pct != capped_pct:
            logger.info(
                "agent_lane_experiment_engine: syncing traffic_pct for %s: %s -> %s",
                test_name, existing.traffic_pct, capped_pct,
            )
            existing.traffic_pct = capped_pct
            db.flush()
        return existing

    experiment = AgentLaneExperiment(
        test_name=test_name,
        variant_a=variant_a,
        variant_b=variant_b,
        traffic_pct=capped_pct,
        status="active",
        hypothesis=hypothesis,
        audience=audience,
        offer=offer,
        control_price_cents=control_price_cents,
        test_price_cents=test_price_cents,
        min_sample=min_sample,
        success_metric=success_metric,
    )
    db.add(experiment)
    db.flush()
    return experiment


def _deterministic_variant(experiment: AgentLaneExperiment, key: str) -> Optional[str]:
    """Pure hash + traffic-cap decision — same key always maps to the same
    arm (or the same out-of-test None). Mirrors ab_engine.py's
    _deterministic_variant; duplicated rather than imported, see module
    docstring."""
    h = int(hashlib.md5(f"{experiment.test_name}{key}".encode()).hexdigest(), 16) % 100
    if h >= experiment.traffic_pct:
        return None
    return "a" if h % 2 == 0 else "b"


def assign_variant_by_thread(opportunity_thread_id: str, test_name: str, db: Session) -> Optional[str]:
    """Deterministically assign a cold opportunity thread to an arm.

    Idempotent: retries on the same (test, thread) return the same arm via
    the existing row rather than re-hashing."""
    experiment = db.execute(
        select(AgentLaneExperiment).where(
            AgentLaneExperiment.test_name == test_name,
            AgentLaneExperiment.status == "active",
        )
    ).scalar_one_or_none()
    if not experiment:
        return None

    existing = db.execute(
        select(AgentLaneExperimentAssignment).where(
            AgentLaneExperimentAssignment.test_id == experiment.id,
            AgentLaneExperimentAssignment.opportunity_thread_id == opportunity_thread_id,
        )
    ).scalar_one_or_none()
    if existing:
        return existing.variant

    variant = _deterministic_variant(experiment, opportunity_thread_id)
    if variant is None:
        return None

    assignment = AgentLaneExperimentAssignment(
        test_id=experiment.id,
        opportunity_thread_id=opportunity_thread_id,
        variant=variant,
    )
    db.add(assignment)
    db.flush()
    return variant


def get_price_variant(offer: str, experiment_id: int, opportunity_thread_id: str, db: Session) -> dict:
    """Return the price arm for a price-band test, for one opportunity thread.

    When PRICE_BAND_TESTING_ENABLED is False (the current default) the
    control arm price is always returned and no assignment is created —
    matching price_assignment.assign_price()'s own flag-off behavior. When
    the flag is True, opportunity_thread_id is deterministically assigned
    an arm via assign_variant_by_thread() (variant "a" -> control, "b" ->
    test, matching AgentLaneExperiment's variant_a/variant_b naming), and
    that arm's price is returned together with the real, persisted
    assignment id.

    Returns:
        {"arm": "control" | "test", "price_cents": int, "experiment_assignment_id": int | None}

    Raises ValueError when the experiment is missing required price columns
    or is not active.
    """
    from src.services.price_assignment import PRICE_BAND_TESTING_ENABLED  # avoid circular at module level

    experiment = db.execute(
        select(AgentLaneExperiment).where(
            AgentLaneExperiment.id == experiment_id,
            AgentLaneExperiment.status == "active",
        )
    ).scalar_one_or_none()
    if not experiment:
        raise ValueError(f"no active AgentLaneExperiment with id={experiment_id}")
    if experiment.offer and experiment.offer != offer:
        logger.warning(
            "get_price_variant: experiment %s offer mismatch (experiment.offer=%s, requested=%s)",
            experiment_id, experiment.offer, offer,
        )

    control_price = experiment.control_price_cents
    if control_price is None:
        raise ValueError(
            f"AgentLaneExperiment id={experiment_id} missing control_price_cents — "
            "populate before calling get_price_variant"
        )

    if not PRICE_BAND_TESTING_ENABLED:
        return {"arm": "control", "price_cents": control_price, "experiment_assignment_id": None}

    test_price = experiment.test_price_cents
    if test_price is None:
        raise ValueError(f"AgentLaneExperiment id={experiment_id} missing test_price_cents")

    arm_label = assign_variant_by_thread(opportunity_thread_id, experiment.test_name, db)
    if arm_label is None:
        return {"arm": "control", "price_cents": control_price, "experiment_assignment_id": None}

    assignment = db.execute(
        select(AgentLaneExperimentAssignment).where(
            AgentLaneExperimentAssignment.test_id == experiment.id,
            AgentLaneExperimentAssignment.opportunity_thread_id == opportunity_thread_id,
        )
    ).scalar_one_or_none()

    if arm_label == "b":
        return {
            "arm": "test",
            "price_cents": test_price,
            "experiment_assignment_id": assignment.id if assignment else None,
        }
    return {
        "arm": "control",
        "price_cents": control_price,
        "experiment_assignment_id": assignment.id if assignment else None,
    }


def record_outcome(opportunity_thread_id: str, test_name: str, outcome: str, db: Session) -> None:
    from datetime import datetime, timezone

    experiment = db.execute(
        select(AgentLaneExperiment).where(AgentLaneExperiment.test_name == test_name)
    ).scalar_one_or_none()
    if not experiment:
        return
    assignment = db.execute(
        select(AgentLaneExperimentAssignment).where(
            AgentLaneExperimentAssignment.test_id == experiment.id,
            AgentLaneExperimentAssignment.opportunity_thread_id == opportunity_thread_id,
        )
    ).scalar_one_or_none()
    if assignment and assignment.outcome != "converted":
        assignment.outcome = outcome
        assignment.outcome_at = datetime.now(timezone.utc)
        db.flush()
