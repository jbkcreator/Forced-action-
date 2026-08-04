"""
LEARN-v2.2 Layer 1, Step 3 — src/services/agent_lane_experiment_engine.py's
record_decision_snapshot(). See tests/test_revint.py::TestPriceVariant for
assign_variant_by_thread()/get_price_variant() coverage (Steps 1-2); this
file covers only the decision-context snapshot built on top of them.
"""

from __future__ import annotations

import pytest

from src.core.models import AgentLaneExperiment, ExperimentDecisionSnapshot
from src.services.agent_lane_experiment_engine import (
    assign_variant_by_thread,
    record_decision_snapshot,
)


def _make_experiment(db, test_name: str, traffic_pct: int = 100) -> AgentLaneExperiment:
    experiment = AgentLaneExperiment(
        test_name=test_name,
        variant_a={"angle": "urgency"},
        variant_b={"angle": "roi"},
        traffic_pct=traffic_pct,
        status="active",
    )
    db.add(experiment)
    db.flush()
    return experiment


class TestDecisionSnapshot:
    def test_no_snapshot_without_a_prior_assignment(self, fresh_db):
        """record_decision_snapshot() captures an assignment's context — it
        doesn't create one. No assignment yet -> None, not an empty row."""
        experiment = _make_experiment(fresh_db, "snapshot_no_assignment")
        result = record_decision_snapshot(
            "OPP-2026-00050", experiment.test_name, fresh_db,
            message_angle="roi", offer="core_subscription",
        )
        assert result is None

    def test_snapshot_captures_assignment_context(self, fresh_db):
        experiment = _make_experiment(fresh_db, "snapshot_full_context")
        thread_id = "OPP-2026-00051"
        variant = assign_variant_by_thread(thread_id, experiment.test_name, fresh_db)
        assert variant is not None  # traffic_pct=100 -> always in-test

        snapshot = record_decision_snapshot(
            thread_id, experiment.test_name, fresh_db,
            message_angle="roi_framing", offer="founder_tier",
            buyer_type=None,  # Hunter's classification not merged yet — degrade gracefully
            target_characteristics={"portfolio_size": 7},
            chosen_action="send_founder_tier_pitch",
            leading_alternative="offered subscription over pack",
        )
        assert snapshot is not None
        assert snapshot.test_id == experiment.id
        assert snapshot.opportunity_thread_id == thread_id
        assert snapshot.assigned_variant == variant
        assert snapshot.message_angle == "roi_framing"
        assert snapshot.offer == "founder_tier"
        assert snapshot.buyer_type is None
        assert snapshot.target_characteristics == {"portfolio_size": 7}
        assert snapshot.chosen_action == "send_founder_tier_pitch"
        assert snapshot.leading_alternative == "offered subscription over pack"

    def test_snapshot_is_idempotent_on_retry(self, fresh_db):
        """A second call for the same (test, thread) returns the existing,
        unchanged snapshot rather than overwriting it — it's meant to be
        immutable once taken."""
        experiment = _make_experiment(fresh_db, "snapshot_idempotent")
        thread_id = "OPP-2026-00052"
        assign_variant_by_thread(thread_id, experiment.test_name, fresh_db)

        first = record_decision_snapshot(
            thread_id, experiment.test_name, fresh_db, message_angle="urgency",
        )
        second = record_decision_snapshot(
            thread_id, experiment.test_name, fresh_db, message_angle="roi",
        )
        assert first.id == second.id
        assert second.message_angle == "urgency"  # unchanged from the first call

        from sqlalchemy import select
        rows = fresh_db.execute(
            select(ExperimentDecisionSnapshot).where(
                ExperimentDecisionSnapshot.test_id == experiment.id,
                ExperimentDecisionSnapshot.opportunity_thread_id == thread_id,
            )
        ).scalars().all()
        assert len(rows) == 1

    def test_no_snapshot_for_unknown_test_name(self, fresh_db):
        result = record_decision_snapshot("OPP-2026-00053", "no_such_experiment", fresh_db)
        assert result is None
