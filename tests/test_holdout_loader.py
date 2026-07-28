"""Tests for the frozen-control holdout gate folded into
src/agents/prompts/loader.py:render_for_subscriber (Task 4.1 — reuses
ab_engine.assign_rollout_arm; see the Task 4.1 grill for the design).

The gate lives INSIDE render_for_subscriber (not a separate function) so
render_for_subscriber_auto inherits it for free and no graph call site needs
to change — graph unit tests patch `render_for_subscriber_auto` by name at
the graph-module level, so introducing a differently-named wrapper would
require editing every graph's import and risk silently breaking those mocks.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import select


class TestGetHoldoutConfig:
    def test_returns_config_for_enabled_graph(self):
        from src.agents.prompts.loader import get_holdout_config

        cfg = get_holdout_config("accelerated_wallet_push")
        assert cfg is not None
        assert cfg["graph"] == "accelerated_wallet_push"
        assert cfg["control_pct"] == 10

    def test_returns_none_for_graph_without_holdout(self):
        from src.agents.prompts.loader import get_holdout_config

        assert get_holdout_config("nonexistent_graph_xyz") is None


class TestHoldoutRequiresActiveTreatment:
    """PR #133 finding 1: a holdout must NOT assign an arm when its graph has
    no active a/b treatment — the variant arm would render the same base
    prompt as control, making the verdict a baseline-vs-baseline comparison
    that noise can false-promote."""

    def test_no_arm_assigned_when_treatment_absent(self):
        from src.agents.prompts import loader
        from unittest.mock import MagicMock

        db = MagicMock()
        with patch.object(loader, "get_holdout_config",
                          return_value={"test_name": "x_holdout", "graph": "x", "control_pct": 10}), \
             patch.object(loader, "get_traffic_config", return_value=None), \
             patch.object(loader, "render_system_and_user", return_value=("s", "u")), \
             patch("src.services.ab_engine.assign_rollout_arm") as mock_assign, \
             patch("src.services.ab_engine.get_or_create_holdout_test") as mock_create:
            loader.render_for_subscriber("x", 1, {}, db)

        mock_assign.assert_not_called()
        mock_create.assert_not_called()

    def test_arm_assigned_when_treatment_present(self):
        from src.agents.prompts import loader
        from unittest.mock import MagicMock

        db = MagicMock()
        with patch.object(loader, "get_holdout_config",
                          return_value={"test_name": "x_holdout", "graph": "x", "control_pct": 10}), \
             patch.object(loader, "get_traffic_config",
                          return_value={"test_name": "x_ab", "graph": "x", "traffic_pct": 100,
                                        "variant_a": {}, "variant_b": {}}), \
             patch.object(loader, "render_system_and_user", return_value=("s", "u")), \
             patch.object(loader, "render_variant", return_value=("sv", "uv")), \
             patch.object(loader, "base_prompt_fingerprint", return_value="fp"), \
             patch("src.services.ab_engine.get_or_create_holdout_test"), \
             patch("src.services.ab_engine.get_or_create_test"), \
             patch("src.services.ab_engine.assign_rollout_arm", return_value="control") as mock_assign, \
             patch("src.services.ab_engine.assign_variant", return_value=None):
            sys_txt, usr_txt, variant, test_name = loader.render_for_subscriber("x", 1, {}, db)

        mock_assign.assert_called_once()
        # Control arm → frozen base prompt, no a/b variant surfaced.
        assert (sys_txt, usr_txt, variant, test_name) == ("s", "u", None, None)


def _make_subscriber(fresh_db):
    from src.core.models import Subscriber
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_hl_{uid}", tier="starter", vertical="roofing",
        county_id="hillsborough", event_feed_uuid=f"hl-uuid-{uid}",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


class TestRenderForSubscriberHoldoutGate:
    def test_control_arm_gets_base_prompt_and_is_recorded(self, fresh_db):
        from src.agents.prompts.loader import render_for_subscriber, render_system_and_user
        from src.services.ab_engine import get_or_create_test
        from src.core.models import AbAssignment

        sub = _make_subscriber(fresh_db)
        context = {"first_name": "Test", "zip_code": "33601"}

        # Force this subscriber into the control arm regardless of hash by
        # pre-assigning them before the loader call — assign_rollout_arm is
        # idempotent per (test, subscriber), so the loader's own call will
        # see this existing row rather than re-rolling.
        test = get_or_create_test(
            test_name="wallet_push_holdout", segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, db=fresh_db,
        )
        fresh_db.add(AbAssignment(test_id=test.id, subscriber_id=sub.id, variant="control"))
        fresh_db.flush()

        sys_txt, usr_txt, ab_variant, ab_test_name = render_for_subscriber(
            "accelerated_wallet_push", sub.id, context, fresh_db,
        )

        expected_sys, expected_usr = render_system_and_user("accelerated_wallet_push", context)
        assert sys_txt == expected_sys
        assert usr_txt == expected_usr
        assert ab_variant is None
        assert ab_test_name is None

        recorded = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == test.id, AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert recorded is not None
        assert recorded.variant == "control"

    def test_variant_arm_falls_through_to_ab_assignment(self, fresh_db, monkeypatch):
        """A subscriber in the holdout's 'variant' arm must still reach the
        graph's normal a/b assignment — the control short-circuit must not
        fire for them. ab_engine.assign_variant is monkeypatched to a fixed
        value because the pre-existing ab_test_traffic_cap guardrail caps
        that a/b test at 10% regardless of YAML — mocking is the only way to
        observe deterministically that the fallthrough path executed, since
        an unmocked call could coincidentally land outside the cap and
        produce output indistinguishable from the control short-circuit."""
        from src.agents.prompts import loader
        from src.services.ab_engine import get_or_create_test
        from src.core.models import AbAssignment

        sub = _make_subscriber(fresh_db)
        context = {"first_name": "Test", "zip_code": "33601"}

        holdout_test = get_or_create_test(
            test_name="wallet_push_holdout", segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, db=fresh_db,
        )
        fresh_db.add(AbAssignment(test_id=holdout_test.id, subscriber_id=sub.id, variant="variant"))
        fresh_db.flush()

        monkeypatch.setattr(
            "src.services.ab_engine.assign_variant",
            lambda sub_id, test_name, db: "a",
        )

        sys_txt, usr_txt, ab_variant, ab_test_name = loader.render_for_subscriber(
            "accelerated_wallet_push", sub.id, context, fresh_db,
        )

        assert ab_variant == "a"
        assert ab_test_name == "accelerated_wallet_push_framing"
        expected_sys, expected_usr = loader.render_variant("accelerated_wallet_push", "a", context)
        assert (sys_txt, usr_txt) == (expected_sys, expected_usr)

    def test_no_holdout_configured_is_pure_passthrough(self, fresh_db):
        """A graph with no entry in lifecycle_holdout_tests.yaml (e.g.
        'nws_urgency') must behave identically to the pre-holdout
        render_for_subscriber: nws_urgency also has no entry in
        lifecycle_ab_tests.yaml, so the expected output is deterministic —
        base prompt, no variant."""
        from src.agents.prompts.loader import (
            render_for_subscriber,
            render_system_and_user,
            get_holdout_config,
        )

        assert get_holdout_config("nws_urgency") is None  # sanity: no holdout entry

        sub = _make_subscriber(fresh_db)
        context = {"first_name": "Test", "zip_code": "33601"}

        result = render_for_subscriber("nws_urgency", sub.id, context, fresh_db)
        expected_sys, expected_usr = render_system_and_user("nws_urgency", context)

        assert result == (expected_sys, expected_usr, None, None)
