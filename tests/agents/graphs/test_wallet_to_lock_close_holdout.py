"""
Regression guard for Task 4.1 lock_close_holdout reachability.

The bug this pins: wallet_to_lock_close originally rendered its prompt via
render_system_and_user() directly, bypassing the holdout gate that lives in
render_for_subscriber (reached through render_for_subscriber_auto). That made
lock_close_holdout inert — no subscriber was ever assigned an arm, so the
conversion hook and holdout_verdict could never see data.

The node's compose-context step MUST route through render_for_subscriber_auto
so the gate assigns + records the arm. render_for_subscriber_auto is mocked
here (it opens its own DB session, exactly why the sibling graph tests mock
it too) — the real gate/assignment behavior is covered against
render_for_subscriber directly in tests/test_holdout_loader.py.
"""

from unittest.mock import patch


def _min_state():
    return {
        "subscriber_id": 4242,
        "subscriber_profile": {"name": "Test User", "vertical": "roofing", "tier": "wallet"},
        "event_payload": {"zip_code": "33601", "credits_spent": 40},
        "segment_data": {},
        "revenue_signal_score": 55,
    }


def test_compose_context_routes_through_holdout_gate():
    from src.agents.graphs import wallet_to_lock_close as g

    with patch.object(
        g, "render_for_subscriber_auto",
        return_value=("sys", "user", None, None),
    ) as mock_auto, patch.object(
        g, "render_fallback_body", return_value="fallback",
    ):
        out = g._node_build_compose_context(_min_state())

    mock_auto.assert_called_once()
    args = mock_auto.call_args.args
    assert args[0] == g.GRAPH_NAME          # graph routed through the gate
    assert args[1] == 4242                  # this subscriber's arm gets assigned
    assert out["_system_prompt"] == "sys"
    assert out["_user_prompt"] == "user"


def test_compose_context_fails_open_to_base_prompt():
    """If the gate errors (e.g. DB down), the node must still produce a
    prompt from the base template rather than blocking the send."""
    from src.agents.graphs import wallet_to_lock_close as g

    with patch.object(
        g, "render_for_subscriber_auto", side_effect=RuntimeError("db down"),
    ), patch.object(
        g, "render_system_and_user", return_value=("base_sys", "base_user"),
    ), patch.object(
        g, "render_fallback_body", return_value="fallback",
    ):
        out = g._node_build_compose_context(_min_state())

    assert out["_system_prompt"] == "base_sys"
    assert out["_user_prompt"] == "base_user"
