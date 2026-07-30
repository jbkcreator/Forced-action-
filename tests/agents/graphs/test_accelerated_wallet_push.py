"""Integration tests for the Accelerated Wallet Push graph (fa016).

Claude / compliance / send_sms are mocked at the compose_and_send module
boundary. Read tools (`get_subscriber_profile`, `get_wallet_state`) are
mocked at the graph module boundary. The finalize node's DB insert is also
patched so tests are pure unit tests of the graph wiring.
"""

import uuid
from unittest.mock import MagicMock, call, patch

import pytest

from src.agents.graphs.accelerated_wallet_push import run_accelerated_wallet_push


FAKE_CLAUDE = {
    "text": "Amal, 3 leads in 33647 you couldn't grab. Reply WALLET. http://x",
    "model": "haiku",
    "input_tokens": 90,
    "output_tokens": 28,
    "cost_usd": 0.00008,
}


PROFILE = {
    "id": 11,
    "tier": "starter",
    "status": "active",
    "vertical": "roofing",
    "name": "Amal Antony",
    "phone": "+15551234567",
    "has_saved_card": True,
    "wallet_opt_out": False,
    "missed_lead_count": 3,
}


def _payload(**overrides):
    p = {
        "tier": "starter_wallet",
        "credits_in_offer": 20,
        "price_cents": 4900,
        "missed_leads": 3,
        "zip_code": "33647",
        "reason": "saved_card_paid_intent",
        "cta_url": "https://app.forcedactionleads.com/dashboard/u?wallet_offer=accept",
    }
    p.update(overrides)
    return p


def _happy_mocks(variant="a"):
    return [
        patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
              return_value=dict(PROFILE)),
        patch("src.agents.graphs.accelerated_wallet_push.get_wallet_state",
              return_value={"enrolled": False, "tier": None, "credits_remaining": 0}),
        patch("src.agents.graphs.accelerated_wallet_push.run_decision_hierarchy",
              return_value={
                  "action_allowed": True,
                  "use_fallback": False,
                  "kill_switch_color": "green",
                  "revenue_signal_score": 55,
                  "ab_variant": variant,
              }),
        patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
              return_value=FAKE_CLAUDE),
        patch("src.agents.subgraphs.compose_and_send.compliance_check",
              return_value={"can_send": True, "reason": "ok"}),
        patch("src.agents.subgraphs.compose_and_send.send_sms",
              return_value={"sent": True, "reason": "ok", "message_outcome_id": 5}),
        patch("src.agents.subgraphs.compose_and_send.log_decision"),
        # Finalize node writes a WalletPushOffer row; stub the Database() ctxmgr.
        patch("src.agents.graphs.accelerated_wallet_push.render_for_subscriber_auto",
              return_value=("sys", "user", variant, "accelerated_wallet_push_framing")),
    ]


def test_happy_path_sends_missing_leads_variant():
    patches = _happy_mocks()
    for p in patches:
        p.start()
    try:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    finally:
        for p in patches:
            p.stop()
    assert r["terminal_status"] == "completed"
    assert r["sent"] is True
    assert r["message_body"]
    assert r["framing_variant"] == "missing_leads"


def test_credits_ready_variant_when_no_missed_leads():
    patches = _happy_mocks()
    for p in patches:
        p.start()
    try:
        r = run_accelerated_wallet_push(
            event_payload=_payload(missed_leads=0), subscriber_id=11,
        )
    finally:
        for p in patches:
            p.stop()
    assert r["terminal_status"] == "completed"
    assert r["framing_variant"] == "credits_ready"


def test_subscriber_not_found_aborts():
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value={}), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=999)
    assert r["terminal_status"] == "aborted"
    assert "subscriber_not_found" in r["failure_reason"]
    mock_cc.assert_not_called()


def test_wallet_opt_out_aborts_before_compose():
    opt_out_profile = dict(PROFILE)
    opt_out_profile["wallet_opt_out"] = True
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value=opt_out_profile), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    assert r["terminal_status"] == "aborted"
    assert "wallet_opt_out" in r["failure_reason"]
    mock_cc.assert_not_called()


def test_already_enrolled_aborts():
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value=dict(PROFILE)), \
         patch("src.agents.graphs.accelerated_wallet_push.get_wallet_state",
               return_value={"enrolled": True, "tier": "starter_wallet", "credits_remaining": 5}), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    assert r["terminal_status"] == "aborted"
    assert "already_enrolled" in r["failure_reason"]
    mock_cc.assert_not_called()


def test_no_saved_card_aborts():
    no_card_profile = dict(PROFILE)
    no_card_profile["has_saved_card"] = False
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value=no_card_profile), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    assert r["terminal_status"] == "aborted"
    assert "no_saved_card" in r["failure_reason"]
    mock_cc.assert_not_called()


def test_compliance_block_aborts_without_send():
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value=dict(PROFILE)), \
         patch("src.agents.graphs.accelerated_wallet_push.get_wallet_state",
               return_value={"enrolled": False}), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
               return_value=FAKE_CLAUDE), \
         patch("src.agents.subgraphs.compose_and_send.compliance_check",
               return_value={"can_send": False, "reason": "opted_out"}), \
         patch("src.agents.subgraphs.compose_and_send.send_sms") as mock_send, \
         patch("src.agents.subgraphs.compose_and_send.log_decision"):
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    assert r["terminal_status"] == "aborted"
    mock_send.assert_not_called()


# ─── A/B attribution tests ────────────────────────────────────────────────────


def test_variant_id_threaded_to_send_sms_variant_a():
    """send_sms must receive the formatted variant_id string, not the raw variant letter."""
    mock_send = MagicMock(return_value={"sent": True, "reason": "ok", "message_outcome_id": 7})
    patches = _happy_mocks(variant="a")
    # Replace the send_sms patch with our spy.
    patches[5] = patch("src.agents.subgraphs.compose_and_send.send_sms", mock_send)
    for p in patches:
        p.start()
    try:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    finally:
        for p in patches:
            p.stop()

    assert r["terminal_status"] == "completed"
    assert r["sent"] is True
    send_kwargs = mock_send.call_args
    assert send_kwargs.kwargs.get("variant_id") == "accelerated_wallet_push_framing:a"


def test_variant_id_threaded_to_send_sms_variant_b():
    """Variant B also carries the correctly formatted variant_id."""
    mock_send = MagicMock(return_value={"sent": True, "reason": "ok", "message_outcome_id": 8})
    patches = _happy_mocks(variant="b")
    patches[5] = patch("src.agents.subgraphs.compose_and_send.send_sms", mock_send)
    for p in patches:
        p.start()
    try:
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)
    finally:
        for p in patches:
            p.stop()

    assert r["terminal_status"] == "completed"
    send_kwargs = mock_send.call_args
    assert send_kwargs.kwargs.get("variant_id") == "accelerated_wallet_push_framing:b"


def test_hierarchy_ab_variant_used_as_fallback_when_render_returns_none():
    """
    When render_for_subscriber_auto returns variant=None (e.g. traffic cap),
    the graph falls back to the ab_variant already assigned by decision_hierarchy.
    send_sms must still receive a non-null variant_id.
    """
    mock_send = MagicMock(return_value={"sent": True, "reason": "ok", "message_outcome_id": 9})
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value=dict(PROFILE)), \
         patch("src.agents.graphs.accelerated_wallet_push.get_wallet_state",
               return_value={"enrolled": False}), \
         patch("src.agents.graphs.accelerated_wallet_push.run_decision_hierarchy",
               return_value={
                   "action_allowed": True,
                   "use_fallback": False,
                   "kill_switch_color": "green",
                   "revenue_signal_score": 55,
                   "ab_variant": "a",   # hierarchy assigned 'a'
               }), \
         patch("src.agents.graphs.accelerated_wallet_push.render_for_subscriber_auto",
               return_value=("sys", "user", None, "accelerated_wallet_push_framing")), \
         patch("src.agents.graphs.accelerated_wallet_push.render_variant",
               return_value=("sys_a", "user_a")), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
               return_value=FAKE_CLAUDE), \
         patch("src.agents.subgraphs.compose_and_send.compliance_check",
               return_value={"can_send": True, "reason": "ok"}), \
         patch("src.agents.subgraphs.compose_and_send.send_sms", mock_send), \
         patch("src.agents.subgraphs.compose_and_send.log_decision"):
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)

    assert r["terminal_status"] == "completed"
    send_kwargs = mock_send.call_args
    # Must use hierarchy's variant, not null.
    assert send_kwargs.kwargs.get("variant_id") == "accelerated_wallet_push_framing:a"


def test_missing_leads_framing_with_variant_a_end_to_end():
    """Full happy-path: missing_leads framing + variant_a → send_sms called with correct attribution."""
    mock_send = MagicMock(return_value={"sent": True, "reason": "ok", "message_outcome_id": 10})
    patches = _happy_mocks(variant="a")
    patches[5] = patch("src.agents.subgraphs.compose_and_send.send_sms", mock_send)
    for p in patches:
        p.start()
    try:
        r = run_accelerated_wallet_push(
            event_payload=_payload(missed_leads=3), subscriber_id=11,
        )
    finally:
        for p in patches:
            p.stop()

    assert r["terminal_status"] == "completed"
    assert r["framing_variant"] == "missing_leads"
    assert mock_send.call_args.kwargs.get("variant_id") == "accelerated_wallet_push_framing:a"


def test_credits_ready_framing_with_variant_b_end_to_end():
    """credits_ready framing + variant_b → send_sms called with correct attribution."""
    mock_send = MagicMock(return_value={"sent": True, "reason": "ok", "message_outcome_id": 11})
    patches = _happy_mocks(variant="b")
    patches[5] = patch("src.agents.subgraphs.compose_and_send.send_sms", mock_send)
    for p in patches:
        p.start()
    try:
        r = run_accelerated_wallet_push(
            event_payload=_payload(missed_leads=0), subscriber_id=11,
        )
    finally:
        for p in patches:
            p.stop()

    assert r["terminal_status"] == "completed"
    assert r["framing_variant"] == "credits_ready"
    assert mock_send.call_args.kwargs.get("variant_id") == "accelerated_wallet_push_framing:b"


def test_no_variant_when_hierarchy_and_render_both_return_none():
    """When both hierarchy and render return no variant, variant_id is None (not a crash)."""
    mock_send = MagicMock(return_value={"sent": True, "reason": "ok", "message_outcome_id": 12})
    with patch("src.agents.graphs.accelerated_wallet_push.get_subscriber_profile",
               return_value=dict(PROFILE)), \
         patch("src.agents.graphs.accelerated_wallet_push.get_wallet_state",
               return_value={"enrolled": False}), \
         patch("src.agents.graphs.accelerated_wallet_push.run_decision_hierarchy",
               return_value={
                   "action_allowed": True,
                   "use_fallback": False,
                   "kill_switch_color": "green",
                   "revenue_signal_score": 55,
                   "ab_variant": None,   # no variant from hierarchy
               }), \
         patch("src.agents.graphs.accelerated_wallet_push.render_for_subscriber_auto",
               return_value=("sys", "user", None, None)), \
         patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
               return_value=FAKE_CLAUDE), \
         patch("src.agents.subgraphs.compose_and_send.compliance_check",
               return_value={"can_send": True, "reason": "ok"}), \
         patch("src.agents.subgraphs.compose_and_send.send_sms", mock_send), \
         patch("src.agents.subgraphs.compose_and_send.log_decision"):
        r = run_accelerated_wallet_push(event_payload=_payload(), subscriber_id=11)

    assert r["terminal_status"] == "completed"
    # No variant assigned — variant_id should be None (graceful, not an error).
    assert mock_send.call_args.kwargs.get("variant_id") is None
