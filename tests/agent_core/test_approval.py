from __future__ import annotations

from datetime import timedelta

import pytest

from packages.agent_core.approval import (
    ApprovalAction,
    apply_click,
    render_action_card,
    render_decided_card,
)
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.redaction import mask_emails, mask_long_digit_runs

from .conftest import APPROVER, OPERATOR, Clock, enqueue_sms

APPROVERS = frozenset({APPROVER})


def test_card_has_exactly_three_buttons_carrying_the_action_id(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    blocks = render_action_card(queue.get(action_id))
    buttons = next(block for block in blocks if block["type"] == "actions")["elements"]
    assert [button["action_id"] for button in buttons] == [action.value for action in ApprovalAction]
    assert [button["text"]["text"] for button in buttons] == ["Approve", "Revise", "Reject"]
    assert {button["value"] for button in buttons} == {str(action_id)}


def test_card_masks_long_digit_runs_and_escapes_markup(queue: PendingActionQueue) -> None:
    action_id = queue.enqueue(tool_name="send_sms", channel="ghl_sms",
                              payload={"to": "7274369951", "body": "<!channel> call me"},
                              summary="Text to 727-436-9951")
    rendered = str(render_action_card(queue.get(action_id)))
    assert "7274369951" not in rendered and "727-436-9951" not in rendered
    assert "[…9951]" in rendered
    assert "<!channel>" not in rendered and "&lt;!channel&gt;" in rendered


def test_mask_leaves_short_numbers_alone() -> None:
    assert mask_long_digit_runs("Loan $350000 at 12 months") == "Loan $350000 at 12 months"


def test_email_card_shows_the_address_masked(queue: PendingActionQueue) -> None:
    action_id = queue.enqueue(tool_name="draft_email", channel="ghl_email",
                              payload={"to_email": "sam.smith@example.com", "subject": "Hi", "body": "Hello"},
                              summary="Email to Sam <sam.smith@example.com>: Hi")
    rendered = str(render_action_card(queue.get(action_id)))
    assert "sam.smith@example.com" not in rendered
    assert "s***@example.com" in rendered


def test_card_shows_the_draft_as_plain_text_not_json(queue: PendingActionQueue) -> None:
    action_id = queue.enqueue(tool_name="after_hours_reply", channel="ghl_sms",
                              payload={"body": "Hi TEST, we missed your call.\nWe will call back.",
                                       "lead_ref": "contact:18008", "to_phone": "+19177303812"},
                              summary="After-hours reply")
    preview = render_action_card(queue.get(action_id))[2]["text"]["text"]
    assert "```" not in preview and "{" not in preview and '"body"' not in preview
    assert "*Lead:* contact:18008" in preview
    assert "*To:* +[…3812]" in preview
    assert preview.endswith("> Hi TEST, we missed your call.\n> We will call back.")


def test_email_mask_keeps_first_letter_and_domain_only() -> None:
    assert mask_emails("write to Sam.Lee+loans@mail.example.co.uk today") == "write to S***@mail.example.co.uk today"
    assert mask_emails("no address here, just @mentions") == "no address here, just @mentions"


def test_decided_card_has_no_buttons(queue: PendingActionQueue) -> None:
    blocks = render_decided_card(queue.get(enqueue_sms(queue)), "Rejected")
    assert all(block["type"] != "actions" for block in blocks)
    assert any(block.get("text", {}).get("text") == "Rejected" for block in blocks)


def test_non_approver_click_changes_nothing(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    outcome = apply_click(queue, ApprovalAction.APPROVE, action_id, OPERATOR, APPROVERS)
    assert not outcome.accepted and not outcome.dispatch
    assert queue.get(action_id).status is ActionStatus.PENDING


def test_empty_approver_set_fails_closed(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    outcome = apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, frozenset())
    assert not outcome.accepted
    assert queue.get(action_id).status is ActionStatus.PENDING


def test_approve_dispatches_once(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    first = apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, APPROVERS)
    second = apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, APPROVERS)
    assert first.accepted and first.dispatch
    assert not second.accepted and not second.dispatch


def test_expired_draft_says_so_instead_of_already_decided(queue: PendingActionQueue, clock: Clock) -> None:
    action_id = enqueue_sms(queue, ttl=timedelta(hours=1))
    clock.advance(timedelta(hours=2))
    outcome = apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, APPROVERS)
    assert not outcome.accepted and not outcome.dispatch
    assert "expired" in outcome.status_text


def test_reject_and_revise(queue: PendingActionQueue) -> None:
    rejected, revised = enqueue_sms(queue), enqueue_sms(queue)
    assert apply_click(queue, ApprovalAction.REJECT, rejected, APPROVER, APPROVERS).accepted
    assert queue.get(rejected).status is ActionStatus.REJECTED
    outcome = apply_click(queue, ApprovalAction.REVISE, revised, APPROVER, APPROVERS)
    assert outcome.accepted and not outcome.dispatch
    assert queue.get(revised).status is ActionStatus.REVISING


TOKEN = "Zk3n8QvR2xYp7LmA9cTb4WsE6uHf1JdG0oKiNqVrXyU"  # made up, 43 URL-safe characters like a real upload token


def test_card_masks_an_upload_link_token_but_sends_it_unchanged(queue: PendingActionQueue) -> None:
    link = f"https://api.example.com/api/lending/docs/{TOKEN}"
    payload = {"to_phone": "+18135550101", "body": f"Hi Mark, upload here: {link} Reply STOP to opt out.",
               "upload_link": link}
    action_id = queue.enqueue(tool_name="doc_chase", channel="ghl_sms", payload=payload, summary="Docs request")
    action = queue.get(action_id)
    for blocks in (render_action_card(action), render_decided_card(action, "Approved")):
        rendered = str(blocks)
        assert TOKEN not in rendered and TOKEN[-8:] not in rendered
        assert "https://api.example.com/api/lending/docs/[…]" in rendered
    assert action.payload["body"] == payload["body"] and action.payload["upload_link"] == link


@pytest.mark.parametrize("text", [
    "https://drive.google.com/drive/folders/1hQroR2KdfqzEsjVOgkLtaNvDlpc8GIJB",  # 33 chars, but shown elsewhere
    "https://calendly.com/next-deal-lending/intro-call",
    "see https://example.com/a/b and call",
])
def test_ordinary_links_outside_cards_are_not_touched_by_slack_safe(text) -> None:
    from packages.agent_core.redaction import slack_safe
    assert slack_safe(text) == text


def test_url_token_mask_only_hits_long_random_segments() -> None:
    from packages.agent_core.redaction import mask_url_tokens
    assert mask_url_tokens(f"go: https://x.test/d/{TOKEN}.") == "go: https://x.test/d/[…]."
    assert mask_url_tokens("https://calendly.com/next-deal-lending/intro-call") == \
        "https://calendly.com/next-deal-lending/intro-call"
    assert mask_url_tokens(f"bare {TOKEN} not a link") == f"bare {TOKEN} not a link"
