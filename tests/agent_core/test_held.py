"""R1 0.4: the ``held`` status. A send that must not go ahead (breaker open, recipient edited, consent revoked) or whose
outcome is unknown is held for a person: nothing sent, nothing retried, and a reviewer reopens it for a fresh approval."""
from __future__ import annotations

from datetime import timedelta

from packages.agent_core.halt import HaltSwitch
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.relay import DispatchOutcome, Relay, SendHeld, SendHold, SendRetryLater

from .conftest import APPROVER, RecordingExecutor, enqueue_sms


def _approved(queue: PendingActionQueue, body: str = "Hi Sam, Josh here.") -> int:
    action_id = enqueue_sms(queue, body)
    queue.approve(action_id, APPROVER)
    return action_id


def test_a_send_check_hold_holds_the_action_and_never_calls_the_executor(queue, halt: HaltSwitch) -> None:
    executor = RecordingExecutor()
    relay = Relay(queue, halt, {"ghl_sms": executor}, send_check=lambda action: SendHold("recipient_changed"))
    action_id = _approved(queue)
    result = relay.dispatch(action_id)
    assert (result.outcome, result.reason) == (DispatchOutcome.HELD, "recipient_changed")
    action = queue.get(action_id)
    assert (action.status, action.held_reason) == (ActionStatus.HELD, "recipient_changed")
    assert executor.sent == []
    assert relay.run().sent == () and relay.run().held == ()  # held is not approved: the sweep never picks it up


def test_an_executor_that_cannot_tell_if_it_sent_holds_the_action_without_a_retry(queue, halt: HaltSwitch) -> None:
    calls: list[int] = []

    def uncertain(payload):
        calls.append(1)
        raise SendHeld("uncertain_outcome", "GhlSmsError: the send may or may not have gone out")

    relay = Relay(queue, halt, {"ghl_sms": uncertain})
    action_id = _approved(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.HELD
    assert relay.run().held == () and len(calls) == 1
    action = queue.get(action_id)
    assert action.status is ActionStatus.HELD and action.held_reason == "uncertain_outcome"
    assert "may or may not" in action.error


def test_a_rate_limited_send_goes_back_to_approved_for_the_next_sweep(queue, halt: HaltSwitch) -> None:
    attempts: list[int] = []

    def limited(payload):
        attempts.append(1)
        if len(attempts) == 1:
            raise SendRetryLater()
        return "msg-9"

    relay = Relay(queue, halt, {"ghl_sms": limited})
    action_id = _approved(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.DEFERRED
    assert queue.get(action_id).status is ActionStatus.APPROVED
    assert relay.run().sent == (action_id,)  # the next minute's sweep sends it, once
    assert len(attempts) == 2


def test_a_tripped_breaker_holds_every_approved_action_on_that_channel_only(queue) -> None:
    sms_one, sms_two = _approved(queue, "one"), _approved(queue, "two")
    pending = enqueue_sms(queue, "not approved yet")
    held = queue.hold_approved(("ghl_sms",), "breaker_open", "GHL SMS paused after 5 failures in a row")
    assert sorted(held) == sorted([sms_one, sms_two])
    assert queue.get(pending).status is ActionStatus.PENDING
    assert sorted(queue.held_ids(("ghl_sms",))) == sorted([sms_one, sms_two]) and queue.held_ids(("ghl_email",)) == []
    assert queue.hold_approved(("ghl_email",), "breaker_open") == []


def test_reopening_a_held_action_asks_for_a_fresh_approval_with_a_fresh_card(queue, clock) -> None:
    action_id = _approved(queue)
    queue.attach_card(action_id, "C_APPROVALS", "1700.1")
    queue.hold_approved(("ghl_sms",), "breaker_open")
    clock.advance(timedelta(hours=100))  # the old expiry has long passed
    assert queue.reopen_held(action_id) is True
    action = queue.get(action_id)
    assert action.status is ActionStatus.PENDING and action.held_reason is None
    assert (action.card_channel, action.card_ts, action.decided_by) == (None, None, None)
    assert action.expires_at > clock.now
    assert queue.awaiting_card() == [action_id]  # Cora's sweep posts a new card
    assert queue.reopen_held(action_id) is False  # only a held action can be reopened


def test_a_held_action_can_be_rejected_and_never_sends(queue, halt: HaltSwitch) -> None:
    executor = RecordingExecutor()
    relay = Relay(queue, halt, {"ghl_sms": executor})
    action_id = _approved(queue)
    queue.hold_approved(("ghl_sms",), "breaker_open")
    assert queue.reject(action_id, APPROVER) is True
    assert queue.get(action_id).status is ActionStatus.REJECTED
    assert relay.dispatch(action_id).outcome is DispatchOutcome.NOT_CLAIMED and executor.sent == []


def test_approving_a_held_action_directly_is_refused(queue) -> None:
    action_id = _approved(queue)
    queue.hold_approved(("ghl_sms",), "breaker_open")
    assert queue.approve(action_id, APPROVER) is False  # it must be reopened first, so every check runs again

def test_held_card_reports_uncertainty_and_retries_failed_updates(queue):
    from packages.agent_core.chatport import FakeChatPort, PostResult
    from packages.agent_core.slack_session import SlackSession
    action_id = _approved(queue)
    queue.attach_card(action_id, "C_APPROVALS", "1")
    queue.claim_for_send(action_id)
    queue.mark_held(action_id, "uncertain_outcome")
    class FlakyChat(FakeChatPort):
        def update(self, **kwargs):
            if not hasattr(self, "attempted"):
                self.attempted = True
                return PostResult(ok=False)
            return super().update(**kwargs)
    session = SlackSession.__new__(SlackSession)
    session._queue, session._chat, session._held_marked = queue, FlakyChat(), set()
    session.mark_held_cards()
    session.mark_held_cards()
    assert len(session._chat.updates) == 1
    assert "may have been sent" in session._chat.updates[0]["text"]


# ── PR #476 review fixes ───────────────────────────────────────────────────────

def test_an_old_card_click_cannot_approve_a_reopened_action(queue, halt: HaltSwitch) -> None:
    """Review 1: the reopened action gets a new card; a delayed click on the old one moves nothing."""
    from packages.agent_core.approval import ApprovalAction, apply_click

    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_APPROVALS", "1700.1")
    queue.approve(action_id, APPROVER, card_ts="1700.1")
    queue.hold_approved(("ghl_sms",), "breaker_open")
    queue.reopen_held(action_id)
    old = apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, {APPROVER}, card_ts="1700.1")
    assert old.accepted is False and queue.get(action_id).status is ActionStatus.PENDING  # before the new card
    queue.attach_card(action_id, "C_APPROVALS", "1800.2")
    assert apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, {APPROVER}, card_ts="1700.1").accepted is False
    assert apply_click(queue, ApprovalAction.REJECT, action_id, APPROVER, {APPROVER}, card_ts="1700.1").accepted is False
    new = apply_click(queue, ApprovalAction.APPROVE, action_id, APPROVER, {APPROVER}, card_ts="1800.2")
    assert new.accepted is True and queue.get(action_id).status is ActionStatus.APPROVED


def test_an_uncertain_send_says_it_may_have_gone_out_right_after_the_click_too() -> None:
    """Review 6: the immediate answer and the sweep's card text are the same, and warn against reopening."""
    from packages.agent_core.slack_session import held_text

    uncertain = held_text("uncertain_outcome", approved=True)
    assert "may have been sent" in uncertain and "Nothing was sent" not in uncertain
    assert "Do not reopen" in uncertain
    assert "Nothing was sent" in held_text("breaker_open", approved=True)


def test_the_card_sweep_reads_the_newest_holds_first(queue) -> None:
    """Review 7: a new hold is always within the limit, however many older ones wait."""
    ids = [_approved(queue, f"m{i}") for i in range(4)]
    queue.hold_approved(("ghl_sms",), "breaker_open")
    assert queue.held_ids(limit=2, newest_first=True) == [ids[3], ids[2]]
    assert queue.held_ids(limit=2) == [ids[0], ids[1]]
