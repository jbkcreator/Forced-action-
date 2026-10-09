from __future__ import annotations

import pytest

from packages.agent_core.halt import AgentHalted, HaltSwitch
from packages.agent_core.pending_actions import ActionStatus, PendingAction, PendingActionQueue
from packages.agent_core.relay import DispatchOutcome, Relay

from .conftest import APPROVER, RecordingExecutor, enqueue_sms


def _approved(queue: PendingActionQueue, body: str = "Hi Sam, Josh here.") -> int:
    action_id = enqueue_sms(queue, body)
    queue.approve(action_id, APPROVER)
    return action_id


def test_sends_the_frozen_payload_once(queue: PendingActionQueue, relay: Relay, executor: RecordingExecutor) -> None:
    action_id = _approved(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.SENT
    assert relay.dispatch(action_id).outcome is DispatchOutcome.NOT_CLAIMED
    assert executor.sent == [{"contact_id": "c-42", "body": "Hi Sam, Josh here."}]
    assert queue.get(action_id).status is ActionStatus.SENT


def test_unapproved_action_never_reaches_the_executor(queue: PendingActionQueue, relay: Relay,
                                                      executor: RecordingExecutor) -> None:
    action_id = enqueue_sms(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.NOT_CLAIMED
    assert executor.sent == []
    assert queue.get(action_id).status is ActionStatus.PENDING


def test_halt_blocks_before_claiming(queue: PendingActionQueue, halt: HaltSwitch, relay: Relay,
                                     executor: RecordingExecutor) -> None:
    action_id = _approved(queue)
    halt.set("stop", APPROVER)
    with pytest.raises(AgentHalted):
        relay.dispatch(action_id)
    assert executor.sent == []
    assert queue.get(action_id).status is ActionStatus.APPROVED


def test_executor_error_marks_failed_without_retry(queue: PendingActionQueue, halt: HaltSwitch) -> None:
    failing = RecordingExecutor(error=ConnectionError("GHL timed out"))
    relay = Relay(queue, halt, {"ghl_sms": failing})
    action_id = _approved(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.FAILED
    action = queue.get(action_id)
    assert action.status is ActionStatus.FAILED
    assert action.error.startswith("ConnectionError")
    assert relay.run().sent == ()


def test_unknown_channel_is_failed_not_sent(queue: PendingActionQueue, halt: HaltSwitch,
                                            executor: RecordingExecutor) -> None:
    relay = Relay(queue, halt, {"ghl_email": executor})
    action_id = _approved(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.FAILED
    assert executor.sent == []


def test_send_check_sees_the_action_and_can_block(queue: PendingActionQueue, halt: HaltSwitch,
                                                  executor: RecordingExecutor) -> None:
    seen: list[PendingAction] = []

    def opted_out(action: PendingAction) -> str:
        seen.append(action)
        return "recipient is suppressed or marked do-not-contact"

    relay = Relay(queue, halt, {"ghl_sms": executor}, send_check=opted_out)
    action_id = _approved(queue)
    result = relay.dispatch(action_id)
    assert result.outcome is DispatchOutcome.BLOCKED
    assert result.reason == "recipient is suppressed or marked do-not-contact"
    assert executor.sent == []
    assert seen[0].recipient_phone == "+17275550100"
    action = queue.get(action_id)
    assert action.status is ActionStatus.BLOCKED
    assert relay.run().sent == ()


def test_erroring_send_check_blocks(queue: PendingActionQueue, halt: HaltSwitch, executor: RecordingExecutor) -> None:
    def broken(action: PendingAction) -> str:
        raise TimeoutError("db down")

    relay = Relay(queue, halt, {"ghl_sms": executor}, send_check=broken)
    action_id = _approved(queue)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.BLOCKED
    assert executor.sent == []
    assert "TimeoutError" in queue.get(action_id).error


def test_passing_send_check_sends(queue: PendingActionQueue, halt: HaltSwitch, executor: RecordingExecutor) -> None:
    relay = Relay(queue, halt, {"ghl_sms": executor}, send_check=lambda action: None)
    assert relay.dispatch(_approved(queue)).outcome is DispatchOutcome.SENT
    assert len(executor.sent) == 1


def test_run_sweeps_every_approved_action(queue: PendingActionQueue, relay: Relay, executor: RecordingExecutor) -> None:
    first, second = _approved(queue, "one"), _approved(queue, "two")
    enqueue_sms(queue, "still pending")
    result = relay.run()
    assert set(result.sent) == {first, second}
    assert [payload["body"] for payload in executor.sent] == ["one", "two"]
