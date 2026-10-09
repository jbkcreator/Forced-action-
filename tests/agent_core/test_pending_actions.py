from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from packages.agent_core.pending_actions import DEFAULT_TTL, ActionStatus, PendingActionQueue, is_expired

from .conftest import APPROVER, OPERATOR, Clock, enqueue_sms


def test_enqueue_freezes_payload_and_recipient(queue: PendingActionQueue, clock: Clock) -> None:
    action_id = enqueue_sms(queue, recipient_email="  Sam@Example.COM ", contact_ref="contact-7")
    action = queue.get(action_id)
    assert action.status is ActionStatus.PENDING
    assert action.payload == {"contact_id": "c-42", "body": "Hi Sam, Josh here."}
    assert action.recipient_phone == "+17275550100"
    assert action.recipient_email == "sam@example.com"
    assert action.contact_ref == "contact-7"
    assert action.revisions == ()
    assert action.expires_at == clock.now + DEFAULT_TTL


def test_non_json_payload_is_rejected(queue: PendingActionQueue) -> None:
    with pytest.raises(TypeError):
        queue.enqueue(tool_name="send_sms", channel="ghl_sms", payload={"at": datetime.now()}, summary="x")


def test_repeated_idempotency_key_returns_the_same_action(queue: PendingActionQueue) -> None:
    first = enqueue_sms(queue, idempotency_key="toolu_01")
    second = enqueue_sms(queue, "a retried draft", idempotency_key="toolu_01")
    third = enqueue_sms(queue, idempotency_key="toolu_02")
    assert first == second != third
    assert queue.get(first).payload["body"] == "Hi Sam, Josh here."


def test_drafts_without_a_key_never_collide(queue: PendingActionQueue) -> None:
    assert enqueue_sms(queue) != enqueue_sms(queue)


def test_approve_moves_a_row_once(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    assert queue.approve(action_id, APPROVER)
    assert not queue.approve(action_id, APPROVER)
    action = queue.get(action_id)
    assert action.status is ActionStatus.APPROVED
    assert action.decided_by == APPROVER


def test_rejected_row_cannot_be_approved(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    assert queue.reject(action_id, APPROVER)
    assert not queue.approve(action_id, APPROVER)


def test_revision_keeps_history_and_needs_fresh_approval(queue: PendingActionQueue, clock: Clock) -> None:
    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_CORA", "1.0")
    assert queue.request_revision(action_id, OPERATOR)
    assert queue.revising_for(OPERATOR).action_id == action_id
    assert not queue.approve(action_id, APPROVER)

    clock.advance(timedelta(hours=10))
    assert queue.apply_revision(action_id, payload={"contact_id": "c-42", "body": "Shorter."},
                                summary="Intro text to Sam (shorter)", note="shorter")
    action = queue.get(action_id)
    assert action.status is ActionStatus.PENDING
    assert action.payload["body"] == "Shorter."
    assert action.card_ts is None
    assert action.expires_at == clock.now + DEFAULT_TTL
    assert [entry["payload"]["body"] for entry in action.revisions] == ["Hi Sam, Josh here."]
    assert action.revisions[0]["revised_by"] == OPERATOR
    assert action.revisions[0]["note"] == "shorter"
    assert queue.revising_for(OPERATOR) is None


def test_revise_and_approve_are_recorded_separately(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.request_revision(action_id, OPERATOR)
    queue.apply_revision(action_id, payload={"body": "v2"}, summary="v2", note="tighter")
    queue.approve(action_id, APPROVER)
    action = queue.get(action_id)
    assert (action.revised_by, action.decided_by) == (OPERATOR, APPROVER)


def test_two_revisions_stack_oldest_first(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue, "v1")
    for body in ("v2", "v3"):
        queue.request_revision(action_id, APPROVER)
        queue.apply_revision(action_id, payload={"body": body}, summary=body, note=f"to {body}")
    assert [entry["payload"]["body"] for entry in queue.get(action_id).revisions] == ["v1", "v2"]


def test_one_open_revision_per_user(queue: PendingActionQueue) -> None:
    first, second = enqueue_sms(queue), enqueue_sms(queue, "Second draft")
    queue.request_revision(first, APPROVER)
    queue.request_revision(second, APPROVER)
    assert queue.get(first).status is ActionStatus.PENDING
    assert queue.revising_for(APPROVER).action_id == second


def test_cancel_revision_returns_to_pending(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.request_revision(action_id, APPROVER)
    assert queue.cancel_revision(action_id)
    assert queue.get(action_id).status is ActionStatus.PENDING


def test_expired_draft_cannot_be_approved(queue: PendingActionQueue, clock: Clock) -> None:
    action_id = enqueue_sms(queue, ttl=timedelta(hours=1))
    clock.advance(timedelta(hours=1))
    assert is_expired(queue.get(action_id), clock.now)
    assert not queue.approve(action_id, APPROVER)
    assert queue.get(action_id).status is ActionStatus.PENDING


def test_expire_stale_sweeps_only_overdue_open_drafts(queue: PendingActionQueue, clock: Clock) -> None:
    overdue = enqueue_sms(queue, ttl=timedelta(hours=1))
    revising = enqueue_sms(queue, ttl=timedelta(hours=1))
    queue.request_revision(revising, APPROVER)
    approved = enqueue_sms(queue, ttl=timedelta(hours=1))
    queue.approve(approved, APPROVER)
    fresh = enqueue_sms(queue)
    clock.advance(timedelta(hours=2))

    expired = {action.action_id for action in queue.expire_stale()}
    assert expired == {overdue, revising}
    assert queue.get(approved).status is ActionStatus.APPROVED
    assert queue.get(fresh).status is ActionStatus.PENDING
    assert queue.expire_stale() == []


def test_claim_is_exclusive_and_outcomes_need_a_claim(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    assert not queue.claim_for_send(action_id)
    queue.approve(action_id, APPROVER)
    assert not queue.mark_sent(action_id, "msg")
    assert queue.claim_for_send(action_id)
    assert not queue.claim_for_send(action_id)
    assert queue.mark_sent(action_id, "msg-9")
    action = queue.get(action_id)
    assert action.status is ActionStatus.SENT
    assert action.provider_ref == "msg-9"


@pytest.mark.parametrize("closer,status", [("mark_failed", ActionStatus.FAILED), ("mark_blocked", ActionStatus.BLOCKED)])
def test_unsent_outcomes_mask_digits_and_truncate(queue: PendingActionQueue, closer: str, status: ActionStatus) -> None:
    action_id = enqueue_sms(queue)
    queue.approve(action_id, APPROVER)
    queue.claim_for_send(action_id)
    getattr(queue, closer)(action_id, "GHL rejected 727-555-0100: " + "x" * 2000)
    action = queue.get(action_id)
    assert action.status is status
    assert "727-555-0100" not in action.error and "[…0100]" in action.error
    assert len(action.error) == 500


def test_approved_ids_lists_only_approved(queue: PendingActionQueue) -> None:
    approved, pending = enqueue_sms(queue), enqueue_sms(queue)
    queue.approve(approved, APPROVER)
    assert queue.approved_ids() == [approved]
    assert pending not in queue.approved_ids()
