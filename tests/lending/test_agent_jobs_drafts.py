"""P1.1: a checked job becomes the same kind of Cora draft the chat tools make, so the existing executors send it."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.lending.agent_jobs.drafts import build_draft
from src.lending.agent_jobs.job import AgentJob
from src.lending.ghl_crm import GhlContact

NOW = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
CONTACT = GhlContact(contact_id="ghl-123", phone="+18135550142", email="mark@example.com",
                     dnd_channels=frozenset(), tags=frozenset())
JOB_ID = "7f1c2e9a-3b4d-4c5e-8f60-718293a4b5c6"


def _job(**overrides) -> AgentJob:
    data = dict(job_id=JOB_ID, lane="NDL", created_by="LendingFlow follow up", created_at=NOW,
                type="send_sms", ghl_contact_id="ghl-123", contact_label="Mark T., Tampa",
                template_version="LF-D2-SMS-v1", body="Hi Mark, any questions?", reason="Day 2 touch",
                requires_approval=True, expires_at=NOW + timedelta(days=1))
    data.update(overrides)
    return AgentJob.model_validate(data)


def test_sms_payload_matches_existing_executor_keys() -> None:
    draft = build_draft(_job(), CONTACT, "ghl")
    assert draft.channel == "ghl_sms"
    assert draft.payload == {"job_id": JOB_ID, "ghl_contact_id": "ghl-123", "template_version": "LF-D2-SMS-v1",
                             "to_phone": "+18135550142", "body": "Hi Mark, any questions?"}
    assert (draft.recipient_phone, draft.recipient_email, draft.contact_ref) == (
        "+18135550142", "mark@example.com", "ghl:ghl-123")


def test_warm_email_payload_has_subject() -> None:
    draft = build_draft(_job(type="send_email", subject="Your loan"), CONTACT, "ghl")
    assert draft.channel == "ghl_email"
    assert {k: draft.payload[k] for k in ("to_email", "to_phone", "subject", "body")} == {
        "to_email": "mark@example.com", "to_phone": "+18135550142", "subject": "Your loan",
        "body": "Hi Mark, any questions?"}


def test_cold_email_uses_instantly_channel_without_footer() -> None:
    draft = build_draft(_job(type="send_email", subject="Your loan"), CONTACT, "instantly")
    assert draft.channel == "instantly_email"
    assert draft.payload["body"] == "Hi Mark, any questions?"
    assert "unsubscribe" not in draft.payload["body"].lower()
    assert "to_phone" not in draft.payload


def test_crm_payloads() -> None:
    assert build_draft(_job(type="move_stage", body="Application"), CONTACT, "ghl").payload["stage"] == "Application"
    assert build_draft(_job(type="add_tag", body=" hot "), CONTACT, "ghl").payload["tag"] == "hot"
    note = build_draft(_job(type="add_note", body="Asked for the doc list"), CONTACT, "ghl")
    assert (note.channel, note.payload["body"]) == ("ghl_note", "Asked for the doc list")


def test_task_default_due_is_5pm_et() -> None:
    draft = build_draft(_job(type="create_task", body="Call Mark"), CONTACT, "ghl")
    assert draft.channel == "ghl_task"
    assert draft.payload["title"] == "Call Mark"
    assert datetime.fromisoformat(draft.payload["due_at"]) == datetime(2026, 11, 12, 22, 0, tzinfo=timezone.utc)
    assert draft.payload["assignee"] is None


def test_task_due_and_assignee_from_job() -> None:
    due = NOW + timedelta(days=2)
    draft = build_draft(_job(type="create_task", body="Call", task_due_at=due, task_assignee="user-david"), CONTACT, "ghl")
    assert datetime.fromisoformat(draft.payload["due_at"]) == due
    assert draft.payload["assignee"] == "user-david"


def test_summary_is_slack_escaped_on_the_card() -> None:
    from packages.agent_core.approval import render_action_card
    from packages.agent_core.pending_actions import ActionStatus, PendingAction

    draft = build_draft(_job(contact_label="<!channel> Mark"), CONTACT, "ghl")
    action = PendingAction(
        action_id=1, tool_name="agent_job:send_sms", channel=draft.channel, payload=draft.payload,
        summary=draft.summary, status=ActionStatus.PENDING, requested_by="agent_job", source_channel=None,
        source_thread_ts=None, card_channel=None, card_ts=None, decided_by=None, revised_by=None, revision_note=None,
        revisions=(), recipient_phone=draft.recipient_phone, recipient_email=draft.recipient_email,
        contact_ref=draft.contact_ref, deal_ref=None, idempotency_key=None, expires_at=None, provider_ref=None,
        error=None)
    rendered = str(render_action_card(action))
    assert "<!channel>" not in rendered
    assert "+18135550142" not in rendered


def test_summary_shows_rail_and_last4_only() -> None:
    draft = build_draft(_job(type="send_email", subject="S"), CONTACT, "instantly")
    assert "LendingFlow follow up" in draft.summary and "LF-D2-SMS-v1" in draft.summary
    assert "via Instantly (cold)" in draft.summary
    assert "0142" in draft.summary
    assert "+18135550142" not in draft.summary and "mark@example.com" not in draft.summary
