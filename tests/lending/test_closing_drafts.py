from __future__ import annotations

from datetime import timedelta

from packages.agent_core.tools import QueuedDraft
from src.lending.closing.drafts import Recipient, chase_draft, submit_draft

MARK = Recipient("Mark", "+18135550101", "mark@example.com", "c1")


def _draft(recipient=MARK, *, text_ok=True, kind="d2"):
    return chase_draft(kind, recipient, link="https://x.test/api/lending/docs/tok", missing=["Photo ID", "LLC documents"],
                       text_ok=text_ok, contact_ref="ghl:c1", deal_ref="doc_chase:1")


def test_text_when_consented():
    draft = _draft()
    assert draft.channel == "ghl_sms" and draft.payload["to_phone"] == "+18135550101"
    assert draft.payload["template_version"] == "DOCS-CHASE-D2-SMS-v1"
    assert "https://x.test/api/lending/docs/tok" in draft.payload["body"] and "2 document(s)" in draft.payload["body"]
    assert draft.recipient_phone == "+18135550101" and draft.contact_ref == "ghl:c1" and draft.deal_ref == "doc_chase:1"


def test_email_when_no_text_consent():
    draft = _draft(text_ok=False)
    assert draft.channel == "ghl_email" and draft.payload["template_version"] == "DOCS-CHASE-D2-EMAIL-v1"
    assert draft.payload["subject"] == "Reminder: documents still needed"
    assert "Photo ID, LLC documents" in draft.payload["body"] and "$" not in draft.payload["body"]


def test_no_consent_no_email_gives_no_draft():
    assert _draft(Recipient("Mark", "+18135550101", None, "c1"), text_ok=False) is None


def test_summary_shows_last4_only():
    summary = _draft().summary
    assert "…0101" in summary and "8135550101" not in summary


def test_no_first_name_greets_there():
    assert _draft(Recipient(None, "+18135550101", None, None)).payload["body"].startswith("Hi there,")


class _Gate:
    def __init__(self):
        self.calls = []

    def submit(self, draft, *, tool_name, context, ttl=None):
        self.calls.append((draft, tool_name, context, ttl))
        return QueuedDraft(41, True)


def test_submit_uses_key_as_idempotency_and_no_approver():
    gate = _Gate()
    assert submit_draft(gate, _draft(), key="doc_chase:1:d2", channel="C_APPROVALS", ttl=timedelta(hours=24)) == 41
    _, tool_name, context, ttl = gate.calls[0]
    assert tool_name == "doc_chase" and context.tool_use_id == "doc_chase:1:d2" and not context.is_approver
    assert context.user_id == "doc_chase" and context.channel == "C_APPROVALS" and ttl == timedelta(hours=24)
