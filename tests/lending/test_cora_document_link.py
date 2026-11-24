"""R1 0.2: Josh asks Cora for a borrower's document and gets a private 15-minute link; she never guesses."""
from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from packages.agent_core.governance import SafetyLevel
from packages.agent_core.tools import ToolContext, ToolInputError
from src.lending.cora_tools import CoraToolkit, SendDocumentLinkInput

JOSH = ToolContext(user_id="U_JOSH", is_approver=True, channel="C", thread_ts="1.0", tool_use_id="toolu_1")
DAVID = ToolContext(user_id="U_DAVID", is_approver=False, channel="C", thread_ts="1.0", tool_use_id="toolu_2")


class _Same:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, *exc):
        return False


class FakeChannels:
    def __init__(self, refuse=False):
        self.private_posts, self.public_posts, self.refuse = [], [], refuse

    def post_private(self, channel_id, user_id, thread_ts, text_):
        if self.refuse:
            raise RuntimeError("channel_not_found")
        self.private_posts.append((channel_id, user_id, thread_ts, text_))

    def post(self, channel, text_):
        self.public_posts.append((channel, text_))


@pytest.fixture
def cora(documents_db):
    channels = FakeChannels()
    toolkit = CoraToolkit(engine=None, memory=None, channels=channels,  # type: ignore[arg-type]
                          inbox_session=lambda: _Same(documents_db.db))
    smith = documents_db.add(1, "Mark Smith", "+18135550101", uploaded=("bank_statements",))
    taylor = documents_db.add(2, "Mark Taylor", "+18135554432", uploaded=("government_id",))
    return toolkit, channels, smith, taylor, documents_db


def _links(docs) -> int:
    return docs.db.execute(text("SELECT count(*) FROM lending.doc_view_links")).scalar()


def test_registered_approver_only():
    tool = {t.name: t for t in CoraToolkit(engine=None, memory=None).tools()}["send_document_link"]  # type: ignore[arg-type]
    assert tool.approver_only and tool.safety is SafetyLevel.INTERNAL_WRITE


def test_one_match_sends_ephemeral_in_thread_and_returns_no_link(cora):
    toolkit, channels, *_ = cora
    result = toolkit.send_document_link(SendDocumentLinkInput(borrower="Mark Smith", item_code="bank_statements"), JOSH)
    (channel, user, thread, message), = channels.private_posts
    assert (channel, user, thread) == ("C", "U_JOSH", "1.0") and channels.public_posts == []
    assert "https://api.example.com/api/lending/docs/view/" in message and "Mark S. …0101" in message
    assert result["sent_privately"] is True and result["expires_in_minutes"] == 15
    assert "http" not in json.dumps(result) and "/docs/view/" not in json.dumps(result)


def test_two_marks_lists_both_and_sends_nothing(cora):
    toolkit, channels, smith, taylor, docs = cora
    result = toolkit.send_document_link(SendDocumentLinkInput(borrower="Mark", item_code="bank_statements"), JOSH)
    assert result["sent_privately"] is False
    choices = {c["lead_ref"]: c for c in result["choose_one"]}
    assert choices[smith.lead_ref]["has_this_document"] is True and choices[taylor.lead_ref]["has_this_document"] is False
    assert choices[smith.lead_ref]["borrower"] == "Mark S. …0101"
    assert channels.private_posts == [] and _links(docs) == 0


def test_pick_by_lead_ref_then_sends(cora):
    toolkit, channels, smith, *_ = cora
    toolkit.send_document_link(SendDocumentLinkInput(borrower=smith.lead_ref, item_code="bank_statements"), JOSH)
    assert len(channels.private_posts) == 1


def test_more_than_10_asks_to_narrow(cora):
    toolkit, channels, _, _, docs = cora
    for n in range(9):
        docs.add(100 + n, f"Mark Person{n}", f"+1813555{1000 + n}", uploaded=())
    with pytest.raises(ToolInputError, match="more than 10"):
        toolkit.send_document_link(SendDocumentLinkInput(borrower="Mark", item_code="bank_statements"), JOSH)
    assert channels.private_posts == []


def test_caller_refused_even_if_approver_flag_bypassed(cora):
    toolkit, channels, _, _, docs = cora
    with pytest.raises(ToolInputError, match="only an admin"):
        toolkit.send_document_link(SendDocumentLinkInput(borrower="Mark Smith", item_code="bank_statements"), DAVID)
    assert channels.private_posts == [] and _links(docs) == 0


def test_no_match_and_item_not_uploaded_are_clear(cora):
    toolkit, *_ = cora
    with pytest.raises(ToolInputError, match="no borrower matching"):
        toolkit.send_document_link(SendDocumentLinkInput(borrower="Zed", item_code="bank_statements"), JOSH)
    with pytest.raises(ToolInputError, match="has not uploaded government_id"):
        toolkit.send_document_link(SendDocumentLinkInput(borrower="Mark Smith", item_code="government_id"), JOSH)


def test_slack_refusal_reports_nothing_shared(cora):
    toolkit, channels, *_ = cora
    channels.refuse = True
    with pytest.raises(ToolInputError, match="nothing was shared"):
        toolkit.send_document_link(SendDocumentLinkInput(borrower="Mark Smith", item_code="bank_statements"), JOSH)
