"""P2.4: the 7 AM closing blocker card."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from packages.agent_core.chatport import FakeChatPort, PostResult
from packages.agent_core.tools import QueuedDraft
from src.lending.closing.blockers import card_day, post_blocker_card, stuck_deals

AT_7_00 = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)  # 7:00 AM EST
DAY = date(2026, 11, 17)


class Gate:
    def __init__(self):
        self.drafts = {}

    def submit(self, draft, *, tool_name, context, ttl=None):
        self.drafts.setdefault(context.tool_use_id, draft)
        return QueuedDraft(len(self.drafts), True)


class FailingChat(FakeChatPort):
    def post(self, **kwargs) -> PostResult:
        return PostResult(ok=False, error="channel_not_found")


def _stage(db, opp, stage, days_ago, phone="+18135550101"):
    db.execute(text("INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, entered_at, "
                    "phone) VALUES (:o, :s, :k, :at, :p)"),
               {"o": opp, "s": stage, "k": stage.lower(), "at": AT_7_00 - days_ago, "p": phone})


def _chase(db, phone="+18135550101", missing=("Photo ID", "LLC documents"), received=("Valuation",)):
    db.execute(text(
        "INSERT INTO lending.doc_chases (lead_ref, first_name, phone, email, lender_key, accepted_at, accepted_by) "
        "VALUES ('web_lead:1', 'Mark', :p, 'mark@example.com', 'central_lending', :at, 'slack:U1')"),
        {"p": phone, "at": AT_7_00 - timedelta(days=5)})
    chase_id = db.execute(text("SELECT max(id) FROM lending.doc_chases")).scalar()
    for i, label in enumerate(missing + received):
        db.execute(text("INSERT INTO lending.doc_chase_items (chase_id, item_code, label, received_at) "
                        "VALUES (:c, :code, :label, :r)"),
                   {"c": chase_id, "code": f"i{i}", "label": label, "r": AT_7_00 if label in received else None})
    return chase_id


def _post(db, chat, gate, now=AT_7_00, consent=True):
    return post_blocker_card(db, chat, gate, channel="C_CORA", approvals_channel="C_APPROVALS", now=now,
                             text_consent=lambda phone: consent)


def _card_text(chat):
    return json.dumps(chat.posts[-1]["blocks"], ensure_ascii=False)


def test_card_due_from_7_until_noon_et():
    assert card_day(AT_7_00) == DAY
    assert card_day(AT_7_00 - timedelta(minutes=1)) is None
    assert card_day(AT_7_00 + timedelta(hours=5)) is None


def test_stuck_deal_in_underwriting_shows_with_its_documents_and_chase(closing_db):
    _stage(closing_db, "opp1", "Submitted to lender", timedelta(days=4))
    _chase(closing_db)
    deal, = stuck_deals(closing_db, AT_7_00)
    assert (deal.owner, deal.days, deal.missing, deal.first_name) == ("borrower", 4, ("Photo ID", "LLC documents"), "Mark")
    chat, gate = FakeChatPort(), Gate()
    assert _post(closing_db, chat, gate)
    card = _card_text(chat)
    assert "🔴" in card and "Mark" in card and "4 days" in card and "Photo ID, LLC documents" in card
    assert gate.drafts == {}  # the document chase already reminds this borrower on day 2/5/8


def test_stuck_borrower_without_chase_gets_a_drafted_check_in(closing_db):
    _stage(closing_db, "opp2", "Application", timedelta(days=4))
    chat, gate = FakeChatPort(), Gate()
    _post(closing_db, chat, gate)
    (key, draft), = gate.drafts.items()
    assert key.startswith("blocker:opp2:") and draft.payload["template_version"] == "BLOCKER-BORROWER-SMS-v1"
    assert "#approvals" in _card_text(chat) or "C_APPROVALS" in _card_text(chat)


def test_lender_owned_deal_gets_inline_draft_and_no_send(closing_db):
    _stage(closing_db, "opp3", "Submitted to lender", timedelta(days=6))
    chat, gate = FakeChatPort(), Gate()
    _post(closing_db, chat, gate)
    assert gate.drafts == {} and "checking on the status" in _card_text(chat) and "waiting on lender" in _card_text(chat)


def test_ready_to_fund_is_waiting_on_title(closing_db):
    _stage(closing_db, "opp4", "Ready to fund", timedelta(days=5))
    assert stuck_deals(closing_db, AT_7_00)[0].owner == "title"


def test_three_days_exactly_and_closed_or_lead_stages_not_listed(closing_db):
    _stage(closing_db, "a", "Application", timedelta(days=3))
    _stage(closing_db, "b", "Funded", timedelta(days=9))
    _stage(closing_db, "c", "Booked", timedelta(days=9))
    assert stuck_deals(closing_db, AT_7_00) == []


def test_no_consent_and_no_email_says_so_on_the_card(closing_db):
    _stage(closing_db, "opp5", "Application", timedelta(days=4))
    chat, gate = FakeChatPort(), Gate()
    _post(closing_db, chat, gate, consent=False)
    assert gate.drafts == {} and "no text consent or email" in _card_text(chat).lower()


def test_posts_once_a_day_and_retries_after_failed_post(closing_db):
    _stage(closing_db, "opp6", "Application", timedelta(days=4))
    with pytest.raises(RuntimeError):
        _post(closing_db, FailingChat(), Gate())
    chat = FakeChatPort()
    assert _post(closing_db, chat, Gate())
    assert not _post(closing_db, chat, Gate(), now=AT_7_00 + timedelta(minutes=1))
    assert len(chat.posts) == 1


def test_no_stuck_deals_still_posts(closing_db):
    chat = FakeChatPort()
    assert _post(closing_db, chat, Gate())
    assert "No deals stuck more than 3 days" in _card_text(chat)


def test_card_shows_last4_only(closing_db):
    _stage(closing_db, "opp7", "Submitted to lender", timedelta(days=4))
    chat = FakeChatPort()
    _post(closing_db, chat, Gate())
    assert "0101" in _card_text(chat) and "8135550101" not in _card_text(chat) + chat.posts[-1]["text"]


class BrokenGate(Gate):
    def submit(self, draft, *, tool_name, context, ttl=None):
        if "opp-bad" in context.tool_use_id:
            raise RuntimeError("lane violation")
        return super().submit(draft, tool_name=tool_name, context=context, ttl=ttl)


def test_one_failing_deal_does_not_stop_the_card(closing_db):
    _stage(closing_db, "opp-bad", "Application", timedelta(days=5), phone="+18135550111")
    _stage(closing_db, "opp-ok", "Application", timedelta(days=4), phone="+18135550112")
    chat, gate = FakeChatPort(), BrokenGate()
    assert _post(closing_db, chat, gate)
    card = _card_text(chat)
    assert "0111" in card and "0112" in card and "call them" in card and len(gate.drafts) == 1


def test_check_in_uses_the_lead_record_email_without_text_consent(closing_db):
    closing_db.execute(text("CREATE TABLE lending.web_leads (id BIGSERIAL PRIMARY KEY, phone VARCHAR(20), "
                            "email VARCHAR(255), ghl_contact_id VARCHAR(64), received_at TIMESTAMPTZ DEFAULT now())"))
    closing_db.execute(text("INSERT INTO lending.web_leads (phone, email, ghl_contact_id) "
                            "VALUES ('+18135550101', 'mark@example.com', 'ghl-9')"))
    _stage(closing_db, "opp8", "Application", timedelta(days=4))
    chat, gate = FakeChatPort(), Gate()
    _post(closing_db, chat, gate, consent=False)
    draft, = gate.drafts.values()
    assert draft.channel == "ghl_email" and draft.payload["to_email"] == "mark@example.com"
    assert draft.payload["ghl_contact_id"] == "ghl-9" and "no text consent or email" not in _card_text(chat).lower()


def test_every_stuck_deal_is_visible_however_long_the_card(closing_db):
    for i in range(60):
        _stage(closing_db, f"opp-{i:02d}", "Submitted to lender", timedelta(days=4, minutes=i),
               phone=f"+1813555{i:04d}")
    chat = FakeChatPort()
    _post(closing_db, chat, Gate())
    everything = "".join(json.dumps(p["blocks"], ensure_ascii=False) for p in chat.posts)
    assert all(f"…{i:04d}" in everything for i in range(60))
    assert "…" not in "".join(b["text"]["text"][-2:] for p in chat.posts for b in p["blocks"] if b["type"] == "section")
    assert all(len(p["blocks"]) <= 50 for p in chat.posts)
    assert all(p["thread_ts"] == chat.posts[0]["ts"] for p in chat.posts[1:])
