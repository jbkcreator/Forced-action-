"""R1 1.5: unified inbox config, setting, table and the Instantly intent constraint."""
from __future__ import annotations

from config import lending_unified_inbox as c


def test_channels_sources_and_copy():
    assert c.CHANNELS == {"ghl_sms", "ghl_email", "ghl_other", "missed_call"}
    assert set(c.SOURCES) == {"ghl", "batchdialer"}  # no HubSpot / Salesmsg / Gmail adapter (client Q1)
    assert c.MISSED_CALL_REPLY_TEXT.endswith("Reply STOP to opt out.")
    assert c.TRIAGE_SLA_SECONDS == 120 and c.AI_GRACE_SECONDS == 30


def test_the_reply_agent_messages_are_the_stop_bot_finals_split_into_handoffs_and_the_opt_out():
    assert len(c.REPLY_AGENT_HANDOFF_MESSAGES) == 2 and len(c.REPLY_AGENT_OPT_OUT_MESSAGES) == 1
    assert all(m == " ".join(m.lower().split())
               for m in (*c.REPLY_AGENT_HANDOFF_MESSAGES, *c.REPLY_AGENT_OPT_OUT_MESSAGES))
    assert "won't text you again" in c.REPLY_AGENT_OPT_OUT_MESSAGES[0]


def test_setting_defaults_off():
    from config.settings import AppSettings

    assert AppSettings.model_fields["lending_unified_inbox_enabled"].default is False


def test_instantly_intent_check_allows_question():
    from src.lending.models import LendingInstantlyReply

    ck = next(x for x in LendingInstantlyReply.__table__.constraints if x.name == "ck_instantly_replies_intent")
    assert "'question'" in str(ck.sqltext)


def test_inbox_events_table_shape():
    from src.lending.models import LendingInboxEvent

    table = LendingInboxEvent.__table__
    assert table.schema == "lending" or table.name == "inbox_events"
    assert {"dedup_key", "channel", "answered_by", "ai_handoff", "josh_pinged", "triage_posted_at"} <= set(table.c.keys())
    assert any(i.name == "uq_inbox_events_dedup" and i.unique for i in table.indexes)
