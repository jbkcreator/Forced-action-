"""R1 0.2: Slack shows document status only, never a link or where a file is stored (pins today's behaviour)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from packages.agent_core.chatport import FakeChatPort
from src.lending.closing.blockers import post_blocker_card
from src.lending.closing.doc_chase import open_chase_summaries
from tests.lending.test_closing_blockers import Gate

AT_7_00 = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)  # 7:00 AM EST


def _no_location(out: str, docs) -> None:
    assert "/api/lending/docs" not in out and str(docs.tmp_path) not in out and "http" not in out
    assert "bank_statements.pdf" not in out


def test_chase_summaries_have_no_links_or_paths(documents_db):
    documents_db.add(1, "Mark Smith", "+18135550101")
    summaries = open_chase_summaries(documents_db.db, AT_7_00)
    assert summaries and summaries[0]["received"] == 1
    _no_location(json.dumps(summaries, default=str), documents_db)


def test_blocker_card_has_no_links_or_paths(documents_db):
    documents_db.add(1, "Mark Smith", "+18135550101")
    documents_db.db.execute(text(
        "INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, entered_at, phone) "
        "VALUES ('opp1', 'Application', 'application', :at, '+18135550101')"), {"at": AT_7_00 - timedelta(days=4)})
    chat = FakeChatPort()
    assert post_blocker_card(documents_db.db, chat, Gate(), channel="C_CORA", approvals_channel="C_APPROVALS",
                             now=AT_7_00, text_consent=lambda phone: True)
    card = json.dumps([post["blocks"] for post in chat.posts], ensure_ascii=False)
    assert "Mark" in card
    _no_location(card, documents_db)
