"""P2.4: starting a document chase when the borrower accepts the term sheet."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.closing.doc_chase import chase_for_token, issue_link, start_chase
from src.lending.closing.drafts import Recipient
from src.lending.partner_service import PartnerLinkNotConfigured

NOW = datetime(2026, 11, 17, 15, 0, tzinfo=timezone.utc)
MARK = Recipient("Mark", "+18135550101", "mark@example.com", "c1")


@pytest.fixture
def link_base(monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", "https://api.example.com", raising=False)


def _start(db, **kwargs):
    return start_chase(db, MARK, lead_ref="web_lead:7", accepted_by="slack:U1", now=NOW, **kwargs)


def test_start_creates_central_checklist(closing_db, link_base):
    started = _start(closing_db)
    assert not started.reused and len(started.missing) == 6 and started.missing[0] == "Signed purchase agreement"
    row = closing_db.execute(text("SELECT lead_ref, phone, lender_key, status, accepted_at FROM lending.doc_chases")).one()
    assert row == ("web_lead:7", "+18135550101", "central_lending", "open", NOW)


def test_start_links_the_deal_in_ghl_by_phone(closing_db, link_base):
    closing_db.execute(text("INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, "
                            "entered_at, phone) VALUES ('opp-1', 'Ready to fund', 'ready to fund', :t, '+18135550101')"),
                       {"t": NOW})
    _start(closing_db)
    assert closing_db.execute(text("SELECT ghl_opportunity_id FROM lending.doc_chases")).scalar() == "opp-1"


def test_quote_condition_adds_reserve_item(closing_db, link_base, monkeypatch):
    monkeypatch.setattr("src.lending.closing.doc_chase._quote_conditions",
                        lambda db, quote_id: ["INTEREST_RESERVE_FULL_TERM"])
    started = _start(closing_db, quote_id="5f0c1e0a-0000-4000-8000-000000000001")
    assert len(started.missing) == 7 and started.missing[-1].startswith("Proof of funds")


def test_missing_quote_table_adds_nothing(closing_db, link_base):
    assert len(_start(closing_db, quote_id="5f0c1e0a-0000-4000-8000-000000000001").missing) == 6


def test_second_start_reuses_open_chase(closing_db, link_base):
    first = _start(closing_db)
    second = _start(closing_db)
    assert second.reused and second.chase_id == first.chase_id
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chases")).scalar() == 1


def test_no_link_host_refuses_to_start(closing_db, monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", "", raising=False)
    with pytest.raises(PartnerLinkNotConfigured):
        _start(closing_db)


def test_no_phone_and_no_email_refused(closing_db, link_base):
    with pytest.raises(ValueError):
        start_chase(closing_db, Recipient("Mark", None, None, None), lead_ref="web_lead:7", accepted_by="slack:U1", now=NOW)


def test_every_issued_link_works_until_expiry_or_close(closing_db, link_base):
    chase_id = _start(closing_db).chase_id
    first, second = issue_link(closing_db, chase_id, NOW), issue_link(closing_db, chase_id, NOW)
    token = first.rsplit("/", 1)[1]
    assert first.startswith("https://api.example.com/api/lending/docs/") and first != second
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chase_links WHERE token_hash = :t"),
                              {"t": token}).scalar() == 0  # only the hash is stored
    assert chase_for_token(closing_db, token, NOW)["id"] == chase_id
    assert chase_for_token(closing_db, second.rsplit("/", 1)[1], NOW)["id"] == chase_id
    assert chase_for_token(closing_db, token, NOW + timedelta(days=31)) is None
    assert chase_for_token(closing_db, "not-a-token", NOW) is None
    closing_db.execute(text("UPDATE lending.doc_chases SET status = 'complete'"))
    assert chase_for_token(closing_db, token, NOW) is None
