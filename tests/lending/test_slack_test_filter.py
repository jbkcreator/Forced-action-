"""LENDING_SLACK_TEST_ENABLED: posts about test records stay out of Slack on live (src.lending.slack_test_filter)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.lending import slack_test_filter
from src.lending.slack_test_filter import (
    is_hidden_lendingflow_uuid, hidden_ghl_contacts, hidden_lendingflow_ids, hidden_loan_file_ids, hidden_phones,
    hidden_web_lead_ids,
)

TEST_PHONE, REAL_PHONE, SHARED_PHONE = "+18135550101", "+18135550102", "+18135550103"


@pytest.fixture
def live(monkeypatch):
    monkeypatch.setattr(slack_test_filter, "slack_test_posts_enabled", lambda: False)


@pytest.fixture
def testing(monkeypatch):
    monkeypatch.setattr(slack_test_filter, "slack_test_posts_enabled", lambda: True)


def _lendingflow(db, tag: str, phone: str, email: str, *, is_test: bool = False, contact: str | None = None) -> tuple:
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, first_name, phone, email,"
        " is_test, ghl_contact_id) VALUES (:v, :h, '{}', 'Ana', :p, :e, :t, :c) RETURNING id, lead_uuid::text"),
        {"v": f"stf-{tag}", "h": f"stf-hash-{tag}", "p": phone, "e": email, "t": is_test, "c": contact}).one()


def _web(db, phone: str, email: str, *, is_test: bool = False, contact: str | None = None) -> int:
    return db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email, is_test, ghl_contact_id) VALUES ('Ben Ray', :p, :e, :t, :c)"
        " RETURNING id"), {"p": phone, "e": email, "t": is_test, "c": contact}).scalar()


def test_a_test_domain_lead_is_hidden_before_the_tagging_job_runs(lending_db, live) -> None:
    lead_id, lead_uuid = _lendingflow(lending_db, "a", TEST_PHONE, "qa@example.com")
    assert hidden_lendingflow_ids(lending_db, [lead_id]) == {lead_id}
    assert is_hidden_lendingflow_uuid(lending_db, lead_uuid)
    assert hidden_phones(lending_db, [TEST_PHONE]) == {TEST_PHONE}


def test_a_lead_tagged_is_test_is_hidden_whatever_its_email(lending_db, live) -> None:
    web_id = _web(lending_db, TEST_PHONE, "someone@gmail.com", is_test=True, contact="stf-c1")
    assert hidden_web_lead_ids(lending_db, [web_id]) == {web_id}
    assert hidden_ghl_contacts(lending_db, ["stf-c1"]) == {"stf-c1"}


def test_a_real_lead_always_posts(lending_db, live) -> None:
    lead_id, lead_uuid = _lendingflow(lending_db, "b", REAL_PHONE, "dana@gmail.com", contact="stf-c2")
    assert hidden_lendingflow_ids(lending_db, [lead_id]) == set()
    assert not is_hidden_lendingflow_uuid(lending_db, lead_uuid)
    assert hidden_phones(lending_db, [REAL_PHONE]) == set()
    assert hidden_ghl_contacts(lending_db, ["stf-c2"]) == set()


def test_a_phone_shared_with_a_real_lead_is_never_hidden(lending_db, live) -> None:
    _lendingflow(lending_db, "c", SHARED_PHONE, "qa@example.com")
    _web(lending_db, SHARED_PHONE, "real.borrower@gmail.com")
    assert hidden_phones(lending_db, [SHARED_PHONE]) == set()


def test_a_test_partner_file_is_hidden(lending_db, live) -> None:
    partner = lending_db.execute(text(
        "INSERT INTO lending.partners (name, partner_type, token_hash) VALUES ('TEST R1 Partner', 'broker', 'stf-hash')"
        " RETURNING id")).scalar()
    file_id = lending_db.execute(text(
        "INSERT INTO lending.loan_files (source, partner_id, borrower_first_name, borrower_last_name, borrower_email,"
        " product, loan_amount, property_state) VALUES ('partner', :p, 'Ana', 'Lopez', 'ana@gmail.com', 'flip',"
        " 300000, 'FL') RETURNING id"),
        {"p": partner}).scalar()
    assert hidden_loan_file_ids(lending_db, [file_id]) == {file_id}


def test_switch_on_lets_test_records_post(lending_db, testing) -> None:
    lead_id, lead_uuid = _lendingflow(lending_db, "d", TEST_PHONE, "qa@example.com")
    assert hidden_lendingflow_ids(lending_db, [lead_id]) == set()
    assert not is_hidden_lendingflow_uuid(lending_db, lead_uuid)
    assert hidden_phones(lending_db, [TEST_PHONE]) == set()


def test_a_failed_lookup_posts_as_usual(live) -> None:
    class BrokenSession:
        def begin_nested(self):
            raise RuntimeError("no database")

    assert hidden_phones(BrokenSession(), [TEST_PHONE]) == set()
    assert hidden_loan_file_ids(BrokenSession(), [1]) == set()


def test_the_switch_is_off_by_default() -> None:
    from config.settings import AppSettings

    assert AppSettings.model_fields["lending_slack_test_enabled"].default is False


def test_a_test_contact_rate_question_is_not_posted(lending_db, live) -> None:
    from migrations.apply_lending_booking_messages import apply_to
    from src.lending.reply_guard import ReplyEvent, handle_reply_event

    apply_to(lending_db.connection())
    _web(lending_db, TEST_PHONE, "qa@test.com", contact="stf-c3")
    posted: list[str] = []
    event = ReplyEvent(message_id="stf-m1", direction="inbound", body="What's your rate?", contact_id="stf-c3",
                       first_name="Ben", phone=TEST_PHONE, handoff=False)
    assert handle_reply_event(lending_db, event, poster=posted.append) == "test_skipped"
    assert posted == []
    assert lending_db.execute(text("SELECT posted_at FROM lending.reply_handoffs WHERE message_id = 'stf-m1'")
                              ).scalar() is None


def test_a_test_lead_alarm_skips_slack_but_is_recorded(lending_db, live) -> None:
    from src.lending import uncalled_alarm_worker as worker

    lead_id, lead_uuid = _lendingflow(lending_db, "e", TEST_PHONE, "qa@example.org")
    row = {"lead_uuid": lead_uuid, "from_web": False}
    assert worker._is_test_lead(lending_db, row)
    assert worker.TEST_SKIPPED == "test_skipped"


def test_a_test_name_with_a_real_email_is_hidden(lending_db, live) -> None:
    lead_id, _ = _lendingflow(lending_db, "f", REAL_PHONE, "sonetel.qa@gmail.com")
    lending_db.execute(text("UPDATE lending.lendingflow_leads SET first_name = 'TEST', last_name = 'Sonetel'"
                            " WHERE id = :id"), {"id": lead_id})
    web_id = lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email) VALUES ('test lead', :p, 'x@gmail.com') RETURNING id"),
        {"p": REAL_PHONE}).scalar()
    assert hidden_lendingflow_ids(lending_db, [lead_id]) == {lead_id}
    assert hidden_web_lead_ids(lending_db, [web_id]) == {web_id}


@pytest.mark.parametrize("name", ["Testa Rossi", "Tester Jones", "Ana Test", "Atest"])
def test_real_names_that_contain_test_are_not_hidden(lending_db, live, name) -> None:
    web_id = lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email) VALUES (:n, :p, 'x@gmail.com') RETURNING id"),
        {"n": name, "p": REAL_PHONE}).scalar()
    assert hidden_web_lead_ids(lending_db, [web_id]) == set()


def test_a_website_lead_from_the_office_ip_is_hidden(lending_db, live, monkeypatch) -> None:
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_test_ips", "203.0.113.7, 198.51.100.1")
    office, home = (lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email, ip_address) VALUES ('Ben Ray', :p, 'x@gmail.com', :ip)"
        " RETURNING id"), {"p": REAL_PHONE, "ip": ip}).scalar() for ip in ("203.0.113.7", "192.0.2.50"))
    assert hidden_web_lead_ids(lending_db, [office, home]) == {office}


def test_the_tagger_tags_test_names_and_office_ips(lending_db, monkeypatch) -> None:
    from config.settings import get_settings
    from src.lending.test_data import tag_test_rows

    monkeypatch.setattr(get_settings(), "lending_test_ips", "203.0.113.7")
    named = lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email) VALUES ('TEST Sonetel', :p, 'x@gmail.com') RETURNING id"),
        {"p": REAL_PHONE}).scalar()
    office = lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email, ip_address) VALUES ('Ben Ray', :p, 'y@gmail.com',"
        " '203.0.113.7') RETURNING id"), {"p": REAL_PHONE}).scalar()
    real = lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone, email, ip_address) VALUES ('Testa Rossi', :p, 'z@gmail.com',"
        " '192.0.2.50') RETURNING id"), {"p": REAL_PHONE}).scalar()
    report = tag_test_rows(lending_db, apply=True)
    assert named in report.tagged["lending.web_leads (test_name)"]
    assert office in report.tagged["lending.web_leads (office_ip)"]
    assert not lending_db.execute(text("SELECT is_test FROM lending.web_leads WHERE id = :id"), {"id": real}).scalar()


def _ingested(db, lead_id: str, first: str, last: str, phone: str, email: str):
    from migrations.apply_lending_lendingflow import apply_to
    from src.lending.lendingflow import parse_lendingflow, save_lead

    apply_to(db.get_bind())
    payload = {"lead_id": lead_id, "first_name": first, "last_name": last, "phone": phone, "email": email,
               "credit_score": "740+", "loan_amount": "$500K - $1M", "loan_type": "DSCR", "state": "Florida"}
    parsed = parse_lendingflow(payload)
    return save_lead(db, parsed, payload), parsed.phone


def test_an_ingested_test_name_lead_is_hidden_despite_its_contact_row(lending_db, live) -> None:
    saved, phone = _ingested(lending_db, "stf-g", "TEST", "Sonetel", "(813) 555-0104", "sonetel.qa@gmail.com")
    assert lending_db.execute(text("SELECT count(*) FROM lending.contacts WHERE phone = :p"), {"p": phone}).scalar() == 1
    assert hidden_lendingflow_ids(lending_db, [saved.lead_id]) == {saved.lead_id}
    assert hidden_phones(lending_db, [phone]) == {phone}


def test_an_ingested_real_lead_on_a_test_name_phone_is_not_hidden(lending_db, live) -> None:
    _, phone = _ingested(lending_db, "stf-h", "TEST", "Sonetel", "(813) 555-0105", "sonetel.qa@gmail.com")
    _ingested(lending_db, "stf-i", "Dana", "Ruiz", "(813) 555-0105", "dana.ruiz@gmail.com")
    assert hidden_phones(lending_db, [phone]) == set()


def test_a_contact_with_no_lead_behind_it_still_counts_as_real(lending_db, live) -> None:
    _, phone = _ingested(lending_db, "stf-j", "TEST", "Sonetel", "(813) 555-0106", "sonetel.qa@gmail.com")
    lending_db.execute(text("UPDATE lending.lendingflow_leads SET contact_id = NULL WHERE phone = :p"), {"p": phone})
    assert hidden_phones(lending_db, [phone]) == set()
