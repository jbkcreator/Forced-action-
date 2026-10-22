"""Partner Door (P-1) without the shared database, GHL, Slack or Redis.

In-memory SQLite with an attached ``lending`` schema runs the module's real SQL; outside services
are fakes. Covers the addendum's acceptance behaviour that does not need the overflow router:
consent capture, revocation, attribution, plus submission, pipeline and partner notices.
"""
from __future__ import annotations

import dataclasses
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi import FastAPI
from markupsafe import escape
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from config.settings import get_settings
from migrations.apply_lending_partner_door import apply_to
from src.api import lending_partner_router
from src.lending import partner_pipeline, partners
from src.lending.contracts import LenderFit, LenderFitResult
from src.lending.db import get_lending_db
from src.lending.prequal import FitLimits
from config.lender_matrix import LenderRules
from src.lending.models import LendingLoanFile, LendingPartner, LendingPartnerFileUpload, LendingPartnerSubmission

CONSENT = "I confirm I have the borrower's permission to share this information with Next Deal Lending."

SQLITE_DDL = (
    """CREATE TABLE lending.partners (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, company TEXT,
        partner_type TEXT NOT NULL, phone TEXT, email TEXT, states TEXT, agreement_status TEXT NOT NULL DEFAULT 'pending',
        referral_terms TEXT, sms_consent BOOLEAN NOT NULL DEFAULT 0, active BOOLEAN NOT NULL DEFAULT 1, created_by TEXT,
        token_hash TEXT NOT NULL UNIQUE, token_revoked_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP)""",
    """CREATE TABLE lending.loan_files (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, partner_id INTEGER,
        borrower_first_name TEXT NOT NULL, borrower_last_name TEXT NOT NULL, borrower_phone TEXT, borrower_email TEXT,
        product TEXT NOT NULL, loan_amount INTEGER NOT NULL, property_address TEXT, property_city TEXT,
        property_state TEXT NOT NULL, property_zip TEXT, purchase_price INTEGER, rehab_budget INTEGER, arv INTEGER,
        completed_projects INTEGER, credit_band TEXT, timeline TEXT, notes TEXT, status TEXT NOT NULL DEFAULT 'received'
        CHECK (status IN ('received','held','routed','declined','approved','funded')), status_reason TEXT,
        status_at TIMESTAMP NOT NULL, lender_fit TEXT, prequal_pdf_path TEXT, funded_at TIMESTAMP,
        notice_status TEXT, notice_emailed_at TIMESTAMP, notice_texted_at TIMESTAMP, notice_failed_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL)""",
    """CREATE TABLE lending.partner_submissions (id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL UNIQUE,
        partner_id INTEGER NOT NULL, consent_checked BOOLEAN NOT NULL, consent_text TEXT NOT NULL,
        consented_at TIMESTAMP NOT NULL, ip_address TEXT, page_url TEXT, user_agent TEXT,
        partner_confirmed_at TIMESTAMP, partner_emailed_at TIMESTAMP, partner_texted_at TIMESTAMP,
        josh_notified_at TIMESTAMP, prequal_sent_at TIMESTAMP, confirm_failed_at TIMESTAMP, created_at TIMESTAMP)""",
    """CREATE TABLE lending.partner_file_uploads (id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL,
        original_name TEXT NOT NULL, stored_path TEXT NOT NULL, content_type TEXT NOT NULL, size_bytes INTEGER NOT NULL,
        created_at TIMESTAMP)""",
)

FORM = {
    "borrower_first_name": "Dave", "borrower_last_name": "Reyes", "borrower_phone": "(813) 555-0142",
    "borrower_email": "Dave@Example.com", "property_address": "412 Palm Ave", "property_city": "Tampa",
    "property_state": "fl", "property_zip": "33602", "product": "flip", "purchase_price": "$180,000",
    "rehab_budget": "45,000", "loan_amount": "200000", "arv": "320000", "completed_projects": "4",
    "credit_band": "680-719", "timeline": "2-4 weeks", "notes": "Contract signed, closing in 3 weeks.",
    "consent": "yes",
}


@pytest.fixture
def engine():
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def _attach(dbapi_connection, _record):
        dbapi_connection.execute("ATTACH DATABASE ':memory:' AS lending")

    with engine.begin() as conn:
        for statement in SQLITE_DDL:
            conn.execute(text(statement))
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(engine):
    @contextmanager
    def factory():
        session = Session(engine)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    return factory


@pytest.fixture
def settings(monkeypatch, tmp_path):
    current = get_settings()
    monkeypatch.setattr(current, "lending_partner_consent_text", CONSENT)
    monkeypatch.setattr(current, "lending_partner_upload_dir", str(tmp_path / "uploads"))
    return current


@pytest.fixture
def client(engine, settings, monkeypatch):
    app = FastAPI()
    app.include_router(lending_partner_router.router)

    def _db():
        session = Session(engine)
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_lending_db] = _db
    queued: list[int] = []
    monkeypatch.setattr(lending_partner_router, "run_pipeline_in_background", queued.append)
    monkeypatch.setattr(lending_partner_router, "enforce_or_429", lambda *args, **kwargs: None)
    test_client = TestClient(app)
    test_client.queued = queued
    return test_client


def _partner(session_factory, **overrides) -> tuple[int, str]:
    values = dict(name="Maria Lopez", partner_type="wholesaler", company="Sunshine Deals LLC",
                  email="maria@example.com", phone="(727) 555-0199", agreement_status="signed")
    values.update(overrides)
    with session_factory() as db:
        return partners.create_partner(db, **values)


def _rows(engine, table: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(text(f"SELECT * FROM lending.{table}")).mappings()]


# -- links ----------------------------------------------------------------------------------------

def test_only_the_token_hash_is_stored_and_the_link_works(engine, session_factory) -> None:
    partner_id, token = _partner(session_factory)
    stored = _rows(engine, "partners")[0]
    assert token not in str(stored) and stored["token_hash"] == partners.hash_token(token)
    assert stored["phone"] == "+17275550199"
    with session_factory() as db:
        assert partners.partner_for_token(db, token).id == partner_id
        assert partners.partner_for_token(db, token + "x") is None


def test_revoked_link_stops_working_but_the_partner_and_files_stay(engine, session_factory, client) -> None:
    partner_id, token = _partner(session_factory)
    client.post(f"/api/lending/partners/{token}/files", data=FORM)
    with session_factory() as db:
        assert partners.revoke_link(db, partner_id)
        assert not partners.revoke_link(db, partner_id)
    page = client.get(f"/api/lending/partners/{token}")
    assert page.status_code == 404 and "no longer active" in page.text
    assert client.get(f"/api/lending/partners/{token}/status").status_code == 404
    assert client.post(f"/api/lending/partners/{token}/files", data=FORM).status_code == 404
    assert len(_rows(engine, "partners")) == 1 and len(_rows(engine, "loan_files")) == 1


def test_new_link_replaces_the_old_one(session_factory) -> None:
    partner_id, old = _partner(session_factory)
    with session_factory() as db:
        new = partners.issue_new_link(db, partner_id)
        assert partners.partner_for_token(db, old) is None
        assert partners.partner_for_token(db, new).id == partner_id
        assert partners.issue_new_link(db, 999) is None


def test_bad_partner_values_are_refused(session_factory) -> None:
    with pytest.raises(ValueError):
        _partner(session_factory, partner_type="lender")
    with pytest.raises(ValueError):
        _partner(session_factory, agreement_status="maybe")


# -- form -----------------------------------------------------------------------------------------

def test_form_shows_the_consent_wording(session_factory, client) -> None:
    _, token = _partner(session_factory)
    page = client.get(f"/api/lending/partners/{token}")
    assert page.status_code == 200
    assert str(escape(CONSENT)) in page.text and "Maria Lopez" in page.text
    assert 'name="consent"' in page.text and "required" in page.text
    assert client.get("/api/lending/partners/not-a-real-token").status_code == 404


def test_form_does_not_open_without_configured_consent_wording(session_factory, client, settings, monkeypatch) -> None:
    _, token = _partner(session_factory)
    monkeypatch.setattr(settings, "lending_partner_consent_text", "  ")
    assert client.get(f"/api/lending/partners/{token}").status_code == 503
    assert client.post(f"/api/lending/partners/{token}/files", data=FORM).status_code == 503


def test_valid_submission_creates_one_partner_file_with_consent_evidence(engine, session_factory, client) -> None:
    partner_id, token = _partner(session_factory)
    response = client.post(f"/api/lending/partners/{token}/files", data=FORM, headers={"user-agent": "PhoneBrowser"})
    assert response.status_code == 200 and "we received it" in response.text

    files = _rows(engine, "loan_files")
    assert len(files) == 1
    file = files[0]
    assert (file["source"], file["partner_id"], file["status"]) == ("partner", partner_id, "received")
    assert (file["borrower_phone"], file["borrower_email"], file["property_state"]) == ("+18135550142", "dave@example.com", "FL")
    assert (file["purchase_price"], file["rehab_budget"], file["loan_amount"]) == (180000, 45000, 200000)
    submission = _rows(engine, "partner_submissions")[0]
    assert submission["file_id"] == file["id"] and submission["consent_checked"]
    assert submission["consent_text"] == CONSENT and submission["consented_at"]
    assert submission["ip_address"] and submission["user_agent"] == "PhoneBrowser"
    assert token not in submission["page_url"] and "<link>" in submission["page_url"]
    assert client.queued == [file["id"]]


def test_submission_without_the_consent_box_is_blocked(engine, session_factory, client) -> None:
    _, token = _partner(session_factory)
    response = client.post(f"/api/lending/partners/{token}/files", data={**FORM, "consent": ""})
    assert response.status_code == 400 and "consent box" in response.text
    assert _rows(engine, "loan_files") == [] and _rows(engine, "partner_submissions") == []
    assert client.queued == []


def test_honeypot_drops_the_submission_quietly(engine, session_factory, client) -> None:
    _, token = _partner(session_factory)
    response = client.post(f"/api/lending/partners/{token}/files", data={**FORM, "company_website": "spam.example"})
    assert response.status_code == 200 and _rows(engine, "loan_files") == []


@pytest.mark.parametrize("field,value,message", [
    ("borrower_phone", "12345", "phone"), ("property_state", "Florida", "two-letter"), ("product", "boat", "loan type"),
    ("credit_band", "great", "credit range"), ("loan_amount", "", "Loan amount"), ("borrower_email", "dave@", "email"),
])
def test_invalid_fields_are_shown_and_nothing_is_saved(engine, session_factory, client, field, value, message) -> None:
    _, token = _partner(session_factory)
    response = client.post(f"/api/lending/partners/{token}/files", data={**FORM, field: value})
    assert response.status_code == 400 and message in response.text
    assert 'value="412 Palm Ave"' in response.text  # what was typed is kept
    assert _rows(engine, "loan_files") == []


def test_uploads_are_stored_with_the_file(engine, session_factory, client, settings) -> None:
    _, token = _partner(session_factory)
    files = [("documents", ("contract.pdf", b"%PDF-1.4 contract", "application/pdf")),
             ("documents", ("front photo.jpg", b"\xff\xd8\xff photo", "image/jpeg"))]
    assert client.post(f"/api/lending/partners/{token}/files", data=FORM, files=files).status_code == 200
    uploads = _rows(engine, "partner_file_uploads")
    assert [u["original_name"] for u in uploads] == ["contract.pdf", "front_photo.jpg"]
    file_id = _rows(engine, "loan_files")[0]["id"]
    for upload in uploads:
        assert f"{file_id}" in upload["stored_path"] and open(upload["stored_path"], "rb").read()


@pytest.mark.parametrize("files,message", [
    ([("documents", ("run.exe", b"MZ", "application/octet-stream"))], "only PDF"),
    ([("documents", ("big.pdf", b"x" * (10 * 1024 * 1024 + 1), "application/pdf"))], "10 MB"),
    ([("documents", (f"p{i}.png", b"\x89PNG", "image/png")) for i in range(6)], "at most 5"),
])
def test_bad_uploads_are_refused_and_leave_nothing_behind(engine, session_factory, client, settings, files, message,
                                                          tmp_path) -> None:
    _, token = _partner(session_factory)
    response = client.post(f"/api/lending/partners/{token}/files", data=FORM, files=files)
    assert response.status_code == 400 and message in response.text
    assert _rows(engine, "loan_files") == [] and _rows(engine, "partner_file_uploads") == []
    upload_dir = tmp_path / "uploads"
    assert not upload_dir.exists() or not any(upload_dir.rglob("*.*"))


def test_dashboard_lists_only_that_partners_files(session_factory, client) -> None:
    _, maria = _partner(session_factory)
    _, other = _partner(session_factory, name="Ben Ortiz", email="ben@example.com")
    client.post(f"/api/lending/partners/{maria}/files", data=FORM)
    client.post(f"/api/lending/partners/{other}/files", data={**FORM, "borrower_last_name": "Nguyen"})
    page = client.get(f"/api/lending/partners/{maria}/status")
    assert page.status_code == 200 and "Reyes" in page.text and "Nguyen" not in page.text
    assert "In review" in page.text


def test_dashboard_shows_friendly_status_and_received_page_does_not_promise_email(engine, session_factory, client) -> None:
    _, token = _partner(session_factory)
    received = client.post(f"/api/lending/partners/{token}/files", data=FORM)
    assert "keep you updated" in received.text and "email you" not in received.text
    with session_factory() as db:
        partners.record_status_change(db, _rows(engine, "loan_files")[0]["id"], "declined", "ltv")
    page = client.get(f"/api/lending/partners/{token}/status")
    assert "Declined by a lender, looking at next option" in page.text and ">declined<" not in page.text
    assert 'class="badge badge-declined"' in page.text


def test_form_is_branded_formats_money_and_locks_submit(session_factory, client) -> None:
    _, token = _partner(session_factory)
    page = client.get(f"/api/lending/partners/{token}").text
    assert "Next Deal Lending" in page and "is not a lender" in page
    assert page.count('class="money"') == 4 and 'id="f-submit"' in page and "button.disabled = true" in page


# -- pipeline -------------------------------------------------------------------------------------

class Recorder:
    def __init__(self) -> None:
        self.emails: list[tuple[str, str, str]] = []
        self.texts: list[tuple[str, str]] = []
        self.slack: list = []
        self.pdfs: list[dict] = []


VERIFIED_BOX = LenderRules(key="test_lender", name="Test Lender", verified=True)


def _services(recorder: Recorder, tmp_path, fit: LenderFitResult | None = None,
              matrix: tuple = (VERIFIED_BOX,)) -> partner_pipeline.PipelineServices:
    # The live matrix has no verified lender with both loan limits yet, so the limits are supplied here.
    fit = fit or LenderFitResult(fitting=[LenderFit(lender_key="test_lender", lender_name="Test Lender")])

    def render(template, context):
        recorder.pdfs.append(context)
        return b"%PDF fake"

    return partner_pipeline.PipelineServices(
        email=lambda to, subject, body: recorder.emails.append((to, subject, body)),
        sms=lambda phone, body: recorder.texts.append((phone, body)),
        slack=recorder.slack.append, render_pdf=render, evaluate_fit=lambda borrower, loan, matrix: fit,
        fit_limits=lambda result, matrix: [FitLimits(100_000, 1_000_000)] if result.fitting else [],
        load_matrix=lambda: matrix,
        opt_outs=lambda phone, email: partner_pipeline.OptOuts(phone=False, email=False),
        text_window_open=lambda phone: True,
        upload_dir=str(tmp_path / "uploads"), prequal_pct=10,
    )


def _submit(session_factory, token: str, **overrides) -> int:
    with session_factory() as db:
        partner = partners.partner_for_token(db, token)
        data = partners.SubmissionInput.model_validate({k: v for k, v in {**FORM, **overrides}.items() if k != "consent"})
        return partners.save_submission(db, partner, data, [], upload_dir="unused",
                                        consent=partners.ConsentEvidence(CONSENT, "203.0.113.9", "/p/<link>", "ua"))


def test_new_file_is_scored_prequalified_held_and_announced(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path), session_factory)

    file = _rows(engine, "loan_files")[0]
    assert (file["status"], file["status_reason"]) == ("held", "awaiting router")
    assert '"fitting"' in file["lender_fit"]
    assert file["prequal_pdf_path"] and open(file["prequal_pdf_path"], "rb").read() == b"%PDF fake"
    assert len(recorder.pdfs) == 1  # one letter rendered for the file
    note = recorder.slack[0]
    assert len(recorder.slack) == 1 and f"#{file_id}" in note.text and "Reyes" in note.text
    assert note.blocks[0]["text"]["text"].startswith(f"*New partner file  #{file_id}*")
    fields = [field["text"] for field in note.blocks[1]["fields"]]
    assert fields[0] == "*Loan*\nFix and flip · $200,000" and fields[3] == "*Lender fit*\n1 fit · 0 possible"
    assert [email[0] for email in recorder.emails] == ["maria@example.com"]
    assert "412 Palm Ave" in recorder.emails[0][1]

    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path), session_factory)  # a retry
    assert len(recorder.slack) == 1 and len(recorder.emails) == 1


def test_the_borrower_is_never_contacted(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory, sms_consent=True)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path), session_factory)
    borrower = {"dave@example.com", "+18135550142"}
    assert all(email[0] not in borrower for email in recorder.emails)
    assert all(phone not in borrower for phone, _ in recorder.texts)
    assert [phone for phone, _ in recorder.texts] == ["+17275550199"]  # the partner, who agreed to texts


def test_other_product_is_held_without_a_prequal(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token, product="other")
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path), session_factory)
    file = _rows(engine, "loan_files")[0]
    assert file["status"] == "held" and file["lender_fit"] is None and file["prequal_pdf_path"] is None
    assert recorder.pdfs == [] and len(recorder.emails) == 1


def test_no_fitting_lender_means_no_prequal_pdf(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path, fit=LenderFitResult()), session_factory)
    assert _rows(engine, "loan_files")[0]["prequal_pdf_path"] is None and recorder.pdfs == []


def test_no_prequal_pdf_while_no_lender_is_verified(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    unverified = (LenderRules(key="test_lender", name="Test Lender", verified=False),)
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path, matrix=unverified), session_factory)
    assert _rows(engine, "loan_files")[0]["prequal_pdf_path"] is None and recorder.pdfs == []


def test_closed_credit_band_sends_its_upper_bound(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token, credit_band="640-679")
    seen = []
    services = dataclasses.replace(_services(Recorder(), tmp_path),
                                   evaluate_fit=lambda borrower, loan, matrix: seen.append(borrower) or LenderFitResult())
    partner_pipeline.process_new_file(file_id, services, session_factory)
    assert (seen[0].credit_band_min_fico, seen[0].credit_band_max_fico) == (640, 679)


def test_pipeline_failure_never_raises(session_factory, tmp_path) -> None:
    def broken():
        raise RuntimeError("db down")

    partner_pipeline.process_new_file(1, _services(Recorder(), tmp_path), broken)


# -- status and attribution -----------------------------------------------------------------------

def test_status_changes_reach_the_partner(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    with session_factory() as db:
        assert partners.record_status_change(db, file_id, "held", "awaiting router") is None  # internal status
        change = partners.record_status_change(db, file_id, "declined", "ltv")
        assert partners.record_status_change(db, file_id, "declined", "ltv") is None  # no change, no notice
    assert change.status == "declined" and change.partner_id
    recorder = Recorder()
    assert partner_pipeline.notify_partner_status(change, _services(recorder, tmp_path), session_factory)
    assert recorder.emails[0][0] == "maria@example.com" and "declined" in recorder.emails[0][2]
    assert recorder.texts == []  # partner did not agree to texts
    with pytest.raises(ValueError):
        with session_factory() as db:
            partners.record_status_change(db, file_id, "lost")


def test_attribution_report_counts_funded_files_per_partner(session_factory) -> None:
    _, token_a = _partner(session_factory, name="Partner A", email="a@example.com")
    _, token_b = _partner(session_factory, name="Partner B", email="b@example.com")
    first_a, _second_a = _submit(session_factory, token_a), _submit(session_factory, token_a)
    _submit(session_factory, token_b)
    now = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
    with session_factory() as db:
        partners.record_status_change(db, first_a, "funded", now=now)
        report = partners.funded_by_partner(db, date(2026, 10, 1), date(2026, 11, 1))
        last_month = partners.funded_by_partner(db, date(2026, 9, 1), date(2026, 10, 1))
    assert report == [("Partner A", 1), ("Partner B", 0)]
    assert dict(last_month) == {"Partner A": 0, "Partner B": 0}


def test_partner_who_agreed_to_texts_gets_the_email_and_a_text(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory, sms_consent=True)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path), session_factory)
    assert [phone for phone, _ in recorder.texts] == ["+17275550199"]
    assert [email[0] for email in recorder.emails] == ["maria@example.com"]


def test_failed_text_falls_back_to_email(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory, sms_consent=True)
    file_id = _submit(session_factory, token)
    recorder = Recorder()

    def broken_sms(phone, body):
        raise RuntimeError("provider down")

    services = dataclasses.replace(_services(recorder, tmp_path), sms=broken_sms)
    partner_pipeline.process_new_file(file_id, services, session_factory)
    assert [email[0] for email in recorder.emails] == ["maria@example.com"]


@pytest.mark.parametrize("override", [
    {"opt_outs": lambda phone, email: partner_pipeline.OptOuts(phone=True, email=False)},  # partner texted STOP
    {"text_window_open": lambda phone: False},  # night time for the partner
])
def test_opted_out_number_or_closed_window_gets_email_only(engine, session_factory, tmp_path, override) -> None:
    _, token = _partner(session_factory, sms_consent=True)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(recorder, tmp_path), **override),
                                      session_factory)
    assert recorder.texts == [] and [email[0] for email in recorder.emails] == ["maria@example.com"]


def test_opted_out_email_is_never_emailed(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    services = dataclasses.replace(_services(recorder, tmp_path),
                                   opt_outs=lambda phone, email: partner_pipeline.OptOuts(phone=False, email=True))
    partner_pipeline.process_new_file(file_id, services, session_factory)
    assert recorder.emails == [] and recorder.texts == []
    row = _rows(engine, "partner_submissions")[0]
    assert row["partner_emailed_at"] is None and row["partner_confirmed_at"] is not None  # nothing left to retry


def test_failing_opt_out_check_blocks_the_send(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()

    def broken(phone, email):
        raise RuntimeError("db down")

    partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(recorder, tmp_path), opt_outs=broken),
                                      session_factory)
    assert recorder.emails == [] and recorder.texts == []
    assert _rows(engine, "partner_submissions")[0]["partner_confirmed_at"] is None  # retried by the sweep


def test_revoked_page_is_plain(session_factory, client) -> None:
    partner_id, token = _partner(session_factory)
    with session_factory() as db:
        partners.revoke_link(db, partner_id)
    page = client.get(f"/api/lending/partners/{token}")
    assert page.status_code == 404 and "This link is no longer active." in page.text
    assert "<style" not in page.text and "Next Deal Lending" not in page.text


def test_upload_error_is_shown_as_its_user_message(session_factory, client) -> None:
    _, token = _partner(session_factory)
    page = client.post(f"/api/lending/partners/{token}/files", data=FORM,
                       files=[("documents", ("virus.exe", b"MZ", "application/octet-stream"))])
    assert page.status_code == 400 and "virus.exe: only PDF, JPG, PNG or HEIC files can be attached" in page.text


def test_change_status_commits_and_tells_the_partner(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    change = partner_pipeline.change_status(file_id, "declined", "ltv", _services(recorder, tmp_path), session_factory)
    assert change is not None and _rows(engine, "loan_files")[0]["status"] == "declined"
    assert len(recorder.emails) == 1 and "declined" in recorder.emails[0][2]
    assert partner_pipeline.change_status(file_id, "held", "x", _services(recorder, tmp_path), session_factory) is None
    assert len(recorder.emails) == 1  # internal status: no notice


def test_funded_file_without_funded_at_still_counts(engine, session_factory) -> None:
    _, token = _partner(session_factory, name="Partner A")
    file_id = _submit(session_factory, token)
    with engine.begin() as conn:  # status moved by something other than record_status_change
        conn.execute(text("UPDATE lending.loan_files SET status = 'funded', status_at = :at WHERE id = :id"),
                     {"at": datetime(2026, 10, 5, tzinfo=timezone.utc), "id": file_id})
    with session_factory() as db:
        assert partners.funded_by_partner(db, date(2026, 10, 1), date(2026, 11, 1)) == [("Partner A", 1)]


def test_sweep_finds_only_files_whose_run_never_finished(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    stuck, done = _submit(session_factory, token), _submit(session_factory, token)
    partner_pipeline.process_new_file(done, _services(Recorder(), tmp_path), session_factory)
    later = datetime.now(timezone.utc) + timedelta(minutes=30)
    with session_factory() as db:
        assert partner_pipeline.unprocessed_file_ids(db, 10, 20, now=later) == [stuck]
        assert partner_pipeline.unprocessed_file_ids(db, 10, 20) == []  # too fresh to call stuck


def test_month_bounds() -> None:
    assert partners._month_bounds("2026-12") == (date(2026, 12, 1), date(2027, 1, 1))
    assert partners._month_bounds(None, today=date(2026, 10, 12)) == (date(2026, 10, 1), date(2026, 11, 1))


# -- schema ---------------------------------------------------------------------------------------

class _Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, statement, *args):
        self.sql.append(str(statement))


@pytest.mark.parametrize("model", [LendingPartner, LendingLoanFile, LendingPartnerSubmission, LendingPartnerFileUpload])
def test_migration_and_models_have_the_same_columns(model) -> None:
    recorder = _Recorder()
    apply_to(recorder)
    ddl = next(sql for sql in recorder.sql if f'"lending".{model.__tablename__} (' in sql)
    body = ddl.split("(", 1)[1]
    ddl_columns = {line.split()[0] for line in body.splitlines() if re.match(r"^\s+[a-z_]+\s", line)}
    assert ddl_columns == {column.name for column in model.__table__.columns}


def test_migration_is_rerunnable() -> None:
    recorder = _Recorder()
    apply_to(recorder)
    assert all("IF NOT EXISTS" in sql for sql in recorder.sql)


def test_money_parses_to_whole_dollars() -> None:
    data = partners.SubmissionInput.model_validate({k: v for k, v in FORM.items() if k != "consent"})
    assert data.purchase_price == 180000 and isinstance(data.loan_amount, int)
    assert Decimal(data.loan_amount) == Decimal(200000)


def test_a_router_crash_does_not_stop_the_confirmation_and_josh_note(engine, session_factory, tmp_path, monkeypatch) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)

    def crash(*args, **kwargs):
        raise RuntimeError("router down")

    monkeypatch.setattr(partner_pipeline, "route_file", crash)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _services(recorder, tmp_path), session_factory)

    file = _rows(engine, "loan_files")[0]
    assert file["status"] == "received" and '"fitting"' in file["lender_fit"]  # scoring kept; the sweep heals the status
    assert len(recorder.slack) == 1 and len(recorder.emails) == 1


def test_sweep_retries_the_slack_card_when_slack_failed_after_routing(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    services = _services(recorder, tmp_path)

    def slack_down(note):
        raise RuntimeError("slack down")

    partner_pipeline.process_new_file(file_id, dataclasses.replace(services, slack=slack_down), session_factory)
    assert _rows(engine, "loan_files")[0]["status"] == "held"
    assert _rows(engine, "partner_submissions")[0]["josh_notified_at"] is None
    later = datetime.now(timezone.utc) + timedelta(minutes=30)
    with session_factory() as db:
        assert partner_pipeline.unprocessed_file_ids(db, 10, 20, now=later) == []
        assert partner_pipeline.unnotified_file_ids(db, 10, 24, 20, now=later) == [file_id]
    partner_pipeline.send_pending_notices(file_id, services, session_factory)
    assert len(recorder.slack) == 1 and _rows(engine, "partner_submissions")[0]["josh_notified_at"] is not None
    assert len(recorder.emails) == 1  # partner confirmation was already sent; not repeated
    with session_factory() as db:
        assert partner_pipeline.unnotified_file_ids(db, 10, 24, 20, now=later) == []
        assert partner_pipeline.unnotified_file_ids(db, 10, 24, 20, now=later + timedelta(days=2)) == []


def test_editing_the_reason_on_a_funded_file_keeps_its_funded_date(engine, session_factory) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    first = datetime(2026, 10, 5, tzinfo=timezone.utc)
    with session_factory() as db:
        partners.record_status_change(db, file_id, "funded", now=first)
        partners.record_status_change(db, file_id, "funded", "wire confirmed", now=datetime(2026, 11, 2, tzinfo=timezone.utc))
        assert partners.funded_by_partner(db, date(2026, 10, 1), date(2026, 11, 1)) == [("Maria Lopez", 1)]
    assert str(_rows(engine, "loan_files")[0]["funded_at"]).startswith("2026-10-05")


# -- pre-qual letter to the partner -------------------------------------------------------------------

def _letter_services(recorder: Recorder, tmp_path, *, letter=None):
    recorder.letters = []

    def default_letter(partner, file, pdf):
        recorder.letters.append((partner.email, file["id"], pdf))

    return dataclasses.replace(_services(recorder, tmp_path), send_letter=letter or default_letter)


def _submission(engine) -> dict:
    return _rows(engine, "partner_submissions")[0]


def test_the_letter_goes_to_the_partner_once(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    services = _letter_services(recorder, tmp_path)
    partner_pipeline.process_new_file(file_id, services, session_factory)

    assert recorder.letters == [("maria@example.com", file_id, b"%PDF fake")]
    assert _submission(engine)["prequal_sent_at"]

    partner_pipeline.send_pending_notices(file_id, services, session_factory)
    assert len(recorder.letters) == 1


def test_no_letter_without_a_partner_email(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory, email=None)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, _letter_services(recorder, tmp_path), session_factory)
    assert recorder.letters == [] and _submission(engine)["prequal_sent_at"] is None
    later = datetime.now(timezone.utc) + timedelta(minutes=30)
    with session_factory() as db:  # the missing letter adds no retry: there is no email to send it to
        assert (partner_pipeline.unnotified_file_ids(db, 10, 24, 20, now=later, letters=True)
                == partner_pipeline.unnotified_file_ids(db, 10, 24, 20, now=later))


def test_letter_with_unknown_outcome_is_never_sent_twice(engine, session_factory, tmp_path) -> None:
    from src.lending.prequal_ghl import SendOutcomeUnknown

    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()

    def timed_out(partner, file, pdf):
        recorder.letters.append(file["id"])
        raise SendOutcomeUnknown("prequal email send: ReadTimeout")

    services = _letter_services(recorder, tmp_path, letter=timed_out)
    partner_pipeline.process_new_file(file_id, services, session_factory)
    partner_pipeline.send_pending_notices(file_id, services, session_factory)
    assert recorder.letters == [file_id] and _submission(engine)["prequal_sent_at"] is not None


class _FakeGhl:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def __call__(self, step, method, path, **kwargs):
        self.calls.append((method, path, kwargs.get("json") or kwargs.get("params") or {}))
        if path == "/contacts/upsert":
            return {"contact": {"id": "c-1"}}
        return {}


def test_ghl_sink_emails_the_letter_to_the_partner(monkeypatch) -> None:
    from types import SimpleNamespace

    from src.lending import partner_ghl

    sink = partner_ghl.PartnerLetterSink(SimpleNamespace(location_id="loc", api_key="k"))
    fake, sent = _FakeGhl(), []
    monkeypatch.setattr(sink, "_call", fake)
    monkeypatch.setattr(sink, "_upload", lambda contact_id, pdf: f"https://files/{contact_id}.pdf")
    monkeypatch.setattr(sink, "_send_email", sent.append)
    sink.send_letter("maria@example.com", "Maria Lopez", "412 Palm Ave", 7, b"%PDF")

    assert fake.calls[0][2]["email"] == "maria@example.com"
    assert sent[0]["contactId"] == "c-1" and sent[0]["attachments"] == ["https://files/c-1.pdf"]
    assert sent[0]["subject"] == "Pre-qualification estimate for 412 Palm Ave (ref #7)"
    assert "non-binding" in sent[0]["html"] and "share it with the borrower" in sent[0]["html"]


# -- review: per-channel retries, status notice retries, Postgres jsonb ---------------------------------

def test_failed_email_is_retried_without_resending_the_text(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory, sms_consent=True)
    file_id = _submit(session_factory, token)
    recorder = Recorder()

    def email_down(to, subject, body):
        raise RuntimeError("email down")

    partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(recorder, tmp_path), email=email_down),
                                      session_factory)
    row = _rows(engine, "partner_submissions")[0]
    assert len(recorder.texts) == 1 and row["partner_texted_at"] and row["partner_confirmed_at"] is None
    partner_pipeline.send_pending_notices(file_id, _services(recorder, tmp_path), session_factory)
    row = _rows(engine, "partner_submissions")[0]
    assert len(recorder.emails) == 1 and len(recorder.texts) == 1  # email recovered, text not repeated
    assert row["partner_emailed_at"] and row["partner_confirmed_at"]


def test_failed_status_notice_is_retried_without_changing_the_status(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()

    def email_down(to, subject, body):
        raise RuntimeError("email down")

    partner_pipeline.change_status(file_id, "declined", "ltv",
                                   dataclasses.replace(_services(recorder, tmp_path), email=email_down), session_factory)
    assert _rows(engine, "loan_files")[0]["notice_status"] == "declined"
    later = datetime.now(timezone.utc) + timedelta(minutes=30)
    with session_factory() as db:
        assert partner_pipeline.pending_status_notice_ids(db, 10, 24, 20, now=later) == [file_id]
    assert partner_pipeline.retry_status_notice(file_id, _services(recorder, tmp_path), session_factory)
    assert len(recorder.emails) == 1 and "declined" in recorder.emails[0][2]
    file = _rows(engine, "loan_files")[0]
    assert file["status"] == "declined" and file["notice_status"] is None
    with session_factory() as db:
        assert partner_pipeline.pending_status_notice_ids(db, 10, 24, 20, now=later) == []
    assert not partner_pipeline.retry_status_notice(file_id, _services(recorder, tmp_path), session_factory)
    assert len(recorder.emails) == 1


def _email_down(to, subject, body):
    raise RuntimeError("email down")


def test_unsent_confirmation_is_flagged_once_after_the_retry_window(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(recorder, tmp_path), email=_email_down),
                                      session_factory)
    assert _rows(engine, "partner_submissions")[0]["partner_confirmed_at"] is None
    alerts_before = len(recorder.slack)
    now = datetime.now(timezone.utc)

    assert partner_pipeline.flag_lapsed_notices(24, _services(recorder, tmp_path), session_factory,
                                                now=now + timedelta(hours=23)) == []
    lapsed = partner_pipeline.flag_lapsed_notices(24, _services(recorder, tmp_path), session_factory,
                                                  now=now + timedelta(hours=25))
    assert [(n.file_id, n.what, n.partner_name) for n in lapsed] == [(file_id, "file received confirmation", "Maria Lopez")]
    assert len(recorder.slack) == alerts_before + 1 and "not delivered" in recorder.slack[-1].text
    assert _rows(engine, "partner_submissions")[0]["confirm_failed_at"] is not None
    assert partner_pipeline.flag_lapsed_notices(24, _services(recorder, tmp_path), session_factory,
                                                now=now + timedelta(hours=26)) == []
    assert len(recorder.slack) == alerts_before + 1


def test_unsent_status_notice_is_flagged_and_a_new_status_clears_the_flag(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    with session_factory() as db:
        db.execute(text("UPDATE lending.partner_submissions SET partner_confirmed_at = :now"),
                   {"now": datetime.now(timezone.utc)})
    partner_pipeline.change_status(file_id, "declined", "ltv",
                                   dataclasses.replace(_services(recorder, tmp_path), email=_email_down), session_factory)
    later = datetime.now(timezone.utc) + timedelta(hours=25)

    lapsed = partner_pipeline.flag_lapsed_notices(24, _services(recorder, tmp_path), session_factory, now=later)
    assert [(n.file_id, n.what) for n in lapsed] == [(file_id, "declined update")]
    file = _rows(engine, "loan_files")[0]
    assert file["notice_failed_at"] is not None and file["notice_status"] == "declined"
    with session_factory() as db:
        partners.record_status_change(db, file_id, "routed")
    assert _rows(engine, "loan_files")[0]["notice_failed_at"] is None


def test_lapsed_notices_are_not_stamped_when_slack_is_not_configured(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder = Recorder()
    partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(recorder, tmp_path), email=_email_down),
                                      session_factory)
    no_slack = dataclasses.replace(_services(recorder, tmp_path), slack=None)
    later = datetime.now(timezone.utc) + timedelta(hours=25)
    assert partner_pipeline.flag_lapsed_notices(24, no_slack, session_factory, now=later) == []
    assert _rows(engine, "partner_submissions")[0]["confirm_failed_at"] is None


def test_lapsed_notices_are_not_stamped_when_the_live_partner_channel_is_unset(engine, session_factory, settings,
                                                                               tmp_path, monkeypatch) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(Recorder(), tmp_path), email=_email_down),
                                      session_factory)
    monkeypatch.setattr(settings, "lending_partner_channel", None)
    later = datetime.now(timezone.utc) + timedelta(hours=25)
    assert partner_pipeline.flag_lapsed_notices(24, None, session_factory, now=later) == []
    assert _rows(engine, "partner_submissions")[0]["confirm_failed_at"] is None


def test_lapsed_notices_are_flagged_in_batches(engine, session_factory, tmp_path) -> None:
    _, token = _partner(session_factory)
    file_ids = [_submit(session_factory, token) for _ in range(3)]
    recorder = Recorder()
    for file_id in file_ids:
        partner_pipeline.process_new_file(file_id, dataclasses.replace(_services(recorder, tmp_path), email=_email_down),
                                          session_factory)
    later = datetime.now(timezone.utc) + timedelta(hours=25)
    first = partner_pipeline.flag_lapsed_notices(24, _services(recorder, tmp_path), session_factory, now=later, limit=2)
    second = partner_pipeline.flag_lapsed_notices(24, _services(recorder, tmp_path), session_factory, now=later, limit=2)
    assert len(first) == 2 and len(second) == 1
    assert {n.file_id for n in first + second} == set(file_ids)


def test_internal_status_clears_a_queued_notice(engine, session_factory) -> None:
    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    with session_factory() as db:
        partners.record_status_change(db, file_id, "routed")
        assert _rows(engine, "loan_files")[0]["notice_status"] == "routed"
        partners.record_status_change(db, file_id, "held", "back to router")
    assert _rows(engine, "loan_files")[0]["notice_status"] is None


@pytest.mark.parametrize("stored", ["json", "dict"])
def test_stored_fit_reads_sqlite_text_and_postgres_jsonb(stored) -> None:
    fit = LenderFitResult(fitting=[LenderFit(lender_key="a", lender_name="A")])
    value = fit.model_dump_json() if stored == "json" else fit.model_dump(mode="json")
    assert partner_pipeline._stored_fit(value) == fit
    assert partner_pipeline._stored_fit(None) is None


@pytest.mark.parametrize("ambiguous, retried", [(True, False), (False, True)])
def test_status_notice_with_unknown_outcome_is_not_resent(engine, session_factory, tmp_path, ambiguous, retried) -> None:
    from src.lending.ghl_sms import GhlSmsError

    _, token = _partner(session_factory)
    file_id = _submit(session_factory, token)
    recorder, attempts = Recorder(), []

    def flaky_email(to, subject, body):
        attempts.append(to)
        raise GhlSmsError("GHL message send returned no message id", ambiguous=ambiguous)

    partner_pipeline.change_status(file_id, "approved", None,
                                   dataclasses.replace(_services(recorder, tmp_path), email=flaky_email), session_factory)
    later = datetime.now(timezone.utc) + timedelta(minutes=30)
    with session_factory() as db:
        pending = partner_pipeline.pending_status_notice_ids(db, 10, 24, 20, now=later)
    assert (pending == [file_id]) is retried
    partner_pipeline.retry_status_notice(file_id, dataclasses.replace(_services(recorder, tmp_path), email=flaky_email),
                                         session_factory)
    assert len(attempts) == (2 if retried else 1)
