"""P2.4: the borrower's secure document upload page."""
from __future__ import annotations

import os
import stat
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.lending.closing.doc_chase import issue_link, start_chase
from src.lending.closing.drafts import Recipient

PDF = b"%PDF-1.4 test"


@pytest.fixture
def setup(closing_db, monkeypatch, tmp_path):
    from config.settings import get_settings
    from src.api import lending_doc_upload_router
    from src.lending.db import get_lending_db
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_partner_link_base_url", "https://api.example.com", raising=False)
    monkeypatch.setattr(settings, "lending_doc_upload_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(lending_doc_upload_router, "enforce_or_429", lambda *args, **kwargs: None)
    events = []
    monkeypatch.setattr("src.lending.closing.doc_chase.add_to_inbox_safely", lambda db, **e: events.append(e))
    now = datetime.now(timezone.utc)
    chase_id = start_chase(closing_db, Recipient("Mark", "+18135550101", None, None), lead_ref="web_lead:7",
                           accepted_by="slack:U1", now=now).chase_id
    token = issue_link(closing_db, chase_id, now).rsplit("/", 1)[1]
    app = FastAPI()
    app.include_router(lending_doc_upload_router.router)
    app.dependency_overrides[get_lending_db] = lambda: closing_db
    return TestClient(app), token, chase_id, tmp_path, events


def _upload(client, token, item="government_id", name="id.pdf", data=PDF, content_type="application/pdf"):
    return client.post(f"/api/lending/docs/{token}/upload", data={"item_code": item},
                       files={"document": (name, data, content_type)}, follow_redirects=False)


def test_page_lists_missing_items(setup):
    client, token, *_ = setup
    page = client.get(f"/api/lending/docs/{token}")
    assert page.status_code == 200 and "Photo ID for each guarantor" in page.text and "Hi Mark" in page.text


def test_upload_marks_item_received_and_stores_generated_name(setup, closing_db):
    client, token, chase_id, tmp_path, events = setup
    response = _upload(client, token, name="../../evil.pdf")
    assert response.status_code == 303 and response.headers["location"] == f"/api/lending/docs/{token}"
    stored = closing_db.execute(text("SELECT stored_path, original_name, size_bytes FROM lending.doc_chase_uploads")).one()
    assert stored.original_name == "../../evil.pdf" and stored.size_bytes == len(PDF)
    path = tmp_path / str(chase_id) / os.path.basename(stored.stored_path)
    assert stored.stored_path == str(path) and path.suffix == ".pdf" and path.read_bytes() == PDF
    assert stat.S_IMODE(path.stat().st_mode) == 0o600 and stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert closing_db.execute(text("SELECT received_at IS NOT NULL FROM lending.doc_chase_items "
                                   "WHERE item_code = 'government_id'")).scalar()
    assert events[-1]["event_type"] == "doc_chase_upload" and "1 of 6" in events[-1]["summary"]
    assert "Photo ID" in client.get(f"/api/lending/docs/{token}").text  # shown as received


def test_wrong_type_and_oversize_refused_400(setup, closing_db):
    client, token, *_ = setup
    assert _upload(client, token, name="x.exe", content_type="application/octet-stream").status_code == 400
    assert _upload(client, token, data=b"0" * (10 * 1024 * 1024 + 1)).status_code == 400
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chase_uploads")).scalar() == 0


def test_item_code_not_on_checklist_refused(setup):
    client, token, *_ = setup
    assert _upload(client, token, item="tax_returns").status_code == 400


def test_unknown_and_closed_chase_get_inactive_page(setup, closing_db):
    client, token, *_ = setup
    assert client.get("/api/lending/docs/not-a-token").status_code == 404
    closing_db.execute(text("UPDATE lending.doc_chases SET status = 'complete'"))
    assert client.get(f"/api/lending/docs/{token}").status_code == 404
    assert _upload(client, token).status_code == 404
