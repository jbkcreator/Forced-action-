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
    monkeypatch.setattr(settings, "lending_doc_chase_sms_links_enabled", True, raising=False)  # phone-only borrower
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


def test_uploads_capped_per_document_and_inbox_not_flooded(setup, closing_db):
    client, token, _, _, events = setup
    statuses = [_upload(client, token).status_code for _ in range(6)]
    assert statuses == [303] * 5 + [400]
    assert len({e["dedup_key"] for e in events}) == 1


def test_upload_refused_when_the_upload_folder_is_not_absolute(setup, closing_db, monkeypatch):
    from config.settings import get_settings
    client, token, chase_id, _, _ = setup
    monkeypatch.setattr(get_settings(), "lending_doc_upload_dir", "data/doc_uploads", raising=False)
    response = _upload(client, token)
    assert response.status_code == 500 and "didn" in response.text
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chase_uploads")).scalar() == 0


def test_a_failed_commit_leaves_no_file_on_disk(setup, closing_db, monkeypatch):
    client, token, _, folder, _ = setup
    monkeypatch.setattr(closing_db, "commit", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    response = _upload(client, token)
    assert response.status_code == 500
    assert [f for f in folder.rglob("*") if f.is_file()] == []


@pytest.mark.parametrize("name,data,content_type", [
    ("id.pdf", b"MZ\x90\x00 not really a pdf", "application/pdf"),
    ("id.jpg", b"%PDF-1.4 a pdf named jpg", "image/jpeg"),
    ("id.png", b"\xff\xd8\xff a jpeg named png", "image/png"),
    ("id.heic", b"\x00\x00\x00\x18ftypmp42", "image/heic"),
])
def test_a_file_whose_content_does_not_match_its_type_is_refused(setup, closing_db, name, data, content_type):
    client, token, _, folder, _ = setup
    response = _upload(client, token, name=name, data=data, content_type=content_type)
    assert response.status_code == 400 and "not a real" in response.text
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chase_uploads")).scalar() == 0
    assert [f for f in folder.rglob("*") if f.is_file()] == []


@pytest.mark.parametrize("name,data,content_type", [
    ("id.jpg", b"\xff\xd8\xff\xe0 jpeg", "image/jpeg"),
    ("id.png", b"\x89PNG\r\n\x1a\n png", "image/png"),
    ("id.heic", b"\x00\x00\x00\x18ftypheic", "image/heic"),
])
def test_real_images_are_accepted(setup, name, data, content_type):
    client, token, _, _, _ = setup
    assert _upload(client, token, name=name, data=data, content_type=content_type).status_code == 303


def test_access_log_lines_never_carry_the_upload_token():
    import logging

    from src.api.lending_doc_upload_router import (
        UploadTokenLogFilter,
        redact_upload_tokens,
    )
    token = "Zk3n8QvR2xYp7LmA9cTb4WsE6uHf1JdG0oKiNqVrXyU"
    assert redact_upload_tokens(f"POST /api/lending/docs/{token}/upload") == "POST /api/lending/docs/[…]/upload"
    record = logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d', (
        "1.2.3.4:5", "GET", f"/api/lending/docs/{token}", "1.1", 200), None)
    UploadTokenLogFilter().filter(record)
    assert token not in record.getMessage() and "/api/lending/docs/[…]" in record.getMessage()


def test_the_lending_api_installs_the_access_log_filter():
    import logging

    import src.lending.api  # noqa: F401  (installs the filters on import)
    from src.api.lending_doc_upload_router import UploadTokenLogFilter
    for name in ("uvicorn.access", "gunicorn.access"):
        assert any(isinstance(f, UploadTokenLogFilter) for f in logging.getLogger(name).filters)


def test_nginx_example_turns_off_access_logs_and_sets_the_body_limit_for_uploads():
    from pathlib import Path
    conf = (Path(__file__).resolve().parents[2] / "deploy" / "nginx" / "lending-api.conf.example").read_text()
    block = conf.split("location /api/lending/docs/ {", 1)[1].split("}", 1)[0]
    assert "access_log off;" in block and "client_max_body_size 11m;" in block
