"""R1 0.2: GET /api/lending/docs/view/{token}, an admin's 15-minute link to one stored document."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from config.settings import get_settings
from src.api import lending_doc_upload_router
from src.api.lending_doc_upload_router import redact_upload_tokens
from src.lending.closing.doc_access import issue_view_link, latest_upload, revoke_links_of_removed_approvers
from src.lending.db import get_lending_db
from src.lending.security.rbac import Role
from src.lending.security.staff_tokens import issue_token, revoke_token, token_for


@pytest.fixture
def view(documents_db, monkeypatch):
    monkeypatch.setattr(lending_doc_upload_router, "enforce_or_429", lambda *args, **kwargs: None)
    app = FastAPI()
    app.include_router(lending_doc_upload_router.router)
    app.dependency_overrides[get_lending_db] = lambda: documents_db.db
    mark = documents_db.add(1, "Mark Smith", "+18135550101")
    upload_id = latest_upload(documents_db.db, mark.lead_ref, "bank_statements")[0]

    def link(issued_to="slack:U_JOSH", at=None):
        url = issue_view_link(documents_db.db, upload_id, issued_to, at or datetime.now(timezone.utc)).url
        return url.replace("https://api.example.com", "")

    return TestClient(app), link, mark, documents_db


def test_admin_link_serves_file_inline_no_store(view):
    client, link, mark, _ = view
    response = client.get(link())
    assert response.status_code == 200 and response.content == b"%PDF-1.4 bank_statements"
    assert response.headers["cache-control"] == "no-store, private"
    assert response.headers["content-disposition"].startswith("inline") and "bank_statements.pdf" in response.headers["content-disposition"]
    assert response.headers["x-content-type-options"] == "nosniff" and response.headers["referrer-policy"] == "no-referrer"


def test_expired_link_is_410(view):
    client, link, _, docs = view
    response = client.get(link(at=datetime.now(timezone.utc) - timedelta(minutes=16)))
    assert response.status_code == 410 and "expired" in response.text and str(docs.tmp_path) not in response.text


def test_unknown_token_is_403(view):
    client, *_ = view
    response = client.get("/api/lending/docs/view/not-a-real-token")
    assert response.status_code == 403 and "not valid" in response.text


def test_removed_approver_link_is_403_even_while_lending_api_still_lists_them(view):
    """PR #478 review finding 1: lending-api's settings are cached until it restarts, so its approver list can be
    stale. Here it still lists U_JOSH; Cora, restarted without him, revokes his link on its sweep -> 403 at once."""
    client, link, _, docs = view
    url = link()
    assert client.get(url).status_code == 200
    assert "U_JOSH" in get_settings().lending_cora_approver_user_ids  # lending-api's stale copy
    assert revoke_links_of_removed_approvers(docs.db, frozenset({"U_SOMEONE_ELSE"}), datetime.now(timezone.utc)) == 1
    assert client.get(url).status_code == 403


def test_new_approver_link_opens_before_lending_api_restarts(view, monkeypatch):
    """The other side of the same cache: Cora issued it to a new approver; lending-api's old list must not block it."""
    client, link, *_ = view
    url = link()
    monkeypatch.setattr(get_settings(), "lending_cora_approver_user_ids", "", raising=False)  # lending-api's old list
    assert client.get(url).status_code == 200


def test_link_from_revoked_token_gets_403(view):
    client, link, _, docs = view
    token = token_for(docs.db, issue_token(docs.db, "Josh", Role.ADMIN, "ops", datetime.now(timezone.utc)),
                      datetime.now(timezone.utc))
    url = link(issued_to=f"token:{token.id}")
    assert client.get(url).status_code == 200
    revoke_token(docs.db, token.id, datetime.now(timezone.utc))
    assert client.get(url).status_code == 403


def test_missing_file_returns_404_page_without_path(view):
    client, link, mark, docs = view
    url = link()
    os.remove(mark.paths["bank_statements"])
    response = client.get(url)
    assert response.status_code == 404 and str(docs.tmp_path) not in response.text


def test_redaction_hides_view_and_upload_tokens_not_staff_paths():
    assert redact_upload_tokens("GET /api/lending/docs/view/abc123 HTTP/1.1") == "GET /api/lending/docs/view/[…] HTTP/1.1"
    assert redact_upload_tokens("/api/lending/docs/abc123/upload") == "/api/lending/docs/[…]/upload"
    assert redact_upload_tokens("/api/lending/docs/staff/borrowers/search") == "/api/lending/docs/staff/borrowers/search"
