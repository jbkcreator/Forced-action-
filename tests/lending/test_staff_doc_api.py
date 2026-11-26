"""R1 0.2 staff document API: admin tokens only, caller tokens 403 everywhere, never guesses a borrower."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api import lending_doc_upload_router
from src.lending.db import get_lending_db
from src.lending.security import rbac
from src.lending.security.rbac import Role
from src.lending.security.staff_tokens import issue_token

SEARCH = "/api/lending/docs/staff/borrowers/search"


@pytest.fixture
def api(documents_db, monkeypatch):
    monkeypatch.setattr(lending_doc_upload_router, "enforce_or_429", lambda *args, **kwargs: None)
    roles_assumed = []
    monkeypatch.setattr(lending_doc_upload_router, "assume_caller_db_role", lambda db: roles_assumed.append(db))
    app = FastAPI()
    app.include_router(lending_doc_upload_router.router)
    app.dependency_overrides[get_lending_db] = lambda: documents_db.db
    now = datetime.now(timezone.utc)
    admin = issue_token(documents_db.db, "Josh", Role.ADMIN, "ops", now)
    caller = issue_token(documents_db.db, "David", Role.CALLER, "ops", now)
    smith = documents_db.add(1, "Mark Smith", "+18135550101", uploaded=("bank_statements",))
    documents_db.add(2, "Mark Taylor", "+18135554432", uploaded=("government_id",))
    return TestClient(app), {"Authorization": f"Bearer {admin}"}, {"Authorization": f"Bearer {caller}"}, smith, \
        documents_db, roles_assumed


def _upload_id(api_):
    client, admin, _, smith, *_ = api_
    listed = client.get(f"/api/lending/docs/staff/borrowers/{smith.lead_ref}/documents", headers=admin).json()
    items = {i["item_code"]: i for i in listed["chases"][0]["items"]}
    return items["bank_statements"]["uploads"][0]["upload_id"]


def test_no_or_bad_token_is_401(api):
    client, *_ = api
    for headers in ({}, {"Authorization": "Bearer ndl_wrong"}, {"Authorization": "Basic abc"}):
        response = client.post(SEARCH, json={"query": "mark"}, headers=headers)
        assert response.status_code == 401 and response.headers["www-authenticate"] == "Bearer"
        assert response.json() == {"detail": "invalid or expired token"}


def test_expired_token_is_401(api):
    client, _, _, _, docs, _ = api
    old = issue_token(docs.db, "Old", Role.ADMIN, "ops", datetime.now(timezone.utc) - timedelta(days=91))
    assert client.post(SEARCH, json={"query": "mark"}, headers={"Authorization": f"Bearer {old}"}).status_code == 401


def test_caller_token_403_on_every_endpoint_and_db_role_assumed(api):
    client, admin, caller, smith, docs, roles_assumed = api
    upload_id = _upload_id(api)
    responses = [client.post(SEARCH, json={"query": "mark"}, headers=caller),
                 client.get(f"/api/lending/docs/staff/borrowers/{smith.lead_ref}/documents", headers=caller),
                 client.post(f"/api/lending/docs/staff/documents/{upload_id}/view-link", headers=caller)]
    assert [r.status_code for r in responses] == [403, 403, 403]
    assert all(r.json() == {"detail": "not permitted"} for r in responses) and len(roles_assumed) == 3
    assert docs.db.execute(text("SELECT count(*) FROM lending.doc_view_links")).scalar() == 0


def test_caller_token_still_403_when_db_role_missing(api, monkeypatch):
    client, _, caller, *_ = api

    def unavailable(db):
        raise rbac.CallerDbRoleUnavailable("lending_caller")

    monkeypatch.setattr(lending_doc_upload_router, "assume_caller_db_role", unavailable)
    assert client.post(SEARCH, json={"query": "mark"}, headers=caller).status_code == 403


def test_admin_search_two_marks_returns_both_masked(api):
    client, admin, *_ = api
    body = client.post(SEARCH, json={"query": "mark"}, headers=admin).json()
    assert body["too_many"] is False and {m["borrower"] for m in body["matches"]} == {"Mark S. …0101", "Mark T. …4432"}
    assert "8135550101" not in str(body)


def test_admin_search_too_many_has_detail(api):
    client, admin, _, _, docs, _ = api
    for n in range(10):
        docs.add(100 + n, f"Mark Person{n}", f"+1813555{1000 + n}", uploaded=())
    body = client.post(SEARCH, json={"query": "mark"}, headers=admin).json()
    assert body["too_many"] is True and body["matches"] == [] and "last 4 digits" in body["detail"]


def test_admin_documents_list_without_paths(api):
    client, admin, _, smith, docs, _ = api
    response = client.get(f"/api/lending/docs/staff/borrowers/{smith.lead_ref}/documents", headers=admin)
    assert response.status_code == 200 and str(docs.tmp_path) not in response.text
    assert client.get("/api/lending/docs/staff/borrowers/web_lead:999/documents", headers=admin).status_code == 404
    assert client.get("/api/lending/docs/staff/borrowers/nope/documents", headers=admin).status_code == 400


def test_admin_view_link_opens_then_expires(api, monkeypatch):
    client, admin, *_ = api
    response = client.post(f"/api/lending/docs/staff/documents/{_upload_id(api)}/view-link", headers=admin)
    assert response.status_code == 201
    path = response.json()["url"].replace("https://api.example.com", "")
    assert client.get(path).content == b"%PDF-1.4 bank_statements"
    later = datetime.now(timezone.utc) + timedelta(minutes=15, seconds=1)
    monkeypatch.setattr(lending_doc_upload_router, "_now", lambda: later)
    assert client.get(path).status_code == 410
    assert client.post("/api/lending/docs/staff/documents/987654/view-link", headers=admin).status_code == 404


def test_errors_never_leak_exception_text(api, monkeypatch):
    client, admin, *_ = api

    def boom(db, query):
        raise RuntimeError("SELECT secret FROM somewhere")

    monkeypatch.setattr(lending_doc_upload_router, "find_document_borrowers", boom)
    response = client.post(SEARCH, json={"query": "mark"}, headers=admin)
    assert response.status_code == 500 and response.json() == {"detail": "internal error"}
