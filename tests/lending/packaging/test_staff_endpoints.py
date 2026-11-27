"""The staff document / package endpoints, with the auth and database dependencies replaced by fakes."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api import lending_doc_upload_router as router_module
from src.lending.db import get_lending_db
from src.lending.packaging import actions
from tests.lending.packaging.fakes import FakeSession


@pytest.fixture
def client():
    db = FakeSession()
    app = FastAPI()
    app.include_router(router_module.router)
    app.dependency_overrides[get_lending_db] = lambda: db
    app.dependency_overrides[router_module.staff_admin] = lambda: SimpleNamespace(id=4)
    return TestClient(app), db


def test_tagging_passes_the_staff_token_as_a_ledger_actor(client, monkeypatch):
    http, db = client
    seen = {}
    monkeypatch.setattr(router_module, "tag_document_type",
                        lambda db, uid, code, *, actor: seen.update(uid=uid, code=code, actor=actor))
    response = http.post("/api/lending/docs/staff/documents/9/type", json={"item_code": "pricing_sheet"})
    assert response.status_code == 200 and seen == {"uid": 9, "code": "pricing_sheet", "actor": "human:staff_token:4"}
    assert db.commits == 1


def test_a_refused_action_is_a_409_with_the_reason(client, monkeypatch):
    http, _ = client

    def refuse(*a, **k):
        raise actions.ActionRefused("Unknown document type 'selfie'.")

    monkeypatch.setattr(router_module, "tag_document_type", refuse)
    response = http.post("/api/lending/docs/staff/documents/9/type", json={"item_code": "selfie"})
    assert response.status_code == 409 and "selfie" in response.json()["detail"]


def test_override_endpoint(client, monkeypatch):
    http, _ = client
    seen = {}

    def override(db, package_id, table, upload_id, *, reason, actor, today):
        seen.update(package_id=package_id, table=table, upload_id=upload_id, reason=reason, actor=actor)
        return SimpleNamespace(status="draft")

    monkeypatch.setattr(router_module, "override_block", override)
    response = http.post("/api/lending/docs/staff/packages/11/override",
                         json={"upload_table": "doc_chase_uploads", "upload_id": 3, "reason": "joint account"})
    assert response.status_code == 200 and response.json()["status"] == "draft"
    assert seen["actor"] == "human:staff_token:4" and seen["upload_id"] == 3


def test_release_endpoint_only_takes_upload_tables(client, monkeypatch):
    http, _ = client
    released = []
    monkeypatch.setattr(router_module, "release_document",
                        lambda db, table, uid, *, reason, actor: released.append((table, uid, actor)))
    assert http.post("/api/lending/docs/staff/documents/users/1/release", json={"reason": "fine"}).status_code == 404
    ok = http.post("/api/lending/docs/staff/documents/partner_file_uploads/4/release", json={"reason": "checked"})
    assert ok.status_code == 200 and released == [("partner_file_uploads", 4, "human:staff_token:4")]


def test_supersede_endpoint_passes_both_documents_and_the_staff_actor(client, monkeypatch):
    http, db = client
    seen = {}
    monkeypatch.setattr(router_module, "supersede_document",
                        lambda db, t, u, bt, bu, *, actor: seen.update(old=(t, u), new=(bt, bu), actor=actor))
    response = http.post("/api/lending/docs/staff/documents/doc_chase_uploads/3/superseded",
                         json={"by_table": "doc_chase_uploads", "by_id": 8})
    assert response.status_code == 200 and db.commits == 1
    assert seen == {"old": ("doc_chase_uploads", 3), "new": ("doc_chase_uploads", 8), "actor": "human:staff_token:4"}
    assert http.post("/api/lending/docs/staff/documents/users/3/superseded",
                     json={"by_table": "doc_chase_uploads", "by_id": 8}).status_code == 404


def test_guarantor_endpoint_records_the_name(client, monkeypatch):
    http, db = client
    seen = {}
    monkeypatch.setattr(router_module, "add_guarantor",
                        lambda db, fid, name, *, actor: seen.update(fid=fid, name=name, actor=actor) or True)
    response = http.post("/api/lending/docs/staff/loan-files/7/guarantors", json={"name": "Carol White"})
    assert response.status_code == 200 and response.json() == {"loan_file_id": 7, "added": True}
    assert seen == {"fid": 7, "name": "Carol White", "actor": "human:staff_token:4"} and db.commits == 1
