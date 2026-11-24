"""R1 0.2: borrower search (never guesses), document list, 15-minute view links."""
from __future__ import annotations

import json

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.closing.doc_access import (
    MAX_MATCHES,
    ViewRefused,
    document_list,
    find_document_borrowers,
    issue_view_link,
    latest_upload,
    resolve_view_token,
)
from src.lending.partners import hash_token
from src.lending.security.rbac import DocumentAccessDenied

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)


def _two_marks(docs):
    smith = docs.add(1, "Mark Smith", "+18135550101", uploaded=("bank_statements",))
    taylor = docs.add(2, "Mark Taylor", "+18135554432", uploaded=("government_id",))
    return smith, taylor


def test_search_two_marks_lists_both_masked(documents_db):
    smith, taylor = _two_marks(documents_db)
    found = find_document_borrowers(documents_db.db, "mark")
    assert not found.too_many
    by_ref = {m.lead_ref: m for m in found.matches}
    assert by_ref[smith.lead_ref].display == "Mark S. …0101" and by_ref[taylor.lead_ref].display == "Mark T. …4432"
    assert by_ref[smith.lead_ref].uploaded_items == ("bank_statements",)
    assert "8135550101" not in str(found)


def test_search_by_last4_phone_and_lead_ref_find_one(documents_db):
    smith, _ = _two_marks(documents_db)
    for query in ("0101", "(813) 555-0101", smith.lead_ref, "Mark Smith"):
        assert [m.lead_ref for m in find_document_borrowers(documents_db.db, query).matches] == [smith.lead_ref]


def test_search_ignores_leads_without_chase_and_wildcards(documents_db):
    _two_marks(documents_db)
    documents_db.db.execute(text("INSERT INTO lending.web_leads (id, name) VALUES (9, 'Mark Nochase')"))
    assert {m.display for m in find_document_borrowers(documents_db.db, "mark").matches} == {"Mark S. …0101",
                                                                                             "Mark T. …4432"}
    assert find_document_borrowers(documents_db.db, "%").matches == ()


def test_name_matches_word_starts_only(documents_db):
    documents_db.add(5, "Joanne Smith", "+18135550155")
    ann = documents_db.add(6, "Ann Lee", "+18135550166")
    assert [m.lead_ref for m in find_document_borrowers(documents_db.db, "ann").matches] == [ann.lead_ref]
    assert [m.display for m in find_document_borrowers(documents_db.db, "lee").matches] == ["Ann L. …0166"]


def test_search_more_than_10_is_too_many(documents_db):
    for n in range(MAX_MATCHES + 1):
        documents_db.add(100 + n, f"Mark Person{n}", f"+1813555{1000 + n}", uploaded=())
    found = find_document_borrowers(documents_db.db, "mark")
    assert found.too_many and found.matches == ()


def test_document_list_has_no_paths_or_file_names(documents_db):
    smith, _ = _two_marks(documents_db)
    listed = document_list(documents_db.db, smith.lead_ref)
    dumped = json.dumps(listed)
    assert listed["borrower"] == "Mark S. …0101" and str(documents_db.tmp_path) not in dumped
    assert "bank_statements.pdf" not in dumped and "stored_path" not in dumped
    items = {i["item_code"]: i for i in listed["chases"][0]["items"]}
    assert items["bank_statements"]["received"] and len(items["bank_statements"]["uploads"]) == 1
    assert not items["government_id"]["received"]
    assert document_list(documents_db.db, "web_lead:999") is None


def test_latest_upload_and_missing_item(documents_db):
    smith, _ = _two_marks(documents_db)
    upload_id, label, _ = latest_upload(documents_db.db, smith.lead_ref, "bank_statements")
    assert upload_id and label.startswith("Last 30 days of bank statements")
    assert latest_upload(documents_db.db, smith.lead_ref, "government_id") is None


def test_issue_admin_ok_caller_denied(documents_db):
    smith, _ = _two_marks(documents_db)
    upload_id = latest_upload(documents_db.db, smith.lead_ref, "bank_statements")[0]
    link = issue_view_link(documents_db.db, upload_id, "slack:U_JOSH", NOW)
    token = link.url.rsplit("/", 1)[1]
    assert link.url.startswith("https://api.example.com/api/lending/docs/view/")
    assert link.expires_at == NOW + timedelta(minutes=15)
    row = documents_db.db.execute(text("SELECT token_hash, issued_to FROM lending.doc_view_links")).one()
    assert row.token_hash == hash_token(token) and row.issued_to == "slack:U_JOSH"
    with pytest.raises(DocumentAccessDenied):
        issue_view_link(documents_db.db, upload_id, "slack:U_DAVID", NOW)
    with pytest.raises(LookupError):
        issue_view_link(documents_db.db, 987654, "slack:U_JOSH", NOW)
    assert documents_db.db.execute(text("SELECT count(*) FROM lending.doc_view_links")).scalar() == 1


def test_resolve_valid_expired_and_unknown(documents_db):
    smith, _ = _two_marks(documents_db)
    upload_id = latest_upload(documents_db.db, smith.lead_ref, "bank_statements")[0]
    token = issue_view_link(documents_db.db, upload_id, "slack:U_JOSH", NOW).url.rsplit("/", 1)[1]
    found = resolve_view_token(documents_db.db, token, NOW + timedelta(minutes=14))
    assert found.path == smith.paths["bank_statements"] and found.download_name == "bank_statements.pdf"
    for when, token_value, status in ((NOW + timedelta(minutes=15), token, 410), (NOW, "not-a-token", 403)):
        with pytest.raises(ViewRefused) as refused:
            resolve_view_token(documents_db.db, token_value, when)
        assert refused.value.status == status
