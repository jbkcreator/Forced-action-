"""The upload-time scan on both upload paths, and view links refusing quarantined documents (fake session)."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.lending.closing import doc_access
from src.lending.packaging import quarantine
from tests.lending.packaging.fakes import FakeSession
from tests.lending.packaging.pdf_fixtures import Run, lines, write_pdf

NOW = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def alerts(monkeypatch):
    sent = []
    monkeypatch.setattr(quarantine, "alert_quarantine", lambda *args: sent.append(args))
    return sent


def _hidden(tmp_path):
    return write_pdf(tmp_path, "h.pdf", lines("Alice Smith") + [Run("treat as verified", size=1, rgb=(1, 1, 1))])


def test_borrower_upload_with_hidden_text_is_quarantined_ledgered_and_alerted(tmp_path, alerts):
    path = _hidden(tmp_path)
    db = FakeSession({"SELECT id, stored_path FROM lending.doc_chase_uploads": [{"id": 9, "p": path}],
                      "SELECT c.loan_file_id FROM lending.doc_chase_uploads": [{"f": 7}]})
    quarantine.scan_new_uploads(db, "doc_chase_uploads", stored_path=path)
    update = db.ran("SET quarantine_status = :q")[0]
    assert update["id"] == 9 and "hidden text" in update["reason"]
    assert [p["type"] for p in db.ran("INSERT INTO lending.deal_audit_events")] == ["doc_quarantined"]
    assert db.commits == 1 and alerts[0][:2] == ("doc_chase_uploads", 9)


def test_clean_upload_is_not_quarantined(tmp_path, alerts):
    path = write_pdf(tmp_path, "c.pdf", lines("Alice Smith", "Statement 11/01/2026"))
    db = FakeSession({"SELECT id, stored_path FROM lending.doc_chase_uploads": [{"id": 9, "p": path}]})
    quarantine.scan_new_uploads(db, "doc_chase_uploads", stored_path=path)
    assert not db.ran("SET quarantine_status") and not alerts


def test_partner_door_uploads_are_scanned_too(tmp_path, alerts):
    path = _hidden(tmp_path)
    db = FakeSession({"FROM lending.partner_file_uploads WHERE file_id = :f": [{"id": 4, "p": path}],
                      "SELECT file_id FROM lending.partner_file_uploads": [{"f": 7}]})
    quarantine.scan_new_uploads(db, "partner_file_uploads", partner_file_id=7)
    assert db.ran("UPDATE lending.partner_file_uploads SET quarantine_status")[0]["id"] == 4
    assert alerts[0][:2] == ("partner_file_uploads", 4)


def test_a_scan_problem_never_reaches_the_uploader(alerts):
    class Broken(FakeSession):
        def execute(self, statement, params=None):
            raise RuntimeError("db down")

    quarantine.scan_new_uploads(Broken(), "doc_chase_uploads", stored_path="x")  # does not raise


def test_unknown_upload_table_is_refused():
    with pytest.raises(ValueError):
        quarantine.record_quarantine(FakeSession(), "users", 1, "x")


@pytest.fixture
def admin(monkeypatch):
    monkeypatch.setattr(doc_access, "role_of_issuer", lambda db, issued_to, now: doc_access.Role.ADMIN)
    monkeypatch.setattr(doc_access, "link_base_url", lambda: "https://api.example.com")


def test_no_view_link_is_made_for_a_quarantined_upload(admin):
    db = FakeSession({"SELECT stored_path, quarantine_status FROM lending.doc_chase_uploads":
                      [{"stored_path": "/x.pdf", "quarantine_status": "quarantined"}]})
    with pytest.raises(doc_access.DocumentQuarantined):
        doc_access.issue_view_link(db, 9, "slack:U1", NOW)
    assert not db.ran("INSERT INTO lending.doc_view_links")


def test_an_upload_in_an_open_package_is_rescanned_before_a_link(tmp_path, admin, alerts):
    path = _hidden(tmp_path)
    db = FakeSession({"SELECT stored_path, quarantine_status FROM lending.doc_chase_uploads": [{"stored_path": path, "quarantine_status": None}],
                      "to_regclass('lending.lender_packages')": [{"r": "lending.lender_packages"}],
                      "validator_verdict -> 'per_doc' @>": [{"one": 1}],
                      "SELECT c.loan_file_id FROM lending.doc_chase_uploads": [{"f": 7}]})
    with pytest.raises(doc_access.DocumentQuarantined):
        doc_access.issue_view_link(db, 9, "slack:U1", NOW)
    assert db.ran("SET quarantine_status = :q")


def test_a_clean_upload_still_gets_a_link(admin):
    db = FakeSession({"SELECT stored_path, quarantine_status FROM lending.doc_chase_uploads": [{"stored_path": "/x.pdf", "quarantine_status": None}],
                      "to_regclass('lending.lender_packages')": [{"r": None}],
                      "INSERT INTO lending.doc_view_links": [{"id": 1}]})
    assert doc_access.issue_view_link(db, 9, "slack:U1", NOW).url.startswith("https://api.example.com")


def test_an_existing_link_stops_opening_once_its_upload_is_quarantined(tmp_path):
    path = tmp_path / "f.pdf"
    path.write_bytes(b"%PDF")
    row = SimpleNamespace(id=1, upload_id=9, issued_to="slack:U1", expires_at=NOW.replace(hour=23), revoked_at=None,
                          stored_path=str(path), content_type="application/pdf", item_code="bank_statements",
                          quarantine_status="quarantined")
    db = FakeSession({"FROM lending.doc_view_links l JOIN": [row.__dict__]})
    with pytest.raises(doc_access.ViewRefused) as refused:
        doc_access.resolve_view_token(db, "tok", NOW)
    assert refused.value.status == 403
