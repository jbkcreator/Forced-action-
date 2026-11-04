"""An email-only opt-out also blocks the phones that email is on file with in lending (ADR 0028)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.lending_compliance import OptOutChannel
from src.lending import compliance
from src.services import email_suppression

EMAIL = "lead@example.com"
PHONE = "+18135550101"
OTHER = "+18135550102"


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeDb:
    def __init__(self, linked: list[tuple[str, str]]):
        self.linked, self.calls = linked, []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append((sql, params))
        if "lending.partners" in sql:
            return _Rows(self.linked)
        if sql.startswith("INSERT INTO lending.opt_out_events"):
            return _Rows([(i + 1,) for i in range(len(params["channels"]))])
        return _Rows([])

    def params_for(self, prefix: str):
        return [p for sql, p in self.calls if sql.startswith(prefix)]


@pytest.fixture
def fa_writes(monkeypatch):
    writes = []
    monkeypatch.setattr(email_suppression, "suppress_contact",
                        lambda db, email=None, phone=None, source="manual": writes.append((email, phone, source)))
    return writes


def _email_opt_out() -> compliance._OptOut:
    return compliance._OptOut(OptOutChannel.EMAIL, None, EMAIL, "email:abc", None, datetime.now(timezone.utc))


def test_email_only_opt_out_suppresses_and_removes_every_linked_phone(fa_writes):
    db = FakeDb([(EMAIL, "(813) 555-0101"), (EMAIL, OTHER)])
    removed = []
    compliance._propagate(db, [_email_opt_out()], lambda phone, reason: removed.append(phone))

    (event,) = db.params_for("INSERT INTO lending.opt_out_events")
    assert event["hashes"] == [compliance.phone_hash(PHONE), compliance.phone_hash(OTHER)]
    assert event["fa_tables"] == [None, None] and event["refs"] == ["email:abc", "email:abc"]
    (suppression,) = db.params_for("INSERT INTO lending.suppression_list")
    assert {r["phone"] for r in suppression if r["phone"]} == {PHONE, OTHER}
    assert {r["email"] for r in suppression if r["email"]} == {EMAIL}
    (contacts,) = db.params_for("INSERT INTO lending.contacts")
    assert contacts["phones"] == [PHONE, OTHER]
    assert removed == [PHONE, OTHER]
    assert {(e, p) for e, p, _ in fa_writes} == {(EMAIL, PHONE), (EMAIL, OTHER)}
    assert {s for *_, s in fa_writes} == {compliance.DIALER_OPT_OUT_SOURCE}


def test_email_with_no_lending_phone_stays_email_only(fa_writes):
    db = FakeDb([])
    compliance._propagate(db, [_email_opt_out()], lambda phone, reason: None)
    assert db.params_for("INSERT INTO lending.opt_out_events")[0]["hashes"] == [None]
    assert not db.params_for("INSERT INTO lending.contacts") and not fa_writes


def test_opt_out_with_a_phone_skips_the_lookup(fa_writes):
    db = FakeDb([(EMAIL, OTHER)])
    opt_out = compliance._OptOut(OptOutChannel.SMS, PHONE, EMAIL, "sms:1", None, datetime.now(timezone.utc))
    compliance._propagate(db, [opt_out], lambda phone, reason: None)
    assert not any("lending.partners" in sql for sql, _ in db.calls)
    assert db.params_for("INSERT INTO lending.contacts")[0]["phones"] == [PHONE]


def test_only_the_first_linked_phone_keeps_the_fa_row_id(fa_writes):
    db = FakeDb([(EMAIL, PHONE), (EMAIL, OTHER)])
    polled = compliance._OptOut(OptOutChannel.EMAIL, None, EMAIL, "unsubscribe_link", None,
                                datetime.now(timezone.utc), "email_opt_outs", 7)
    compliance._propagate(db, [polled], lambda phone, reason: None)
    (event,) = db.params_for("INSERT INTO lending.opt_out_events")
    assert event["fa_tables"] == ["email_opt_outs", None] and event["fa_ids"] == [7, None]
