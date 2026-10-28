"""P-1 partner_service: the checks and operations behind ``/partners`` and the partner CLI (SQLite, no Slack)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending import partner_service, partners
from src.lending.partner_service import PartnerError, PartnerIn, PartnerLinkNotConfigured
from tests.lending.test_partner_door import engine  # noqa: F401 - fixture

NOW = datetime(2026, 10, 27, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(engine):  # noqa: F811
    session = Session(engine)
    yield session
    session.close()


def _body(**overrides) -> PartnerIn:
    fields = {"name": " Maria Lopez ", "partner_type": "wholesaler", "email": "Maria@Example.com",
              "phone": "(813) 555-0142", "states": "fl, ga, FL"}
    fields.update(overrides)
    return PartnerIn(**fields)


def test_create_stores_cleaned_values_and_only_the_token_hash(db) -> None:
    issued = partner_service.create(db, _body(), created_by="slack:U1", now=NOW)
    row = db.execute(text("SELECT * FROM lending.partners WHERE id = :id"), {"id": issued.partner_id}).mappings().one()
    assert (row["name"], row["email"], row["phone"], row["states"]) == ("Maria Lopez", "maria@example.com",
                                                                         "+18135550142", "FL,GA")
    assert row["created_by"] == "slack:U1" and row["agreement_status"] == "pending"
    assert row["token_hash"] == partners.hash_token(issued.token)
    assert issued.token not in {str(value) for value in row.values()}
    assert partners.partner_for_token(db, issued.token).id == issued.partner_id
    assert not issued.reused


@pytest.mark.parametrize("overrides, field", [
    ({"name": "  "}, "name"),
    ({"partner_type": "lender"}, "partner_type"),
    ({"email": "not-an-email"}, "email"),
    ({"phone": "123"}, "phone"),
    ({"states": "Florida"}, "states"),
    ({"agreement_status": "maybe"}, "agreement_status"),
    ({"email": None, "phone": "(813) 555-0142", "sms_consent": False}, "email"),  # unreachable partner
    ({"email": None, "phone": None, "sms_consent": True}, "phone"),  # texts agreed, nowhere to send them
])
def test_each_bad_field_is_named_and_nothing_is_created(db, overrides, field) -> None:
    with pytest.raises(PartnerError) as error:
        partner_service.create(db, _body(**overrides), created_by="slack:U1", now=NOW)
    assert field in error.value.errors
    assert db.execute(text("SELECT COUNT(*) FROM lending.partners")).scalar() == 0


def test_a_phone_with_text_consent_is_enough_without_an_email(db) -> None:
    issued = partner_service.create(db, _body(email=None, sms_consent=True), created_by="slack:U1", now=NOW)
    assert partners.partner_by_id(db, issued.partner_id).sms_consent is True


def test_a_redelivered_create_reissues_the_link_instead_of_making_a_twin(db) -> None:
    first = partner_service.create(db, _body(), created_by="slack:U1", now=NOW)
    again = partner_service.create(db, _body(name="maria lopez"), created_by="slack:U1", now=NOW + timedelta(minutes=1))
    assert again.reused and again.partner_id == first.partner_id
    assert db.execute(text("SELECT COUNT(*) FROM lending.partners")).scalar() == 1
    assert partners.partner_for_token(db, first.token) is None
    assert partners.partner_for_token(db, again.token).id == first.partner_id


def test_the_same_name_later_or_from_someone_else_is_a_new_partner(db) -> None:
    partner_service.create(db, _body(), created_by="slack:U1", now=NOW)
    later = partner_service.create(db, _body(), created_by="slack:U1", now=NOW + timedelta(minutes=11))
    other = partner_service.create(db, _body(), created_by="slack:U2", now=NOW + timedelta(minutes=1))
    assert not later.reused and not other.reused
    assert db.execute(text("SELECT COUNT(*) FROM lending.partners")).scalar() == 3


def test_new_link_kills_the_old_one_and_reopens_a_revoked_link(db) -> None:
    first = partner_service.create(db, _body(), created_by="slack:U1", now=NOW)
    second = partner_service.new_link(db, first.partner_id)
    assert partners.partner_for_token(db, first.token) is None
    assert partners.partner_for_token(db, second.token).id == first.partner_id
    partner_service.revoke(db, first.partner_id)
    third = partner_service.new_link(db, first.partner_id)
    assert partners.partner_for_token(db, third.token).id == first.partner_id


def test_revoke_leaves_the_partner_and_says_when_already_off(db) -> None:
    issued = partner_service.create(db, _body(), created_by="slack:U1", now=NOW)
    assert partner_service.revoke(db, issued.partner_id) == "Maria Lopez"
    assert partners.partner_for_token(db, issued.token) is None
    assert partners.partner_by_id(db, issued.partner_id) is not None
    with pytest.raises(PartnerError) as error:
        partner_service.revoke(db, issued.partner_id)
    assert "already switched off" in error.value.errors["partner_id"]
    with pytest.raises(PartnerError):
        partner_service.new_link(db, 999)


def test_list_shows_link_state_newest_first(db) -> None:
    old = partner_service.create(db, _body(name="Old One"), created_by="cli", now=NOW - timedelta(days=1))
    new = partner_service.create(db, _body(name="New One"), created_by="cli", now=NOW)
    partner_service.revoke(db, old.partner_id)
    listed = partner_service.list_partners(db)
    assert [(p["id"], p["link_state"]) for p in listed] == [(new.partner_id, "live"), (old.partner_id, "revoked")]


@pytest.mark.parametrize("base, expected", [
    ("https://api.nextdeallending.com/", "https://api.nextdeallending.com/api/lending/partners/tok"),
    ("https://api.nextdeallending.com", "https://api.nextdeallending.com/api/lending/partners/tok"),
    ("", None),
    ("http://api.nextdeallending.com", None),
])
def test_partner_link_needs_an_https_host(monkeypatch, base, expected) -> None:
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", base)
    if expected is None:
        with pytest.raises(PartnerLinkNotConfigured):
            partner_service.partner_link("tok")
    else:
        assert partner_service.partner_link("tok") == expected


def test_cli_create_prints_the_full_link_and_refuses_bad_input(engine, monkeypatch, capsys) -> None:  # noqa: F811
    from contextlib import contextmanager

    import src.lending.db as lending_db

    @contextmanager
    def session():
        s = Session(engine)
        try:
            yield s
            s.commit()
        finally:
            s.close()

    monkeypatch.setattr(lending_db, "lending_session", session)
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", "https://api.nextdeallending.com")
    assert partners.main(["create", "--name", "Maria", "--type", "broker", "--email", "m@x.com"]) == 0
    assert "https://api.nextdeallending.com/api/lending/partners/" in capsys.readouterr().out
    assert partners.main(["create", "--name", "Bad", "--type", "broker", "--email", "nope"]) == 2
    with Session(engine) as check:
        assert check.execute(text("SELECT COUNT(*) FROM lending.partners")).scalar() == 1
