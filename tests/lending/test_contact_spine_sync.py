"""R1 1.1 contact spine sync: one contact per person, stable ids, merges that flatten, nothing written on a bad read."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.lending.spine import contact_spine
from src.lending.spine.contact_spine import canonical_id, sync_contact_spine
from src.lending.spine.sources import SOURCE_READERS
from tests.lending.contact_spine_db import spine_session

NOW = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def db():
    with spine_session() as session:
        yield session


def _lead(db, id_, phone, email=None, first="Ann"):
    db.execute(text("INSERT INTO lending.lendingflow_leads (id, first_name, phone, email) VALUES (:i, :f, :p, :e)"),
               {"i": id_, "f": first, "p": phone, "e": email})


def _web(db, id_, phone, email=None):
    db.execute(text("INSERT INTO lending.web_leads (id, name, phone, email) VALUES (:i, 'W', :p, :e)"),
               {"i": id_, "p": phone, "e": email})


def _owner(db, source, ref):
    return db.execute(text("SELECT contact_id FROM lending.contact_sources WHERE source = :s AND source_ref = :r"),
                      {"s": source, "r": str(ref)}).scalar()


def _count(db, table):
    return db.execute(text(f"SELECT count(*) FROM lending.{table}")).scalar()


def test_first_sync_creates_one_contact_per_person(db):
    _lead(db, 1, "8135550101", "ann@x.com")
    _web(db, 2, "8135550109", "ann@x.com")
    _web(db, 3, "8135550102")
    result = sync_contact_spine(db, now=NOW)
    assert result.records == 3
    assert result.contacts_created == 2
    assert result.merges == 0
    assert result.failed_sources == ()
    assert _owner(db, "lendingflow_leads", 1) == _owner(db, "web_leads", 2) != _owner(db, "web_leads", 3)
    assert db.execute(text("SELECT display_name FROM lending.contact_spine WHERE id = :i"),
                      {"i": _owner(db, "web_leads", 2)}).scalar() == "Ann"


def test_second_sync_changes_nothing(db):
    _lead(db, 1, "8135550101", "ann@x.com")
    _web(db, 3, "8135550102")
    sync_contact_spine(db, now=NOW)
    before = db.execute(text("SELECT source, source_ref, contact_id FROM lending.contact_sources ORDER BY 1, 2")).all()
    again = sync_contact_spine(db, now=NOW)
    assert (again.contacts_created, again.merges, again.identifiers_written, again.sources_written) == (0, 0, 0, 0)
    assert db.execute(text("SELECT source, source_ref, contact_id FROM lending.contact_sources ORDER BY 1, 2")).all() == before
    assert _count(db, "contact_spine") == 2


def test_new_record_linking_two_contacts_merges_them(db):
    _lead(db, 1, "8135550101")
    _web(db, 2, "8135550102", "b@x.com")
    sync_contact_spine(db, now=NOW)
    a, b = _owner(db, "lendingflow_leads", 1), _owner(db, "web_leads", 2)
    db.execute(text("INSERT INTO lending.after_hours_conversations (id, first_name, phone, email) "
                    "VALUES (5, 'Ann', '8135550101', 'b@x.com')"))
    result = sync_contact_spine(db, now=NOW)
    survivor, merged = min(a, b), max(a, b)
    assert result.merges == 1
    assert db.execute(text("SELECT merged_into FROM lending.contact_spine WHERE id = :i"), {"i": merged}).scalar() == survivor
    assert {_owner(db, "lendingflow_leads", 1), _owner(db, "web_leads", 2),
            _owner(db, "after_hours_conversations", 5)} == {survivor}
    assert db.execute(text("SELECT count(*) FROM lending.contact_identifiers WHERE contact_id = :i"),
                      {"i": merged}).scalar() == 0
    assert db.execute(text("SELECT survivor_id, merged_id FROM lending.contact_spine_merges")).all() == [(survivor, merged)]


def test_merge_chain_points_to_final_survivor(db):
    _web(db, 1, "8135550101")
    _web(db, 2, "8135550102")
    _web(db, 3, "8135550103")
    sync_contact_spine(db, now=NOW)
    c1, c2, c3 = (_owner(db, "web_leads", i) for i in (1, 2, 3))
    _lead(db, 10, "8135550102", "x@x.com")
    _lead(db, 11, "8135550103", "x@x.com")
    sync_contact_spine(db, now=NOW)  # c2 + c3 -> min(c2, c3)
    _lead(db, 12, "8135550101", "y@x.com")
    _lead(db, 13, "8135550102", "y@x.com")
    sync_contact_spine(db, now=NOW)  # c1 + that survivor -> c1
    survivor = min(c1, c2, c3)
    assert {canonical_id(db, c) for c in (c1, c2, c3)} == {survivor}
    assert db.execute(text("SELECT count(*) FROM lending.contact_spine WHERE merged_into IS NOT NULL "
                           "AND merged_into <> :s"), {"s": survivor}).scalar() == 0


def test_canonical_id_of_unknown_contact_is_none(db):
    assert canonical_id(db, 999) is None


def test_failed_source_writes_nothing(db):
    _lead(db, 1, "8135550101")

    def broken(_db):
        raise RuntimeError("boom")
        yield  # pragma: no cover

    readers = SOURCE_READERS + (("broken", broken),)
    result = sync_contact_spine(db, now=NOW, readers=readers)
    assert result.failed_sources == ("broken",)
    assert result.contacts_created == 1
    assert _count(db, "contact_spine") == 0
    assert _count(db, "contact_sources") == 0


def test_phone_later_known_as_landline_stops_merging(db):
    _web(db, 1, "8135550101", "a@x.com")
    sync_contact_spine(db, now=NOW)
    db.execute(text("INSERT INTO lending.contacts (id, phone, line_type) VALUES (1, '+18135550101', 'landline')"))
    _web(db, 2, "8135550101", "b@x.com")
    result = sync_contact_spine(db, now=NOW)
    assert result.contacts_created == 1  # web_leads:2; the landline-only lending.contacts row makes no contact
    assert _owner(db, "web_leads", 1) != _owner(db, "web_leads", 2)
    assert _owner(db, "contacts", 1) is None
    assert db.execute(text("SELECT count(*) FROM lending.contact_identifiers WHERE kind = 'phone' "
                           "AND value = '+18135550101' AND merge_key")).scalar() == 0
    assert db.execute(text("SELECT count(*) FROM lending.contact_identifiers WHERE kind = 'phone' "
                           "AND value = '+18135550101'")).scalar() == 2
    assert sync_contact_spine(db, now=NOW).identifiers_written == 0


def test_landline_back_to_mobile_merges_again(db):
    db.execute(text("INSERT INTO lending.contacts (id, phone, line_type) VALUES (1, '+18135550101', 'landline')"))
    _web(db, 1, "8135550101", "a@x.com")
    _web(db, 2, "8135550101", "b@x.com")
    sync_contact_spine(db, now=NOW)
    db.execute(text("UPDATE lending.contacts SET line_type = 'mobile'"))
    result = sync_contact_spine(db, now=NOW)
    assert result.merges == 1  # both web leads are one person again, and the lending.contacts row joins them
    assert _owner(db, "web_leads", 1) == _owner(db, "web_leads", 2) == _owner(db, "contacts", 1)
    assert db.execute(text("SELECT count(*) FROM lending.contact_identifiers WHERE kind = 'phone'")).scalar() == 1
    assert sync_contact_spine(db, now=NOW).identifiers_written == 0


def test_dry_run_writes_nothing(db):
    _lead(db, 1, "8135550101")
    result = sync_contact_spine(db, now=NOW, dry_run=True)
    assert result.contacts_created == 1
    assert _count(db, "contact_spine") == 0


def test_task_does_nothing_while_switch_is_off(monkeypatch):
    from src.tasks import lending_contact_spine_sync as task

    monkeypatch.setattr(task, "get_settings", lambda: type("S", (), {"lending_contact_spine_enabled": False})())
    monkeypatch.setattr(task, "lending_session", lambda: pytest.fail("opened a session while off"))
    assert task.main([]) == 0


def test_task_exits_1_when_a_source_failed(monkeypatch):
    from contextlib import contextmanager

    from src.tasks import lending_contact_spine_sync as task

    @contextmanager
    def fake_session():
        yield object()

    monkeypatch.setattr(task, "get_settings", lambda: type("S", (), {"lending_contact_spine_enabled": True})())
    monkeypatch.setattr(task, "lending_session", fake_session)
    monkeypatch.setattr(task, "sync_contact_spine", lambda db, **kw: contact_spine.SyncResult(
        records=1, contacts_created=0, merges=0, identifiers_written=0, sources_written=0,
        failed_sources=("web_leads",)))
    assert task.main([]) == 1


def test_ghl_id_on_a_line_later_known_as_landline_stops_merging(db):
    db.execute(text("INSERT INTO lending.web_leads (id, name, phone, ghl_contact_id) VALUES (1, 'A', '8135550101', 'G1')"))
    sync_contact_spine(db, now=NOW)
    db.execute(text("INSERT INTO lending.contacts (id, phone, line_type) VALUES (1, '+18135550101', 'landline')"))
    db.execute(text("INSERT INTO lending.web_leads (id, name, phone, ghl_contact_id) VALUES (2, 'B', '8135550101', 'G1')"))
    sync_contact_spine(db, now=NOW)
    assert _owner(db, "web_leads", 1) != _owner(db, "web_leads", 2)
    assert db.execute(text("SELECT count(*) FROM lending.contact_identifiers WHERE kind = 'ghl_contact' "
                           "AND merge_key")).scalar() == 0
