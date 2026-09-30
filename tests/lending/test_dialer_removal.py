"""Removing a phone from the dialer pool.

Runs on the real DB inside a transaction that is always rolled back; the load
table is created inside that transaction, so nothing persists.
"""
from __future__ import annotations

import inspect
import os
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.lending.dialer_removal import (
    REMOVED_FROM_DIALER,
    DialerRemovalUndecided,
    remove_contact_from_pool,
)
from src.lending.models import LendingDialerLoadRecord

needs_db = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

PHONE = "+18135558201"


def test_matches_the_compliance_floor_dialer_remover_contract(monkeypatch):
    from src.lending import dialer_port
    from src.lending.compliance import _default_dialer_remover

    monkeypatch.setattr(dialer_port, "get_dialer",
                        lambda: dialer_port.BatchDialerAdapter(http=lambda *a, **k: {}))
    remover = _default_dialer_remover()
    assert remover is not None
    assert list(inspect.signature(remover).parameters) == ["phone", "reason"]


def test_invalid_phone_raises():
    with pytest.raises(ValueError):
        remove_contact_from_pool("not-a-phone")


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _context(session):
    @contextmanager
    def ctx():
        yield session
        session.flush()
    return ctx


def _load(db, contact_id=77):
    db.execute(text(
        "INSERT INTO lending.dialer_load_records "
        "(run_id, pool, source_record_ref, phone, phone_hash, dialer_contact_id) "
        "VALUES ('run-1', 'builders', 'a', :p, :h, :c)"
    ), {"p": PHONE, "h": "a" * 64, "c": contact_id})


def _state(db):
    return db.execute(text(
        "SELECT active, deactivation_reason FROM lending.dialer_load_records WHERE phone = :p"
    ), {"p": PHONE}).first()


@needs_db
class TestRemoval:
    def test_removes_from_aircall_then_deactivates(self, db):
        _load(db)
        removed = []
        remove_contact_from_pool("(813) 555-8201", removal=removed.append, db_context=_context(db))
        assert removed == [77]
        assert tuple(_state(db)) == (False, REMOVED_FROM_DIALER)

    def test_undecided_aircall_control_raises_and_changes_nothing(self, db):
        _load(db)
        with pytest.raises(DialerRemovalUndecided):
            remove_contact_from_pool(PHONE, db_context=_context(db))
        assert tuple(_state(db)) == (True, None)

    def test_aircall_failure_keeps_the_row_active(self, db):
        _load(db)

        def failing(contact_id):
            raise RuntimeError("aircall down")

        with pytest.raises(RuntimeError):
            remove_contact_from_pool(PHONE, removal=failing, db_context=_context(db))
        assert tuple(_state(db)) == (True, None)

    def test_phone_not_in_the_dialer_is_a_no_op(self, db):
        removed = []
        remove_contact_from_pool(PHONE, removal=removed.append, db_context=_context(db))
        assert removed == []

    def test_row_without_aircall_contact_is_deactivated_without_calling_aircall(self, db):
        _load(db, contact_id=None)
        removed = []
        remove_contact_from_pool(PHONE, removal=removed.append, db_context=_context(db))
        assert removed == []
        assert tuple(_state(db)) == (False, REMOVED_FROM_DIALER)
