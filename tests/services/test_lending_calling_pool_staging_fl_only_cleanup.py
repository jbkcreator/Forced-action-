"""F17: one-time cleanup of NH/WI mortgage_broker rows staged before the B4/finding
#9 state-filter fix (pool_extraction.py matched county name only, not state).
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from migrations.apply_lending_calling_pool_staging_fl_only_cleanup import run

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)


def _stage(db, state, *, pool_name="mortgage_broker"):
    db.execute(text(
        "INSERT INTO lending.calling_pool_staging (run_id, pool_name, aircall_campaign_tag, source_table, "
        "source_tag, normalized_phone, phone_available, borrower_name, entity_name, state) "
        "VALUES (:r, :pool, 'X', 'ofr_mortgage_brokers', 'list_4', :p, true, 'B', 'E LLC', :state)"),
        {"r": str(uuid.uuid4()), "pool": pool_name, "p": f"+1813555{hash(state) % 10000:04d}", "state": state},
    )


def _run_isolated(monkeypatch, seed):
    """run() commits internally, so it must not share the test's own rollback-bound
    transaction (same issue finding #12's and the warm-network loader's tests hit) —
    use a throwaway connection and wipe what it wrote afterward."""
    engine = create_engine(os.environ["DATABASE_URL"])
    try:
        session = Session(bind=engine)
        seed(session)

        class _Ctx:
            def __enter__(self):
                return session

            def __exit__(self, *exc):
                return False

        import migrations.apply_lending_calling_pool_staging_fl_only_cleanup as mod
        monkeypatch.setattr(mod, "get_db_context", lambda: _Ctx())
        yield session
        session.close()
    finally:
        with engine.begin() as cleanup_conn:
            cleanup_conn.execute(text(
                "DELETE FROM lending.calling_pool_staging WHERE entity_name = 'E LLC'"
            ))
        engine.dispose()


def test_removes_out_of_state_brokers_but_keeps_fl(monkeypatch):
    gen = _run_isolated(monkeypatch, lambda s: (_stage(s, "NH"), _stage(s, "WI"), _stage(s, "FL")))
    session = next(gen)
    run()
    remaining = session.execute(text(
        "SELECT state FROM lending.calling_pool_staging WHERE pool_name = 'mortgage_broker' "
        "AND entity_name = 'E LLC' ORDER BY state"
    )).scalars().all()
    assert remaining == ["FL"]
    next(gen, None)


def test_is_idempotent_a_second_run_removes_nothing(monkeypatch):
    gen = _run_isolated(monkeypatch, lambda s: _stage(s, "FL"))
    session = next(gen)
    run()
    run()  # idempotent: nothing left to remove, no error
    count = session.execute(text(
        "SELECT count(*) FROM lending.calling_pool_staging WHERE pool_name = 'mortgage_broker' "
        "AND entity_name = 'E LLC'"
    )).scalar()
    assert count == 1
    next(gen, None)


def test_never_touches_other_pools(monkeypatch):
    gen = _run_isolated(monkeypatch, lambda s: _stage(s, "NH", pool_name="active_builder"))
    session = next(gen)
    run()
    count = session.execute(text(
        "SELECT count(*) FROM lending.calling_pool_staging WHERE pool_name = 'active_builder' "
        "AND entity_name = 'E LLC'"
    )).scalar()
    assert count == 1
    next(gen, None)
