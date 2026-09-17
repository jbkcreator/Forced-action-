"""WP-7 — abandonment sweep task: dry-run vs --apply, cutoff behavior.

The dry-run and fresh-session cases share `fresh_db`'s connection via a
monkeypatched get_db_context — safe, because dry_run never commits.
`--apply` genuinely commits (that's the point of the test), so it must NOT
reuse fresh_db's transaction — sharing it would commit fresh_db's whole
transaction against the shared DB and make the fixture's rollback a no-op.
It uses the real get_db_context instead, with its own explicit teardown,
same convention as tests/scenarios/test_selfserve_flow.py.
"""
import uuid

import pytest
from sqlalchemy import text

from src.core.database import get_db_context
from src.services.selfserve_sessions import create_session
from src.tasks.selfserve_abandonment_sweep import run


class _ReuseConnection:
    """Test-only stand-in for get_db_context() that reuses an existing,
    already-open session instead of opening a new connection — so what the
    test set up (uncommitted) and what the sweep queries are the same
    transaction. Only safe when the code under test never commits."""

    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return False


def _make_stale_session(db, hours_old: int) -> str:
    session_row = create_session(db, prefill_snapshot={"property_id": None, "fields": {}})
    session_row.token = str(uuid.uuid4())
    db.flush()
    db.execute(
        text(
            "UPDATE selfserve_sessions SET last_activity_at = now() - make_interval(hours => :h) "
            "WHERE token = :t"
        ),
        {"h": hours_old, "t": session_row.token},
    )
    db.flush()
    return session_row.token


def test_dry_run_reports_but_does_not_mutate(fresh_db, monkeypatch):
    token = _make_stale_session(fresh_db, hours_old=48)
    monkeypatch.setattr(
        "src.tasks.selfserve_abandonment_sweep.get_db_context",
        lambda: _ReuseConnection(fresh_db),
    )

    result = run(dry_run=True, older_than_hours=24)
    assert token in result["tokens"]
    assert result["dry_run"] is True

    row = fresh_db.execute(text("SELECT status FROM selfserve_sessions WHERE token = :t"), {"t": token}).first()
    assert row.status != "abandoned"  # dry run must not mutate


def test_fresh_session_not_swept(fresh_db, monkeypatch):
    token = _make_stale_session(fresh_db, hours_old=1)
    monkeypatch.setattr(
        "src.tasks.selfserve_abandonment_sweep.get_db_context",
        lambda: _ReuseConnection(fresh_db),
    )

    result = run(dry_run=True, older_than_hours=24)
    assert token not in result["tokens"]


@pytest.fixture
def committed_stale_session():
    with get_db_context() as db:
        session_row = create_session(db, prefill_snapshot={"property_id": None, "fields": {}})
        session_row.token = str(uuid.uuid4())
        db.flush()
        db.execute(
            text(
                "UPDATE selfserve_sessions SET last_activity_at = now() - interval '48 hours' "
                "WHERE token = :t"
            ),
            {"t": session_row.token},
        )
        db.commit()
        token = session_row.token
    yield token
    with get_db_context() as db:
        db.execute(text("DELETE FROM selfserve_sessions WHERE token = :t"), {"t": token})
        db.commit()


def test_apply_marks_stale_sessions_abandoned(committed_stale_session):
    result = run(dry_run=False, older_than_hours=24)
    assert committed_stale_session in result["tokens"]

    with get_db_context() as db:
        row = db.execute(
            text("SELECT status FROM selfserve_sessions WHERE token = :t"),
            {"t": committed_stale_session},
        ).first()
    assert row.status == "abandoned"
