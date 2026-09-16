"""WP-9 dial-list disposition write-back + daily cron driver tests.

Disposition: in-memory SQLite with only agent_lane_opportunity_outcomes;
asserts won/lost inserts, idempotent rerun, validation ValueErrors,
source_ref determinism, and that a DB failure is logged and re-raised.

Cron: the daily driver's dry-run generates without posting; the live run
delegates to generate_and_deliver. Both monkeypatch the DB + engine seams.
"""
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import BigInteger, create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


@compiles(BigInteger, "sqlite")
def _compile_bigint_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    # SQLite autoincrements only INTEGER PRIMARY KEY, not BIGINT — map it so the
    # server-side id default works in-memory (Postgres keeps BIGINT in prod).
    return "INTEGER"

from src.core.models import AgentLaneOpportunityOutcome
from src.services.dial_list.disposition import (
    DispositionResult,
    record_dial_disposition,
)
from src.services.dial_list.models import DialList

AS_OF = date(2026, 9, 16)


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    AgentLaneOpportunityOutcome.__table__.create(bind=engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _count(db) -> int:
    return db.query(AgentLaneOpportunityOutcome).count()


# --- disposition -----------------------------------------------------------

def test_record_win_inserts():
    engine = create_engine("sqlite:///:memory:")
    AgentLaneOpportunityOutcome.__table__.create(bind=engine)
    session = sessionmaker(bind=engine)()
    try:
        res = record_dial_disposition(
            session, opportunity_thread_id="THREAD-1", outcome="won", as_of=AS_OF
        )
        assert isinstance(res, DispositionResult)
        assert res.inserted is True
        assert res.outcome == "won"
        assert res.reason_code is None
        assert res.source_ref == "dial_list:THREAD-1:2026-09-16"
        row = session.query(AgentLaneOpportunityOutcome).one()
        assert row.outcome == "won"
        assert row.reason_code is None
        assert row.coded_by == "dial_list"
    finally:
        session.close()
        engine.dispose()


def test_record_loss_inserts_with_reason(db):
    res = record_dial_disposition(
        db, opportunity_thread_id="T2", outcome="lost",
        loss_code="price", actor="josh", as_of=AS_OF,
    )
    assert res.inserted is True
    assert res.reason_code == "price"
    row = db.query(AgentLaneOpportunityOutcome).one()
    assert row.outcome == "lost"
    assert row.reason_code == "price"
    assert row.coded_by == "dial_list:josh"


def test_rerun_is_idempotent(db):
    first = record_dial_disposition(
        db, opportunity_thread_id="T3", outcome="won", as_of=AS_OF
    )
    second = record_dial_disposition(
        db, opportunity_thread_id="T3", outcome="won", as_of=AS_OF
    )
    assert first.inserted is True
    assert second.inserted is False  # thread already terminal, not overwritten
    assert _count(db) == 1


def test_source_ref_is_deterministic(db):
    a = record_dial_disposition(
        db, opportunity_thread_id="T4", outcome="won", as_of=AS_OF
    )
    # same thread + date always yields the same ref
    assert a.source_ref == "dial_list:T4:2026-09-16"


def test_invalid_outcome_raises(db):
    with pytest.raises(ValueError, match="invalid outcome"):
        record_dial_disposition(
            db, opportunity_thread_id="T5", outcome="maybe", as_of=AS_OF
        )
    assert _count(db) == 0


def test_lost_without_code_raises(db):
    with pytest.raises(ValueError, match="requires a loss_code"):
        record_dial_disposition(
            db, opportunity_thread_id="T6", outcome="lost", as_of=AS_OF
        )
    assert _count(db) == 0


def test_won_with_code_raises(db):
    with pytest.raises(ValueError, match="must not carry a loss_code"):
        record_dial_disposition(
            db, opportunity_thread_id="T7", outcome="won",
            loss_code="price", as_of=AS_OF,
        )
    assert _count(db) == 0


def test_invalid_loss_code_raises(db):
    with pytest.raises(ValueError, match="invalid loss_code"):
        record_dial_disposition(
            db, opportunity_thread_id="T8", outcome="lost",
            loss_code="vibes", as_of=AS_OF,
        )
    assert _count(db) == 0


def test_db_failure_logs_and_reraises(caplog):
    session = MagicMock()
    session.execute.side_effect = OperationalError("stmt", {}, Exception("boom"))
    with pytest.raises(SQLAlchemyError):
        record_dial_disposition(
            session, opportunity_thread_id="T9", outcome="won", as_of=AS_OF
        )
    session.rollback.assert_called_once()
    assert "record_dial_disposition failed" in caplog.text


# --- cron driver -----------------------------------------------------------

def _dial_list(n: int) -> DialList:
    return DialList(
        generated_for=AS_OF, entries=[], candidate_count=n,
        config_version="wp9-1.0.0",
    )


@contextmanager
def _fake_ctx():
    yield MagicMock()


def test_cron_dry_run_generates_without_posting(monkeypatch):
    from src.tasks import dial_list_daily

    gen = MagicMock(return_value=_dial_list(3))
    deliver = MagicMock()
    monkeypatch.setattr(dial_list_daily, "get_db_context", _fake_ctx)
    monkeypatch.setattr(dial_list_daily, "generate_dial_list", gen)
    monkeypatch.setattr(dial_list_daily, "generate_and_deliver", deliver)

    rc = dial_list_daily.main(["--dry-run", "--as-of", "2026-09-16"])

    assert rc == 0
    gen.assert_called_once()
    deliver.assert_not_called()  # dry-run never posts


def test_cron_live_run_delivers(monkeypatch):
    from src.tasks import dial_list_daily

    deliver = MagicMock(return_value=(_dial_list(2), "111.222"))
    gen = MagicMock(return_value=_dial_list(2))
    monkeypatch.setattr(dial_list_daily, "get_db_context", _fake_ctx)
    monkeypatch.setattr(dial_list_daily, "generate_and_deliver", deliver)
    monkeypatch.setattr(dial_list_daily, "generate_dial_list", gen)

    rc = dial_list_daily.main(["--as-of", "2026-09-16"])

    assert rc == 0
    deliver.assert_called_once()
    gen.assert_not_called()  # live path uses generate_and_deliver, not the dry seam


def test_cron_failure_returns_nonzero(monkeypatch):
    from src.tasks import dial_list_daily

    monkeypatch.setattr(dial_list_daily, "get_db_context", _fake_ctx)
    monkeypatch.setattr(
        dial_list_daily, "generate_and_deliver",
        MagicMock(side_effect=RuntimeError("kaboom")),
    )
    rc = dial_list_daily.main([])
    assert rc == 1
