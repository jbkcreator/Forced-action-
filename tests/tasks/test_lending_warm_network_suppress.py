"""Josh's warm-network CSV loader (F8): dry run by default, --apply commits."""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

PHONE = "+18135559901"


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _write_csv(tmp_path, phone):
    path = tmp_path / "warm_network.csv"
    path.write_text(f"name,phone\nJane Roe,{phone}\n", encoding="utf-8")
    return path


def test_dry_run_reports_without_writing(tmp_path, monkeypatch, db, caplog):
    from src.tasks import lending_warm_network_suppress as mod

    class _Ctx:
        def __enter__(self):
            return db

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(mod, "get_db_context", lambda: _Ctx())
    csv_path = _write_csv(tmp_path, PHONE)
    with caplog.at_level("INFO"):
        assert mod.main(["--input", str(csv_path)]) == 0
    assert "would suppress 1" in caplog.text
    count = db.execute(text("SELECT count(*) FROM lending.suppression_list WHERE phone = :p"), {"p": PHONE}).scalar()
    assert count == 0


def test_apply_commits_the_suppression(tmp_path, monkeypatch):
    """Uses its own connection (not the rollback-bound ``db`` fixture): main()'s own
    ``session.commit()`` would consume the fixture's externally-managed transaction,
    the same issue finding #12's atomicity test hit. Cleans up after itself."""
    from src.tasks import lending_warm_network_suppress as mod

    engine = create_engine(os.environ["DATABASE_URL"])
    try:
        class _Ctx:
            def __enter__(self):
                self._session = Session(bind=engine)
                return self._session

            def __exit__(self, *exc):
                self._session.close()
                return False

        monkeypatch.setattr(mod, "get_db_context", lambda: _Ctx())
        csv_path = _write_csv(tmp_path, PHONE)
        assert mod.main(["--input", str(csv_path), "--apply"]) == 0
        with engine.connect() as conn:
            reason = conn.execute(
                text("SELECT reason FROM lending.suppression_list WHERE phone = :p"), {"p": PHONE}
            ).scalar()
        assert reason == "WARM_NETWORK"
    finally:
        with engine.begin() as cleanup_conn:
            cleanup_conn.execute(text("DELETE FROM lending.suppression_list WHERE phone = :p"), {"p": PHONE})
        engine.dispose()
