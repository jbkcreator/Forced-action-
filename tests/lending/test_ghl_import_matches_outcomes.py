"""The GHL call import attaches waiting GHL outcomes once the calls are stored, then delivers them (G3). Pure."""
from __future__ import annotations

from contextlib import contextmanager

from src.lending import ghl_call_import as imp


def test_main_matches_after_the_import_and_delivers_after_the_commit(monkeypatch):
    from config.settings import get_settings
    from src.lending import db as lending_db_mod, ghl_account, ghl_disposition_match

    order = []
    monkeypatch.setattr(get_settings(), "lending_ghl_call_import_enabled", True, raising=False)
    monkeypatch.setattr(ghl_account, "lending_ghl_account", lambda: object())

    @contextmanager
    def session():
        yield "db"
        order.append("commit")
    monkeypatch.setattr(lending_db_mod, "lending_session", session)
    monkeypatch.setattr(imp, "import_calls", lambda db, *a, **k: order.append("import") or imp.ImportStats())
    monkeypatch.setattr(ghl_disposition_match, "attach_pending", lambda db, now: order.append("match") or [5, 6])
    monkeypatch.setattr(ghl_disposition_match, "deliver_matched", lambda ids: order.append(("deliver", list(ids))))
    monkeypatch.setattr("src.lending.ghl_calls.call_messages_reader", lambda acct: None)
    monkeypatch.setattr("src.lending.ghl_calls.recent_callers_reader", lambda acct: None)

    assert imp.main() == 0
    assert order == ["import", "match", "commit", ("deliver", [5, 6])]
