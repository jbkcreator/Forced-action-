"""Finding #4 regression: --live-trace must never run as (or inside) a dry run."""
from __future__ import annotations

import pytest


def test_live_trace_without_apply_is_rejected_before_touching_the_db(monkeypatch):
    from src.tasks import property_radar_lead_handoff as mod

    def _must_not_be_called():
        pytest.fail("get_db_context must not be entered: --live-trace without --apply "
                    "must be rejected before any DB session (or Tracerfy charge) happens")

    monkeypatch.setattr(mod, "get_db_context", _must_not_be_called)
    with pytest.raises(ValueError, match="needs --apply"):
        mod.run(live_trace=True, apply=False)


def test_live_trace_with_apply_is_allowed_through_the_gate(monkeypatch):
    from src.tasks import property_radar_lead_handoff as mod

    calls = {"live_trace": False}

    class _FakeSession:
        def commit(self):
            pass

        def rollback(self):
            pass

    class _Ctx:
        def __enter__(self):
            return _FakeSession()

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(mod, "get_db_context", lambda: _Ctx())
    monkeypatch.setattr(mod, "iter_staged_leads", lambda *a, **k: iter([]))
    monkeypatch.setattr(mod, "_live_trace", lambda session, campaign, *, thin_path_only: (
        calls.__setitem__("live_trace", True) or {}
    ))
    mod.run(live_trace=True, apply=True)
    assert calls["live_trace"] is True
