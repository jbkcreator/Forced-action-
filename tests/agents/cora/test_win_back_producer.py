from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.agents.cora import queue, store
from src.agents.cora.ingestion import win_back_producer


def _subscriber(**overrides):
    defaults = dict(
        id=9001, name="Jane Prospect", email="jane@example.com", phone="8135551000",
        churned_at=datetime.now(timezone.utc) - timedelta(days=10),
        last_reactivation_attempt_at=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _patch_sourcing(monkeypatch, subs, eligibility_result=(True, "eligible", "zip_held")):
    monkeypatch.setattr(
        "src.tasks.reactivation_scheduler._lapsed_subscriber_ids", lambda db: [s.id for s in subs],
    )
    monkeypatch.setattr(
        "src.tasks.reactivation_scheduler._fetch_subscribers", lambda ids, db: subs,
    )
    monkeypatch.setattr(
        "src.services.reactivation_eligibility.check_tier3_winback_eligibility",
        lambda sub, db: eligibility_result,
    )


def test_eligible_subscriber_produces_correctly_shaped_target(fresh_db, monkeypatch):
    sub = _subscriber()
    _patch_sourcing(monkeypatch, [sub])

    produced = win_back_producer.produce_win_back_targets(fresh_db)
    assert produced == ["SUB-9001"]

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    payload = published[0].payload
    assert payload["cell_id"] == "win_back"
    assert payload["buyer_entity"]["opportunity_thread_id"] == "SUB-9001"
    assert payload["buyer_entity"]["confidence_score"] == 100
    assert payload["contact_email"] == "jane@example.com"
    assert payload["contact_phone"] == "8135551000"
    assert any(f["fact_key"] == "winback_branch" for f in payload["facts_used"])


def test_ineligible_subscriber_skipped(fresh_db, monkeypatch):
    sub = _subscriber()
    _patch_sourcing(monkeypatch, [sub], eligibility_result=(False, "on_cooldown", None))

    produced = win_back_producer.produce_win_back_targets(fresh_db)
    assert produced == []


def test_recently_attempted_by_live_reactivation_system_skipped(fresh_db, monkeypatch):
    sub = _subscriber(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(days=2))
    _patch_sourcing(monkeypatch, [sub])

    produced = win_back_producer.produce_win_back_targets(fresh_db)
    assert produced == []  # 2 days ago is within the 14-day cross-system safety window


def test_attempted_outside_safety_window_is_not_skipped(fresh_db, monkeypatch):
    sub = _subscriber(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(days=30))
    _patch_sourcing(monkeypatch, [sub])

    produced = win_back_producer.produce_win_back_targets(fresh_db)
    assert produced == ["SUB-9001"]


def test_duplicate_actionable_draft_skipped(fresh_db, monkeypatch):
    sub = _subscriber()
    _patch_sourcing(monkeypatch, [sub])
    store.append_draft(fresh_db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id="SUB-9001", buyer_entity_id=sub.id,
        cell_id="win_back", offer="core_subscription", avenue="wholesalers", angle="win_back_offer",
        subject="s", body="b", facts_used=[], source_refs=[], recommended_channel="email",
        confidence_score=100,
    ))

    produced = win_back_producer.produce_win_back_targets(fresh_db)
    assert produced == []


def test_never_imports_the_forbidden_reactivation_graph():
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(win_back_producer))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            assert "src.agents.graphs" not in node.module
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "src.agents.graphs" not in alias.name
