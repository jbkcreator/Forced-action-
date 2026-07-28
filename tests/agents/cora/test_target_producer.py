from __future__ import annotations

from unittest.mock import MagicMock

from src.agents.cora import queue, store
from src.agents.cora.ingestion import target_producer
from tests.agents.cora.fixtures.whales import WHALES


def _stub_contact_and_entity(monkeypatch, whale):
    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", lambda db, tid: whale)
    monkeypatch.setattr(target_producer, "get_contact_channel", lambda db, bid: {"email": "x@example.com", "phone": None})


def test_produce_auction_fast_follow_targets_publishes_a_correctly_shaped_event(monkeypatch):
    whale = dict(WHALES[0], latest_auction_deed_date="2026-07-20")
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: [whale])

    produced = target_producer.produce_auction_fast_follow_targets(MagicMock())
    assert produced == [whale["opportunity_thread_id"]]

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    assert published[0].payload["cell_id"] == "auction_fast_follow"
    assert any(f["fact_key"] == "latest_auction_deed_date" for f in published[0].payload["facts_used"])
    queue.ack(published[0].message_id)


def test_produce_auction_fast_follow_targets_skips_thread_with_active_draft(monkeypatch):
    # This producer's only dedup is store.has_duplicate_actionable_draft — the
    # idempotency_key set at publish time is consumed by the WORKER, not here,
    # so re-sweeping with no draft yet persisted legitimately republishes.
    whale = dict(WHALES[0], latest_auction_deed_date="2026-07-20")
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: [whale])

    store.append_draft(store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=whale["opportunity_thread_id"],
        buyer_entity_id=whale["id"], cell_id="auction_fast_follow", offer="core_subscription",
        avenue="flippers", angle="auction_congrats", subject="s", body="b",
        facts_used=[], source_refs=[], recommended_channel="email",
        confidence_score=whale["confidence_score"],
    ))

    produced = target_producer.produce_auction_fast_follow_targets(MagicMock())
    assert produced == []


def test_produce_auction_fast_follow_targets_skips_unresolvable_entity(monkeypatch):
    whale = dict(WHALES[1], latest_auction_deed_date="2026-07-21")
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: [whale])
    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", lambda db, tid: None)

    produced = target_producer.produce_auction_fast_follow_targets(MagicMock())
    assert produced == []


def test_produce_auction_fast_follow_targets_never_calls_refresh_whale_flags():
    # Structural guarantee: this producer must stay read-only — it must never
    # import/trigger whale_auction_fast_follow.py's own write path.
    import inspect

    source = inspect.getsource(target_producer)
    assert "refresh_whale_flags" not in source
    assert "run_whale_fast_follow" not in source
