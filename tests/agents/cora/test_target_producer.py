from __future__ import annotations

import json
import uuid

from sqlalchemy import text

from src.agents.cora import queue, store
from src.agents.cora.ingestion import target_producer
from tests.agents.cora.fixtures.whales import WHALES


def _stub_contact_and_entity(monkeypatch, whale):
    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", lambda db, tid: whale)
    monkeypatch.setattr(target_producer, "get_contact_channel", lambda db, bid: {"email": "x@example.com", "phone": None})


def _make_venture(db, *, ladder_stage: str = "portfolio", is_active: bool = True) -> tuple[str, str]:
    """A throwaway (venture_key, county_id) pair for venture_key-attribution tests."""
    venture_key = f"test_producer_{uuid.uuid4().hex[:10]}"
    county_id = f"{venture_key}_county"
    db.execute(
        text("""
            INSERT INTO ventures (venture_key, display_name, brand_name, ladder_stage, is_active)
            VALUES (:k, 'Test Producer Venture', 'Test Producer Venture', :stage, :active)
        """),
        {"k": venture_key, "stage": ladder_stage, "active": is_active},
    )
    db.execute(
        text("""
            INSERT INTO counties (county_id, display_name, venture_key, zip_prefixes, is_active)
            VALUES (:c, 'Test Producer County', :k, '[]'::jsonb, true)
        """),
        {"c": county_id, "k": venture_key},
    )
    db.flush()
    return venture_key, county_id


def test_produce_auction_fast_follow_targets_publishes_a_correctly_shaped_event(fresh_db, monkeypatch):
    whale = dict(WHALES[0], latest_auction_deed_date="2026-07-20")
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: [whale])

    produced = target_producer.produce_auction_fast_follow_targets(fresh_db)
    assert produced == [whale["opportunity_thread_id"]]

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    assert published[0].payload["cell_id"] == "auction_fast_follow"
    assert any(f["fact_key"] == "latest_auction_deed_date" for f in published[0].payload["facts_used"])
    queue.ack(published[0].message_id)


def test_produce_auction_fast_follow_targets_skips_thread_with_active_draft(fresh_db, monkeypatch):
    # This producer's only dedup is store.has_duplicate_actionable_draft — the
    # idempotency_key set at publish time is consumed by the WORKER, not here,
    # so re-sweeping with no draft yet persisted legitimately republishes.
    whale = dict(WHALES[0], latest_auction_deed_date="2026-07-20")
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: [whale])

    store.append_draft(fresh_db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=whale["opportunity_thread_id"],
        buyer_entity_id=whale["id"], cell_id="auction_fast_follow", offer="core_subscription",
        avenue="flippers", angle="auction_congrats", subject="s", body="b",
        facts_used=[], source_refs=[], recommended_channel="email",
        confidence_score=whale["confidence_score"],
    ))

    produced = target_producer.produce_auction_fast_follow_targets(fresh_db)
    assert produced == []


def test_produce_auction_fast_follow_targets_skips_unresolvable_entity(fresh_db, monkeypatch):
    whale = dict(WHALES[1], latest_auction_deed_date="2026-07-21")
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: [whale])
    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", lambda db, tid: None)

    produced = target_producer.produce_auction_fast_follow_targets(fresh_db)
    assert produced == []


def test_produce_auction_fast_follow_targets_never_calls_refresh_whale_flags():
    # Structural guarantee: this producer must stay read-only — it must never
    # import/trigger whale_auction_fast_follow.py's own write path.
    import inspect

    source = inspect.getsource(target_producer)
    assert "refresh_whale_flags" not in source
    assert "run_whale_fast_follow" not in source


# ── venture_key attribution (CLONE-v2.2 / CL4) ──────────────────────────────


def test_produce_targets_publishes_a_correctly_shaped_event(fresh_db, monkeypatch):
    whale = WHALES[0]
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_ranked_whales", lambda db, **kw: [whale])

    produced = target_producer.produce_targets(fresh_db)
    assert produced == [whale["opportunity_thread_id"]]

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    assert published[0].payload["cell_id"] == "founder_tier_blitz"
    queue.ack(published[0].message_id)


def test_produce_targets_attaches_the_targets_own_venture_key(fresh_db, monkeypatch):
    """A second venture's targets must not be attributed to the default
    venture — reply rate is read off this column
    (src/services/venture_ladder.py:cell_reply_rates), so a wrong value here
    leaves the second venture's reply rate permanently empty and its
    auto-double unable to ever fire."""
    venture_key, county_id = _make_venture(fresh_db)
    whale = dict(WHALES[0], county_id=county_id)
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_ranked_whales", lambda db, **kw: [whale])

    target_producer.produce_targets(fresh_db)

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    assert published[0].payload["venture_key"] == venture_key
    queue.ack(published[0].message_id)


def test_produce_targets_falls_back_to_default_venture_when_county_unresolvable(
    fresh_db, monkeypatch
):
    from config.venture_template import DEFAULT_VENTURE_KEY

    whale = dict(WHALES[1], county_id="no_such_county_at_all")
    _stub_contact_and_entity(monkeypatch, whale)
    monkeypatch.setattr(target_producer, "get_ranked_whales", lambda db, **kw: [whale])

    target_producer.produce_targets(fresh_db)

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    assert published[0].payload["venture_key"] == DEFAULT_VENTURE_KEY
    queue.ack(published[0].message_id)


def test_produce_targets_applies_the_cell_production_multiplier_to_the_limit(
    fresh_db, monkeypatch
):
    """Issue: cell_production_multipliers() was implemented but never applied
    to a producer's limit, so a qualifying cell's auto-double was a no-op in
    production. Two recorded cell-level auto-doubles for founder_tier_blitz
    means a 2**2 = 4x multiplier."""
    venture_key, county_id = _make_venture(fresh_db, ladder_stage="cell")
    for _ in range(2):
        fresh_db.execute(
            text("""
                INSERT INTO venture_ladder_events (
                    venture_key, from_stage, to_stage, decision, gate_results, actor
                ) VALUES (:k, 'cell', 'cell', 'auto_double', CAST(:payload AS jsonb), 'test')
            """),
            {
                "k": venture_key,
                "payload": json.dumps({"scope": "cell", "cell_id": "founder_tier_blitz"}),
            },
        )
    fresh_db.flush()

    captured: dict = {}

    def _fake_get_ranked_whales(db, **kw):
        captured.update(kw)
        return []

    monkeypatch.setattr(target_producer, "get_ranked_whales", _fake_get_ranked_whales)

    target_producer.produce_targets(fresh_db, limit=25, county_id=county_id)
    assert captured["limit"] == 25 * 4


def test_produce_targets_uses_1x_multiplier_for_a_venture_with_no_auto_doubles(
    fresh_db, monkeypatch
):
    venture_key, county_id = _make_venture(fresh_db)

    captured: dict = {}

    def _fake_get_ranked_whales(db, **kw):
        captured.update(kw)
        return []

    monkeypatch.setattr(target_producer, "get_ranked_whales", _fake_get_ranked_whales)

    target_producer.produce_targets(fresh_db, limit=25, county_id=county_id)
    assert captured["limit"] == 25
