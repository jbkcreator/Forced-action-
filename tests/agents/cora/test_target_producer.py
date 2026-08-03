from __future__ import annotations

import json
import uuid

from sqlalchemy import text

from config.venture_template import DEFAULT_VENTURE_KEY
from src.agents.cora import queue, store
from src.agents.cora.ingestion import target_producer
from tests.agents.cora.fixtures.whales import WHALES


def _seed_cell_auto_doubles(db, venture_key: str, cell_id: str, count: int) -> None:
    for _ in range(count):
        db.execute(
            text("""
                INSERT INTO venture_ladder_events (
                    venture_key, from_stage, to_stage, decision, gate_results, actor
                ) VALUES (:k, 'cell', 'cell', 'auto_double', CAST(:payload AS jsonb), 'test')
            """),
            {"k": venture_key, "payload": json.dumps({"scope": "cell", "cell_id": cell_id})},
        )
    db.flush()


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
    _seed_cell_auto_doubles(fresh_db, venture_key, "founder_tier_blitz", count=2)

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


# ── fleet-wide (scheduled) sweep applies EACH venture's own multiplier ──────
#
# Regression: run_periodic()/--produce-targets (the only production callers)
# always call produce_targets()/produce_auction_fast_follow_targets() with
# county_id=None. Before this, that meant _cell_production_limit resolved
# None -> DEFAULT_VENTURE_KEY and read ONLY that venture's multiplier — a
# non-default venture's own recorded cell auto-double was silently never
# read by the scheduled process at all, even though a county_id-scoped call
# (as in the tests above) correctly picked it up. These drive the actual
# `produce_targets(fresh_db)` / `produce_auction_fast_follow_targets(fresh_db)`
# entry points the scheduler uses — no county_id passed anywhere.


def _fake_row(thread_id: str, county_id: str) -> dict:
    return {
        "opportunity_thread_id": thread_id,
        "county_id": county_id,
        "total_purchase_count": 5,
        "total_cash_volume": 100_000.0,
        "whale_flagged_at": None,
    }


def test_produce_targets_fleet_wide_sweep_applies_the_non_default_ventures_own_multiplier(
    fresh_db, monkeypatch
):
    from src.services import venture_ladder as vl

    venture_key, county_id = _make_venture(fresh_db, ladder_stage="cell")
    _seed_cell_auto_doubles(fresh_db, venture_key, target_producer.FOUNDER_TIER_BLITZ_CELL_ID, count=2)

    second_multiplier = vl.cell_production_multipliers(fresh_db, venture_key).get(
        target_producer.FOUNDER_TIER_BLITZ_CELL_ID, 1
    )
    assert second_multiplier == 4  # sanity: the two seeded doublings took effect
    default_multiplier = vl.cell_production_multipliers(fresh_db, DEFAULT_VENTURE_KEY).get(
        target_producer.FOUNDER_TIER_BLITZ_CELL_ID, 1
    )

    limit = 3
    # Over-supply BOTH ventures past the highest possible cap
    # (limit * AUTO_DOUBLE_CELL_MAX_MULTIPLIER) so the multiplier cap binds,
    # not the row count.
    supply_per_venture = limit * 5
    fake_rows = (
        [_fake_row(f"OPP-DEFAULT-{i:03d}", "hillsborough") for i in range(supply_per_venture)]
        + [_fake_row(f"OPP-SECOND-{i:03d}", county_id) for i in range(supply_per_venture)]
    )
    monkeypatch.setattr(target_producer, "get_ranked_whales", lambda db, **kw: fake_rows)

    def _fake_buyer_entity(db, tid):
        cid = county_id if tid.startswith("OPP-SECOND") else "hillsborough"
        return {"id": 1, "opportunity_thread_id": tid, "county_id": cid, "confidence_score": 90}

    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", _fake_buyer_entity)
    monkeypatch.setattr(target_producer, "get_contact_channel", lambda db, bid: {"email": "x@example.com", "phone": None})

    target_producer.produce_targets(fresh_db, limit=limit)  # county_id=None — the real scheduler's call shape

    published = queue.read_batch("test-consumer", count=100, block_ms=200)
    by_venture: dict = {}
    for msg in published:
        by_venture[msg.payload["venture_key"]] = by_venture.get(msg.payload["venture_key"], 0) + 1
        queue.ack(msg.message_id)

    assert by_venture.get(venture_key, 0) == limit * second_multiplier
    assert by_venture.get(DEFAULT_VENTURE_KEY, 0) == limit * default_multiplier


def test_produce_targets_fleet_wide_fetch_size_covers_all_ventures_caps(
    fresh_db, monkeypatch
):
    """Regression: fleet SQL LIMIT must be sum(multipliers)*limit, not MAX_MULTIPLIER*limit.

    With default-venture rows ranked first, the old MAX_MULTIPLIER*limit fetch window fills
    entirely with default rows and the second venture receives zero candidates despite having
    earned a 4x multiplier. The sum-based fetch extends the window to include second rows.
    """
    from config.venture_ladder import AUTO_DOUBLE_CELL_MAX_MULTIPLIER

    venture_key, county_id = _make_venture(fresh_db, ladder_stage="cell")
    _seed_cell_auto_doubles(fresh_db, venture_key, target_producer.FOUNDER_TIER_BLITZ_CELL_ID, count=2)

    limit = 3
    # Fill the old fetch window with default-venture rows only, then append second-venture rows.
    max_fetch_old = limit * AUTO_DOUBLE_CELL_MAX_MULTIPLIER  # = 12
    default_rows = [_fake_row(f"OPP-FFSZ-DEF-{i:03d}", "hillsborough") for i in range(max_fetch_old)]
    second_rows = [_fake_row(f"OPP-FFSZ-SEC-{i:03d}", county_id) for i in range(limit * 4)]
    all_rows = default_rows + second_rows

    # Honor the limit kwarg — mirrors what the SQL LIMIT clause does.
    monkeypatch.setattr(target_producer, "get_ranked_whales", lambda db, **kw: all_rows[: kw["limit"]])

    def _fake_buyer_entity(db, tid):
        cid = county_id if "SEC" in tid else "hillsborough"
        return {"id": 1, "opportunity_thread_id": tid, "county_id": cid, "confidence_score": 90}

    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", _fake_buyer_entity)
    monkeypatch.setattr(target_producer, "get_contact_channel", lambda db, bid: {"email": "x@example.com", "phone": None})

    target_producer.produce_targets(fresh_db, limit=limit)

    published = queue.read_batch("test-consumer", count=100, block_ms=200)
    second_count = sum(1 for m in published if m.payload["venture_key"] == venture_key)
    for m in published:
        queue.ack(m.message_id)

    # With the old MAX_MULTIPLIER fetch: all 12 slots occupied by default rows →
    # second_count == 0. With the sum-based fetch (1+4)*3=15: second rows enter
    # the window → second_count > 0.
    assert second_count > 0, (
        "second venture got 0 targets — fleet fetch size must be sum(multipliers)*limit"
    )


def test_produce_auction_fast_follow_targets_fleet_wide_sweep_applies_the_non_default_ventures_own_multiplier(
    fresh_db, monkeypatch
):
    from src.services import venture_ladder as vl

    venture_key, county_id = _make_venture(fresh_db, ladder_stage="cell")
    _seed_cell_auto_doubles(fresh_db, venture_key, target_producer.AUCTION_FAST_FOLLOW_CELL_ID, count=1)

    second_multiplier = vl.cell_production_multipliers(fresh_db, venture_key).get(
        target_producer.AUCTION_FAST_FOLLOW_CELL_ID, 1
    )
    assert second_multiplier == 2
    default_multiplier = vl.cell_production_multipliers(fresh_db, DEFAULT_VENTURE_KEY).get(
        target_producer.AUCTION_FAST_FOLLOW_CELL_ID, 1
    )

    limit = 2
    supply_per_venture = limit * 5
    fake_rows = (
        [_fake_row(f"OPP-AFF-DEFAULT-{i:03d}", "hillsborough") for i in range(supply_per_venture)]
        + [_fake_row(f"OPP-AFF-SECOND-{i:03d}", county_id) for i in range(supply_per_venture)]
    )
    monkeypatch.setattr(target_producer, "get_recent_auction_fast_follow_whales", lambda db, **kw: fake_rows)

    def _fake_buyer_entity(db, tid):
        cid = county_id if tid.startswith("OPP-AFF-SECOND") else "hillsborough"
        return {"id": 1, "opportunity_thread_id": tid, "county_id": cid, "confidence_score": 90}

    monkeypatch.setattr(target_producer, "get_buyer_entity_by_opportunity_thread_id", _fake_buyer_entity)
    monkeypatch.setattr(target_producer, "get_contact_channel", lambda db, bid: {"email": "x@example.com", "phone": None})

    target_producer.produce_auction_fast_follow_targets(fresh_db, limit=limit)  # county_id=None

    published = queue.read_batch("test-consumer", count=100, block_ms=200)
    by_venture: dict = {}
    for msg in published:
        by_venture[msg.payload["venture_key"]] = by_venture.get(msg.payload["venture_key"], 0) + 1
        queue.ack(msg.message_id)

    assert by_venture.get(venture_key, 0) == limit * second_multiplier
    assert by_venture.get(DEFAULT_VENTURE_KEY, 0) == limit * default_multiplier
