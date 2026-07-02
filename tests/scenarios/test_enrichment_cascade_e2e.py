"""
Enrichment Cascade v4 — E2E Stage Tests (ADR 0016 / ADR 0017).

Runs against the real Postgres DATABASE_URL (the same DB the prod scoring run
writes to). Every test seeds its own rows under the sentinel county_id
"ztest-cascade" so the cascade and the scoring trigger only ever see seeded
fixtures — nothing in the live county is read or mutated. All seeded rows are
deleted in teardown (FK-safe order).

External paid vendors (Tracerfy, BatchData, IDI, PDL) are patched with
DB-backed side effects so each stage's persistence + accounting runs for real
against Postgres, deterministically and at zero spend. Patch targets match the
*local* import sites inside the implementation (every vendor + run_cascade is
imported inside its function, so we patch the source module, not the consumer).

Coverage
  cascade    — no-candidates, all-keys-absent graceful skip, stop-at-hit,
               miss→batchdata fallthrough, entity-skip→address-only,
               cost-ceiling block, triangulation once at end (ADR 0015),
               voter phone never promoted (ADR 0013), direct-mail on terminal miss
  trigger    — Gold+ INSERT detected, Silver→Gold intraday detected,
               Gold→Platinum suppressed (already Gold+), real score_all_properties
               emit path gated by enrichment_cascade_enabled (on + off)
  batcher    — dedup, size flush, timer flush, missing-property_id warning,
               flush-failure isolation
  supervisor — gold_lead_scored routed to batcher, uninitialized-batcher drop
  e2e        — publish gold_lead_scored → supervisor → batcher → run_cascade

Run:
    pytest tests/scenarios/test_enrichment_cascade_e2e.py -v -m scenario
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    DistressScore,
    EnrichedContact,
    Foreclosure,
    Owner,
    Property,
    Voter,
)

pytestmark = pytest.mark.scenario

_COUNTY  = "ztest-cascade"
_PHONE   = "+18135550101"
_ALT_PHONE = "+18135550202"

# Patch targets — every vendor + run_cascade is imported *inside* the function
# that uses it, so we patch the symbol in its source module.
_TRACERFY_SRC  = "src.services.tracerfy_fallback.run_tracerfy_fallback"
_BATCHDATA_SRC = "src.services.skip_trace.run_skip_trace"
_PDL_SRC       = "src.services.pdl_skip_trace.run_pdl_lookup"
_IDI_SRC       = "src.services.idi_fallback.run_idi_fallback"
_DIRECTMAIL_SRC = "src.services.direct_mail.flag_direct_mail_eligible"
_RUN_CASCADE_SRC = "src.services.skip_trace_waterfall.run_cascade"
_PUBLISH_SRC   = "src.agents.events.ingestion.publish_cora_event"
_GATE_SRC      = "src.services.enrichment_router.is_paid_enrichment_allowed"

_PHONE_META = {"phone_1": {"score": 85, "reachable": True, "type": "mobile"}}


# ── seed / teardown ───────────────────────────────────────────────────────────

def _naive() -> datetime:
    # score_date / enriched_at are TIMESTAMP WITHOUT TIME ZONE — store naive UTC
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _seed_property(session, *, name: str = "JOHN A DOE", is_llc: bool = False,
                   seed_score: bool = True) -> dict:
    uid  = uuid.uuid4().hex[:10]
    prop = Property(
        parcel_id=f"ZTESTCASC-{uid}", address="123 Test Ave",
        city="Tampa", state="FL", zip="33601", county_id=_COUNTY,
    )
    session.add(prop)
    session.flush()

    owner = Owner(
        property_id=prop.id,
        owner_name=(f"LLC HOLDINGS {uid[:6]}" if is_llc else name),
        county_id=_COUNTY,
        phone_1=None,
    )
    session.add(owner)
    if seed_score:
        session.add(DistressScore(
            property_id=prop.id, lead_tier="Gold", final_cds_score=65.0,
            score_date=_naive(), county_id=_COUNTY,
        ))
    session.flush()
    return {"property_id": prop.id, "owner_id": owner.id}


def _cleanup(property_id: int) -> None:
    with get_db_context() as s:
        prospect = s.execute(text(
            "SELECT prospect_id FROM prospects WHERE property_id = :p"
        ), {"p": property_id}).fetchone()
        if prospect:
            s.execute(text(
                "DELETE FROM processed_events WHERE event_id IN "
                "(SELECT event_id FROM events WHERE prospect_id = :pid)"
            ), {"pid": prospect.prospect_id})
            s.execute(text("DELETE FROM events WHERE prospect_id = :pid"), {"pid": prospect.prospect_id})
            s.execute(text("DELETE FROM prospects WHERE prospect_id = :pid"), {"pid": prospect.prospect_id})
        for tbl in ("algorithmic_variance_log", "enrichment_usage_logs", "voters",
                    "enriched_contacts", "distress_scores", "foreclosures", "owners"):
            s.execute(text(f"DELETE FROM {tbl} WHERE property_id = :p"),  # noqa: S608 — fixed names
                      {"p": property_id})
        s.execute(text("DELETE FROM properties WHERE id = :p"), {"p": property_id})
        s.commit()


@pytest.fixture
def seeded():
    with get_db_context() as s:
        ids = _seed_property(s)
        s.commit()
    yield ids
    _cleanup(ids["property_id"])


@pytest.fixture
def seeded_llc():
    with get_db_context() as s:
        ids = _seed_property(s, is_llc=True)
        s.commit()
    yield ids
    _cleanup(ids["property_id"])


@pytest.fixture
def cascade_keys():
    """Make the paid-vendor keys truthy so each stage's branch is entered.

    IDI stays unkeyed on purpose — it is key-gated (ADR 0017) and must remain a
    clean no-op until a contracted rate exists. Vendor functions are patched, so
    the keys are only ever read for their truthiness gate.
    """
    prev = (
        settings.tracerfy_api_key,
        settings.batch_skip_tracing_api_key,
        settings.pdl_api_key,
        settings.idi_api_key,
    )
    settings.tracerfy_api_key           = SecretStr("test-tracerfy")
    settings.batch_skip_tracing_api_key = SecretStr("test-batchdata")
    settings.pdl_api_key                = SecretStr("test-pdl")
    settings.idi_api_key                = None
    yield
    (settings.tracerfy_api_key, settings.batch_skip_tracing_api_key,
     settings.pdl_api_key, settings.idi_api_key) = prev


# ── vendor side-effects (DB-backed, deterministic) ───────────────────────────

def _tracerfy_effect(hit_ids: set[int], skip_ids: set[int] | None = None):
    """side_effect for run_tracerfy_fallback — writes hit/miss tracerfy EC rows.

    trace_type='normal'   : entity owners in skip_ids are returned as
                            entity_skip_ids (no EC row), mirroring the real
                            entity-classification skip.
    trace_type='advanced' : Address-Only pass — no entity skip; a hit writes
                            (or, with retrace_misses, updates) the tracerfy row.
    """
    _skip = skip_ids or set()

    def _effect(owner_ids=None, county_id=_COUNTY, trace_type="normal",
                retrace_misses=False, **_kw):
        entity_skip_ids: list[int] = []
        with get_db_context() as s:
            for oid in (owner_ids or []):
                owner = s.get(Owner, oid)
                if not owner:
                    continue
                if trace_type == "normal" and oid in _skip:
                    entity_skip_ids.append(oid)
                    continue
                is_hit = oid in hit_ids
                existing = (
                    s.query(EnrichedContact)
                    .filter_by(property_id=owner.property_id, source="tracerfy")
                    .first()
                )
                if retrace_misses and existing:
                    existing.match_success = is_hit
                    existing.mobile_phone  = _PHONE if is_hit else None
                    existing.confidence    = 0.85 if is_hit else 0.0
                elif not existing:
                    s.add(EnrichedContact(
                        property_id=owner.property_id,
                        county_id=owner.county_id or _COUNTY,
                        source="tracerfy", match_success=is_hit,
                        mobile_phone=_PHONE if is_hit else None,
                        confidence=0.85 if is_hit else 0.0, enriched_at=_naive(),
                    ))
                if is_hit and not owner.phone_1:
                    owner.phone_1       = _PHONE
                    owner.phone_metadata = dict(_PHONE_META)
            s.commit()
        return {"entity_skip_ids": entity_skip_ids,
                "hits": len(hit_ids & set(owner_ids or [])), "misses": 0}

    return _effect


def _batchdata_effect(hit_ids: set[int]):
    def _effect(owner_ids=None, county_id=_COUNTY, **_kw):
        with get_db_context() as s:
            for oid in (owner_ids or []):
                owner = s.get(Owner, oid)
                if not owner:
                    continue
                is_hit = oid in hit_ids
                s.add(EnrichedContact(
                    property_id=owner.property_id,
                    county_id=owner.county_id or _COUNTY,
                    source="batch_skip_tracing", match_success=is_hit,
                    mobile_phone=_PHONE if is_hit else None,
                    confidence=0.82 if is_hit else 0.0, enriched_at=_naive(),
                ))
                if is_hit and not owner.phone_1:
                    owner.phone_1       = _PHONE
                    owner.phone_metadata = dict(_PHONE_META)
            s.commit()
        return {"hits": len(hit_ids), "misses": 0}
    return _effect


def _pdl_result(*, hit: bool, mailing_address: str | None = None):
    r = MagicMock()
    r.success         = hit
    r.confidence      = 0.90 if hit else 0.0
    r.mobile_phone    = _PHONE if hit else None
    r.landline        = None
    r.email           = None
    r.mailing_address = mailing_address
    r.cost_cents      = 28 if hit else 0
    r.error           = None
    return r


# ── Section 1 — run_cascade() integration ────────────────────────────────────

def test_cascade_no_candidates_returns_empty_stats():
    from src.services.skip_trace_waterfall import run_cascade
    stats = run_cascade(county_id="ztest-empty-county-xyz", limit=10)
    assert stats.total_leads == 0
    assert stats.hits == 0
    assert stats.misses == 0
    assert stats.total_cost_cents == 0
    assert "tracerfy" in stats.per_provider


def test_cascade_all_keys_absent_skips_gracefully(seeded):
    from src.services.skip_trace_waterfall import run_cascade
    prev = (settings.tracerfy_api_key, settings.batch_skip_tracing_api_key,
            settings.pdl_api_key, settings.idi_api_key)
    try:
        settings.tracerfy_api_key = settings.batch_skip_tracing_api_key = None
        settings.pdl_api_key = settings.idi_api_key = None
        stats = run_cascade(county_id=_COUNTY, owner_ids=[seeded["owner_id"]])
    finally:
        (settings.tracerfy_api_key, settings.batch_skip_tracing_api_key,
         settings.pdl_api_key, settings.idi_api_key) = prev

    assert stats.total_leads == 1
    assert stats.hits == 0
    assert stats.misses == 1


def test_cascade_stops_at_tracerfy_hit(seeded, cascade_keys):
    from src.services.skip_trace_waterfall import run_cascade
    oid = seeded["owner_id"]

    with patch(_TRACERFY_SRC, side_effect=_tracerfy_effect({oid})), \
         patch(_BATCHDATA_SRC) as mock_bd, \
         patch(_IDI_SRC, return_value={"skipped": True}) as mock_idi, \
         patch(_PDL_SRC) as mock_pdl:
        stats = run_cascade(county_id=_COUNTY, owner_ids=[oid])

    assert stats.hits == 1
    assert stats.total_leads == 1
    assert stats.per_provider["tracerfy"]["hits"] == 1
    assert stats.total_cost_cents == 2          # $0.02 Tracerfy hit, nothing else
    mock_bd.assert_not_called()
    mock_idi.assert_not_called()
    mock_pdl.assert_not_called()


def test_cascade_tracerfy_miss_falls_through_to_batchdata(seeded, cascade_keys):
    from src.services.skip_trace_waterfall import run_cascade
    oid = seeded["owner_id"]

    with patch(_TRACERFY_SRC, side_effect=_tracerfy_effect(set())), \
         patch(_BATCHDATA_SRC, side_effect=_batchdata_effect({oid})), \
         patch(_IDI_SRC, return_value={"skipped": True}), \
         patch(_PDL_SRC) as mock_pdl:
        stats = run_cascade(county_id=_COUNTY, owner_ids=[oid])

    assert stats.hits == 1
    assert stats.per_provider["batchdata"]["hits"] == 1
    assert stats.total_cost_cents == 7          # BatchData always charged $0.07
    mock_pdl.assert_not_called()


def test_cascade_entity_skip_routes_to_address_only(seeded_llc, cascade_keys):
    """An LLC entity-skipped by Tracerfy Standard is recovered by Address-Only."""
    from src.services.skip_trace_waterfall import run_cascade
    oid = seeded_llc["owner_id"]
    seen_trace_types: list[str] = []

    base_effect = _tracerfy_effect(hit_ids={oid}, skip_ids={oid})

    def _tracking_effect(*a, **kw):
        seen_trace_types.append(kw.get("trace_type", "normal"))
        return base_effect(*a, **kw)

    with patch(_TRACERFY_SRC, side_effect=_tracking_effect), \
         patch(_BATCHDATA_SRC) as mock_bd, \
         patch(_IDI_SRC, return_value={"skipped": True}), \
         patch(_PDL_SRC):
        stats = run_cascade(county_id=_COUNTY, owner_ids=[oid])

    assert "normal" in seen_trace_types, "Tracerfy Standard not called"
    assert "advanced" in seen_trace_types, "Address-Only not attempted for entity-skip"
    assert stats.hits == 1
    assert stats.per_provider["tracerfy_advanced"]["hits"] == 1
    mock_bd.assert_not_called()


def test_cascade_respects_cost_ceiling(seeded, cascade_keys):
    """Ceiling below the deep-tier cost blocks PDL even though its key is set."""
    from src.services.skip_trace_waterfall import run_cascade
    oid = seeded["owner_id"]

    prev = settings.skip_trace_cost_ceiling_cents
    try:
        settings.skip_trace_cost_ceiling_cents = 9   # 0(tracerfy miss)+7(bd)=7; PDL 28 would exceed
        with patch(_TRACERFY_SRC, side_effect=_tracerfy_effect(set())), \
             patch(_BATCHDATA_SRC, side_effect=_batchdata_effect(set())), \
             patch(_IDI_SRC, return_value={"skipped": True}), \
             patch(_PDL_SRC) as mock_pdl:
            run_cascade(county_id=_COUNTY, owner_ids=[oid])
    finally:
        settings.skip_trace_cost_ceiling_cents = prev

    mock_pdl.assert_not_called()


def test_cascade_triangulation_runs_once_at_end(seeded, cascade_keys):
    from src.services.skip_trace_waterfall import run_cascade
    oid = seeded["owner_id"]

    prev = settings.triangulation_enabled
    settings.triangulation_enabled = True
    try:
        with patch(_TRACERFY_SRC, side_effect=_tracerfy_effect({oid})), \
             patch(_BATCHDATA_SRC), \
             patch(_IDI_SRC, return_value={"skipped": True}), \
             patch(_PDL_SRC), \
             patch("src.services.skip_trace_waterfall._triangulate_owner") as mock_tri:
            run_cascade(county_id=_COUNTY, owner_ids=[oid])
    finally:
        settings.triangulation_enabled = prev

    assert mock_tri.call_count == 1            # once per hit owner at cascade end
    assert mock_tri.call_args.args[1] == oid


def test_cascade_voter_phone_never_promoted(seeded, cascade_keys):
    """ADR 0013: a voter phone seeds triangulation but never lands in owners.phone_1."""
    from src.services.skip_trace_waterfall import run_cascade
    pid, oid = seeded["property_id"], seeded["owner_id"]

    with get_db_context() as s:
        s.add(Voter(
            property_id=pid, county_id=_COUNTY,
            source_voter_id=f"ZTV{uuid.uuid4().hex[:8]}",
            voter_name="JOHN A DOE", registration_status="ACT",
            phone_1=_ALT_PHONE,
        ))
        s.commit()

    with patch(_TRACERFY_SRC, side_effect=_tracerfy_effect(set())), \
         patch(_BATCHDATA_SRC, side_effect=_batchdata_effect(set())), \
         patch(_IDI_SRC, return_value={"skipped": True}), \
         patch(_PDL_SRC, return_value=_pdl_result(hit=False)), \
         patch(_DIRECTMAIL_SRC, return_value=False):
        run_cascade(county_id=_COUNTY, owner_ids=[oid])

    with get_db_context() as s:
        phone = s.execute(text("SELECT phone_1 FROM owners WHERE id = :o"),
                          {"o": oid}).scalar()
    assert phone is None, f"voter phone leaked into owners.phone_1: {phone!r}"


def test_cascade_flags_direct_mail_on_terminal_miss(seeded, cascade_keys):
    from src.services.skip_trace_waterfall import run_cascade
    pid, oid = seeded["property_id"], seeded["owner_id"]

    with patch(_TRACERFY_SRC, side_effect=_tracerfy_effect(set())), \
         patch(_BATCHDATA_SRC, side_effect=_batchdata_effect(set())), \
         patch(_IDI_SRC, return_value={"skipped": True}), \
         patch(_PDL_SRC, return_value=_pdl_result(hit=False,
               mailing_address="123 Test Ave Tampa FL 33601")), \
         patch(_DIRECTMAIL_SRC, return_value=True) as mock_dm:
        stats = run_cascade(county_id=_COUNTY, owner_ids=[oid])

    assert stats.hits == 0
    assert stats.misses == 1
    mock_dm.assert_called_once()
    assert mock_dm.call_args.args[0] == pid


# ── Section 2 — scoring trigger (Gold+ detection in real _persist_score_batch) ─

def _scored_item(property_id: int, tier: str) -> dict:
    return {
        "property_id":     property_id,
        "final_cds_score": 65.0 if tier in ("Gold", "Platinum", "Ultra Platinum") else 45.0,
        "lead_tier":       tier,
        "urgency_level":   "High",
        "qualified":       True,
        "county_id":       _COUNTY,
        "zip":             "33601",
        "factor_scores":   {},
        "vertical_scores": {"wholesalers": 65.0},
        "distress_types":  [],
        "ghl_contact_id":  None,
    }


def _persist(pid: int, tier: str) -> dict:
    from src.services.cds_engine import MultiVerticalScorer
    now    = datetime.now(timezone.utc)
    tod    = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    tmrw   = tod + timedelta(days=1)
    with get_db_context() as s:
        result = MultiVerticalScorer(s)._persist_score_batch(
            [_scored_item(pid, tier)],
            scoring_run_id=int(now.timestamp()),
            today_start=tod, tomorrow_start=tmrw,
        )
        s.commit()
    return result


def test_trigger_new_gold_insert_detected():
    with get_db_context() as s:
        ids = _seed_property(s, seed_score=False)
        s.commit()
    pid = ids["property_id"]
    try:
        result = _persist(pid, "Gold")
        entering = result["new_gold_plus_entering"]
        assert len(entering) == 1
        assert entering[0]["property_id"] == pid
        assert entering[0]["lead_tier"] == "Gold"
    finally:
        _cleanup(pid)


def test_trigger_intraday_silver_to_gold_detected():
    with get_db_context() as s:
        ids = _seed_property(s, seed_score=False)
        s.commit()
    pid = ids["property_id"]
    try:
        now = datetime.now(timezone.utc)
        tod = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        with get_db_context() as s:
            s.execute(text("""
                INSERT INTO distress_scores
                  (property_id, county_id, score_date, final_cds_score, lead_tier,
                   urgency_level, qualified, factor_scores, vertical_scores,
                   distress_types, scoring_run_id)
                VALUES (:p, :c, :now, 45.0, 'Silver', 'Medium', false,
                        '{}', '{}', '[]', :run)
            """), {"p": pid, "c": _COUNTY, "now": tod, "run": int(now.timestamp()) - 1})
            s.commit()

        result = _persist(pid, "Gold")
        entering = result["new_gold_plus_entering"]
        assert len(entering) == 1
        assert entering[0]["property_id"] == pid
    finally:
        _cleanup(pid)


def test_trigger_gold_to_platinum_suppressed():
    with get_db_context() as s:
        ids = _seed_property(s, seed_score=False)
        s.commit()
    pid = ids["property_id"]
    try:
        now = datetime.now(timezone.utc)
        tod = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        with get_db_context() as s:
            s.execute(text("""
                INSERT INTO distress_scores
                  (property_id, county_id, score_date, final_cds_score, lead_tier,
                   urgency_level, qualified, factor_scores, vertical_scores,
                   distress_types, scoring_run_id)
                VALUES (:p, :c, :now, 65.0, 'Gold', 'High', true,
                        '{}', '{}', '[]', :run)
            """), {"p": pid, "c": _COUNTY, "now": tod, "run": int(now.timestamp()) - 1})
            s.commit()

        result = _persist(pid, "Platinum")
        assert result["new_gold_plus_entering"] == [], \
            "Gold→Platinum must not emit (already Gold+)"
    finally:
        _cleanup(pid)


# ── Section 2b — real emit path through score_all_properties, flag-gated ──────

def _seed_scoreable() -> dict:
    """Seed a property with a fresh foreclosure so score_all_properties produces
    a with-signal batch (the precondition the emit block checks)."""
    with get_db_context() as s:
        ids = _seed_property(s, seed_score=False)
        s.add(Foreclosure(
            property_id=ids["property_id"],
            case_number=f"ZTEST-FC-{uuid.uuid4().hex[:10]}",
            filing_date=date.today(),
            county_id=_COUNTY,
        ))
        s.commit()
    return ids


def _run_score_all(pid: int):
    """Run real score_all_properties with persistence stubbed to inject a Gold+
    entry, GHL disabled. Exercises the actual emit+flag glue (ADR 0016)."""
    import src.services.cds_engine as cds_mod
    from src.services.cds_engine import MultiVerticalScorer

    fake_result = {
        "new": 1, "updated": 0, "unchanged": 0, "upgraded": 0, "qualified": 1,
        "ghl_queued": [], "new_gold_records": [],
        "new_gold_plus_entering": [{
            "property_id": pid, "county_id": _COUNTY, "lead_tier": "Gold",
            "zip": "33601", "scoring_run_id": 999,
        }],
    }
    with patch.object(cds_mod, "_GHL_PUSH_ENABLED", False), \
         patch.object(MultiVerticalScorer, "_persist_score_batch", return_value=fake_result):
        with get_db_context() as s:
            MultiVerticalScorer(s).score_all_properties(
                property_ids=[pid], save_to_db=True,
            )


def test_emit_fires_when_cascade_enabled():
    ids = _seed_scoreable()
    pid = ids["property_id"]
    prev = settings.enrichment_cascade_enabled
    settings.enrichment_cascade_enabled = True
    try:
        with patch(_PUBLISH_SRC) as mock_pub:
            _run_score_all(pid)

        assert mock_pub.call_count == 1
        event = mock_pub.call_args.args[0]
        assert event["event_type"] == "gold_lead_scored"
        assert event["payload"]["property_id"] == pid
        assert event["payload"]["lead_tier"] == "Gold"
        assert event["idempotency_key"] == f"gold_lead_scored:{pid}:999"
    finally:
        settings.enrichment_cascade_enabled = prev
        _cleanup(pid)


def test_emit_suppressed_when_cascade_disabled():
    ids = _seed_scoreable()
    pid = ids["property_id"]
    prev = settings.enrichment_cascade_enabled
    settings.enrichment_cascade_enabled = False
    try:
        with patch(_PUBLISH_SRC) as mock_pub:
            _run_score_all(pid)
        mock_pub.assert_not_called()
    finally:
        settings.enrichment_cascade_enabled = prev
        _cleanup(pid)


# ── Section 3 — EnrichmentBatcher ────────────────────────────────────────────

def test_batcher_dedup_keeps_latest_payload():
    from src.agents.enrichment_consumer import EnrichmentBatcher
    b = EnrichmentBatcher(flush_size=100, flush_seconds=9999)
    b.add({"property_id": "42", "lead_tier": "Gold"})
    b.add({"property_id": "42", "lead_tier": "Platinum"})
    with b._lock:
        assert len(b._buffer) == 1
        assert b._buffer["42"]["lead_tier"] == "Platinum"


def test_batcher_size_flush_invokes_cascade():
    from src.agents.enrichment_consumer import EnrichmentBatcher
    flushed: list = []
    with patch.object(EnrichmentBatcher, "_run_cascade_for_batch",
                      side_effect=lambda payloads: flushed.extend(payloads)):
        b = EnrichmentBatcher(flush_size=2, flush_seconds=9999)
        b.add({"property_id": "1", "county_id": _COUNTY})
        assert flushed == []
        b.add({"property_id": "2", "county_id": _COUNTY})
    assert {p["property_id"] for p in flushed} == {"1", "2"}


def test_batcher_timer_flush_invokes_cascade():
    from src.agents.enrichment_consumer import EnrichmentBatcher
    flushed: list = []
    stop = threading.Event()
    with patch.object(EnrichmentBatcher, "_run_cascade_for_batch",
                      side_effect=lambda payloads: flushed.extend(payloads)):
        b = EnrichmentBatcher(flush_size=999, flush_seconds=1)
        b.start(stop_event=stop)
        b.add({"property_id": "77", "county_id": _COUNTY})
        deadline = time.monotonic() + 4.0
        while not flushed and time.monotonic() < deadline:
            time.sleep(0.1)
        stop.set()
        b._timer_thread.join(timeout=3.0)
    assert any(p["property_id"] == "77" for p in flushed), "timer flush did not fire"


def test_batcher_missing_property_id_warns_and_drops():
    from src.agents.enrichment_consumer import EnrichmentBatcher
    b = EnrichmentBatcher(flush_size=100, flush_seconds=9999)
    with patch("src.agents.enrichment_consumer.logger") as mock_log:
        b.add({"lead_tier": "Gold"})
    with b._lock:
        assert len(b._buffer) == 0
    mock_log.warning.assert_called_once()
    assert "property_id" in mock_log.warning.call_args.args[0]


def test_batcher_flush_failure_is_isolated():
    """A cascade exception during flush is logged, not raised — listener survives."""
    from src.agents.enrichment_consumer import EnrichmentBatcher
    with patch.object(EnrichmentBatcher, "_run_cascade_for_batch",
                      side_effect=RuntimeError("db down")):
        b = EnrichmentBatcher(flush_size=1, flush_seconds=9999)
        b.add({"property_id": "99", "county_id": _COUNTY})   # size flush — must not raise
    with b._lock:
        assert len(b._buffer) == 0                            # buffer was drained


# ── Section 4 — supervisor routing ───────────────────────────────────────────

def test_supervisor_routes_gold_lead_scored_to_batcher():
    from src.agents.supervisor import dispatch_event
    mock_batcher = MagicMock()
    with patch("src.agents.enrichment_consumer.get_batcher", return_value=mock_batcher):
        result = dispatch_event({
            "event_type": "gold_lead_scored",
            "payload": {"property_id": "123", "county_id": _COUNTY, "lead_tier": "Gold"},
        })
    mock_batcher.add.assert_called_once()
    assert result["outcome"] == "routed"
    assert result["graph_name"] == "enrichment_cascade"


def test_supervisor_uninitialized_batcher_drops_cleanly():
    from src.agents.supervisor import dispatch_event
    with patch("src.agents.enrichment_consumer.get_batcher", return_value=None), \
         patch("src.agents.supervisor.logger") as mock_log:
        result = dispatch_event({
            "event_type": "gold_lead_scored",
            "payload": {"property_id": "404", "county_id": _COUNTY, "lead_tier": "Gold"},
        })
    assert result["outcome"] == "routed"
    mock_log.warning.assert_called_once()
    assert "not initialized" in mock_log.warning.call_args.args[0]


# ── Section 5 — full e2e: publish → supervisor → batcher → run_cascade ────────

def test_full_e2e_gold_event_drives_cascade(seeded):
    """A gold_lead_scored event routes through the supervisor and batcher and
    reaches run_cascade with the owner_ids resolved from the property_id.

    This test verifies the dispatch -> batcher -> cascade plumbing, not
    Task 6.2's budget gate (EnrichmentRouter now sits in front of run_cascade
    at this call site) — so the gate is forced open here. Without this, the
    test's outcome would depend on the real platform-wide spend ratio in
    whatever Postgres instance runs it, which is exactly what Task 6.2's own
    tests (test_algorithmic_variance_control.py) isolate and cover already.
    """
    from src.agents.enrichment_consumer import EnrichmentBatcher
    from src.agents.supervisor import dispatch_event
    from src.services.skip_trace_waterfall import WaterfallStats

    pid, oid = seeded["property_id"], seeded["owner_id"]
    cascade_calls: list[dict] = []

    def _mock_cascade(county_id=_COUNTY, owner_ids=None, **_kw):
        cascade_calls.append({"county_id": county_id, "owner_ids": list(owner_ids or [])})
        return WaterfallStats(total_leads=1, hits=0, misses=1)

    stop = threading.Event()
    with patch(_RUN_CASCADE_SRC, side_effect=_mock_cascade), \
         patch("src.services.enrichment_router.is_paid_enrichment_allowed",
               return_value=(True, {
                   "spend_cents": 0, "revenue_cents": 0, "ratio": 0.0, "threshold": 0.25,
                   "window_days": 30, "routing_reason": "spend_ratio_safe",
                   "selected_path": "paid_trace", "override_applied": False,
               })):
        batcher = EnrichmentBatcher(flush_size=1, flush_seconds=9999)
        batcher.start(stop_event=stop)
        try:
            with patch("src.agents.enrichment_consumer.get_batcher", return_value=batcher):
                dispatch_event({
                    "event_type": "gold_lead_scored",
                    "payload": {
                        "property_id": str(pid), "county_id": _COUNTY,
                        "lead_tier": "Gold", "scoring_run_id": 1234,
                    },
                })
        finally:
            stop.set()

    assert len(cascade_calls) == 1
    assert cascade_calls[0]["county_id"] == _COUNTY
    assert oid in cascade_calls[0]["owner_ids"]


# ── consume_prospect_created respects the budget gate (Task 6.2) ──────────
# consume_prospect_created() previously called run_cascade() directly,
# bypassing EnrichmentRouter entirely — the one cascade-triggering path
# Task 6.2 missed when it was first wired into the other 3 real call sites.
#
# consume_prospect_created()'s own event-polling query has no county_id or
# date scoping (a pre-existing, separate structural gap, not something this
# test set out to fix) — it processes ALL globally unprocessed
# prospect.created events, up to 500 at a time. Running it for real against
# the shared DB is only safe when the queue is actually empty of unrelated
# events, so this test asserts that precondition up front and fails loudly
# instead of silently touching real production owners if it's ever violated.

def test_consume_prospect_created_blocked_uses_free_fallback_not_cascade(seeded):
    from src.services.event_bus import emit_event
    from src.services.prospect_service import get_or_create_prospect
    from src.services.skip_trace_waterfall import consume_prospect_created

    pid, oid = seeded["property_id"], seeded["owner_id"]

    with get_db_context() as s:
        pending = s.execute(text("""
            SELECT COUNT(*) FROM events e
            LEFT JOIN processed_events pe ON pe.event_id = e.event_id AND pe.consumer = 'cascade'
            WHERE e.event_type = 'prospect.created' AND pe.event_id IS NULL
        """)).scalar()
        assert pending == 0, (
            "real unprocessed prospect.created events exist — refusing to run "
            "consume_prospect_created() for real, it has no county/date scoping "
            "and would process them alongside this test's seeded event"
        )

        prospect_id_str = get_or_create_prospect(s, pid)
        emit_event(
            s, event_type="prospect.created", actor="test", source_component="test",
            prospect_id=uuid.UUID(prospect_id_str), payload={"property_id": pid},
        )
        s.commit()

    with patch(_RUN_CASCADE_SRC) as mock_cascade, \
         patch(_GATE_SRC, return_value=(False, {
             "spend_cents": 0, "revenue_cents": 0, "ratio": 0.9, "threshold": 0.25,
             "window_days": 30, "routing_reason": "spend_ratio_exceeded",
             "selected_path": "blocked", "override_applied": False,
         })):
        stats = consume_prospect_created(county_id=_COUNTY)
        mock_cascade.assert_not_called()

    assert stats.total_leads == 1


# ── enrichment_background_loop Step 9 counts free-fallback hits (Task 6.2) ─
# Step 9's hit-detection query previously only recognized paid sources
# (tracerfy/batch_skip_tracing/idi/pdl), so a lead resolved via the free
# voter fallback (source='voters') was marked contact_refresh_status='failed'
# even though a real contact was found. Steps 1-8 are short-circuited via
# mocks (unrelated to this fix, and each already has its own coverage) so
# this test isolates Step 9's query behavior specifically.

def test_step9_marks_free_fallback_hit_as_fresh_not_failed(seeded):
    from types import SimpleNamespace
    from src.services.enrichment_background_loop import run_once

    pid, oid = seeded["property_id"], seeded["owner_id"]

    with get_db_context() as s:
        s.execute(text("""
            INSERT INTO enriched_contacts (property_id, county_id, source, mobile_phone, match_success, enriched_at)
            VALUES (:pid, :county_id, 'voters', :phone, TRUE, NOW())
        """), {"pid": pid, "county_id": _COUNTY, "phone": _PHONE})
        s.commit()

    claimed_row = SimpleNamespace(owner_id=oid, property_id=pid, county_id=_COUNTY)
    batch_result = {
        "selected_path": "blocked", "cascade_stats": None,
        "free_results": {pid: {"found": True, "source": "voters", "reason": None, "mobile_phone": _PHONE}},
    }

    with patch("src.services.enrichment_background_loop._reset_stuck_queued", return_value=0), \
         patch("src.services.enrichment_background_loop._fetch_and_claim_candidates", return_value=[claimed_row]), \
         patch("src.services.enrichment_background_loop._seed_voters", return_value=set()), \
         patch("src.services.enrichment_background_loop._filter_dnc", return_value=set()), \
         patch("src.services.enrichment_router.EnrichmentRouter.fetch_contact_profiles_batch",
               return_value=batch_result):
        stats = run_once(county_id=_COUNTY, limit=50, dry_run=False)

    assert stats["free_fallback_hits"] == 1

    with get_db_context() as s:
        row = s.execute(text(
            "SELECT contact_refresh_status FROM owners WHERE id = :oid"
        ), {"oid": oid}).fetchone()
    assert row.contact_refresh_status == "fresh"
