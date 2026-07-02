"""Task 6.2 — Algorithmic Variance Control Layer, DB-backed correctness tests
(real Postgres via fresh_db).

Two groups of tests, deliberately using different isolation strategies:

  * BudgetManager ratio tests — compute_platform_enrichment_spend_ratio() is
    a PLATFORM-WIDE aggregate with no grouping key, unlike Task 6.1's
    per-subscriber queries. There is no way to scope a test assertion away
    from real production enrichment_usage_logs rows other than moving the
    time window itself, so these tests pin everything to a fixed far-future
    `as_of` (matching the FRM/TO=2099 pattern used throughout Task 6.1's
    tests) and seed data at that same timestamp — real production data
    (dated ~2026) never falls inside a window anchored at 2099.
  * EnrichmentRouter tests — patch is_paid_enrichment_allowed() directly at
    its usage site (src.services.enrichment_router.is_paid_enrichment_allowed)
    to return a controlled decision, rather than seeding real ratio data.
    This isolates routing/logging/fallback behavior from ratio-computation
    correctness, which is already covered by the BudgetManager tests above.

No test here calls db.commit()/session.commit() anywhere in the code under
test — verified before writing this file, since an internal commit inside
a fresh_db-fixtured test previously broke rollback isolation and leaked
real data into production (see git history). fresh_db's rollback is the
only thing responsible for cleanup in this file.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import EnrichmentUsageLog, Owner, Property, Subscriber, Voter
from src.services.budget_manager import get_current_enrichment_spend_ratio, is_paid_enrichment_allowed
from src.services.enrichment_router import EnrichmentRouter, LeadRecord, execute_free_voter_registry_cross_match
from src.services.revenue_ledger import mark_ledger_refunded, record_revenue

pytestmark = pytest.mark.scenario_platform

_RUN_CASCADE_SRC = "src.services.skip_trace_waterfall.run_cascade"
_GATE_SRC = "src.services.enrichment_router.is_paid_enrichment_allowed"

# Fixed far-future reference point for BudgetManager tests — real production
# enrichment_usage_logs rows are all dated ~2026, so a window anchored here
# never sees them.
_AS_OF = datetime(2099, 6, 15, tzinfo=timezone.utc)
_NOW = datetime.now(timezone.utc)  # fine for router tests — they don't touch the ratio query


def _subscriber(db, cust):
    s = Subscriber(stripe_customer_id=cust, tier="pro", vertical="roofing", county_id="hillsborough")
    db.add(s); db.flush()
    return s


def _prop(db, parcel):
    p = Property(parcel_id=parcel, zip="33565", county_id="hillsborough")
    db.add(p); db.flush()
    return p


def _owner(db, prop, phone_1=None):
    o = Owner(property_id=prop.id, owner_name="JOHN DOE", county_id="hillsborough", phone_1=phone_1)
    db.add(o); db.flush()
    return o


def _voter(db, prop, phone_1):
    v = Voter(property_id=prop.id, county_id="hillsborough", source_voter_id=f"SV{prop.id}",
              voter_name="JOHN DOE", phone_1=phone_1)
    db.add(v); db.flush()
    return v


def _seed_ratio(db, *, spend_cents: int, revenue_cents: int, at: datetime = _AS_OF):
    """Seed enough enrichment_usage_logs + a subscription ledger row to
    produce a specific platform-wide spend/revenue ratio, isolated at `at`
    (defaults to the fixed far-future reference point)."""
    sub = _subscriber(db, f"cus_t62_{spend_cents}_{revenue_cents}_{at.timestamp()}")
    if revenue_cents > 0:
        record_revenue(db, subscriber_id=sub.id, product_type="subscription",
                       amount_cents=revenue_cents, source_table="subscription_invoices",
                       source_id=sub.id * 1000, occurred_at=at)
    if spend_cents > 0:
        prop = _prop(db, f"T62-ratio-{spend_cents}-{revenue_cents}-{at.timestamp()}")
        db.add(EnrichmentUsageLog(vendor="tracerfy", purpose="batch_skip_trace",
                                  property_id=prop.id, cost_cents=spend_cents,
                                  success=True, created_at=at))
    db.flush()
    return sub


def _mock_decision(allowed: bool, **overrides) -> tuple:
    detail = {
        "spend_cents": 0, "revenue_cents": 0, "ratio": None, "window_days": 30,
        "threshold": 0.25, "routing_reason": "spend_ratio_safe" if allowed else "spend_ratio_exceeded",
        "selected_path": "paid_trace" if allowed else "blocked", "override_applied": False,
    }
    detail.update(overrides)
    return allowed, detail


# ── BudgetManager: real ratio math against real Postgres, isolated by as_of ─

def test_ratio_10_percent_allows_paid(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=10, revenue_cents=100)
    allowed, detail = is_paid_enrichment_allowed(db, as_of=_AS_OF)
    assert allowed is True
    assert detail["routing_reason"] == "spend_ratio_safe"
    assert detail["selected_path"] == "paid_trace"


def test_ratio_24_99_percent_allows_paid(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=2499, revenue_cents=10000)
    allowed, detail = is_paid_enrichment_allowed(db, as_of=_AS_OF)
    assert allowed is True
    assert detail["routing_reason"] == "spend_ratio_safe"


def test_ratio_exactly_25_percent_blocks(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=2500, revenue_cents=10000)
    allowed, detail = is_paid_enrichment_allowed(db, as_of=_AS_OF)
    assert allowed is False
    assert detail["routing_reason"] == "spend_ratio_exceeded"
    assert detail["selected_path"] == "blocked"


def test_ratio_30_percent_blocks(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=30, revenue_cents=100)
    allowed, detail = is_paid_enrichment_allowed(db, as_of=_AS_OF)
    assert allowed is False
    assert detail["routing_reason"] == "spend_ratio_exceeded"


def test_revenue_zero_blocks_by_default(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=10, revenue_cents=0)
    allowed, detail = is_paid_enrichment_allowed(db, as_of=_AS_OF)
    assert allowed is False
    assert detail["routing_reason"] == "zero_revenue_guard"
    assert detail["ratio"] is None


def test_spend_zero_revenue_positive_is_safe(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=0, revenue_cents=100)
    allowed, detail = is_paid_enrichment_allowed(db, as_of=_AS_OF)
    assert allowed is True
    assert detail["ratio"] == 0.0


def test_override_true_bypasses_block(fresh_db):
    db = fresh_db
    _seed_ratio(db, spend_cents=90, revenue_cents=100)  # ratio 0.90, way over threshold
    allowed, detail = is_paid_enrichment_allowed(db, override=True, as_of=_AS_OF)
    assert allowed is True
    assert detail["routing_reason"] == "manual_override"
    assert detail["selected_path"] == "override_paid"


def test_window_boundary_excludes_older_rows(fresh_db):
    db = fresh_db
    outside_window = _AS_OF - timedelta(days=45)
    _seed_ratio(db, spend_cents=9999, revenue_cents=1, at=outside_window)
    metrics = get_current_enrichment_spend_ratio(db, window_days=30, as_of=_AS_OF)
    assert metrics["spend_cents"] == 0
    assert metrics["revenue_cents"] == 0
    assert metrics["ratio"] is None


def test_reversed_ledger_row_excluded_from_revenue(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t62_reversed")
    record_revenue(db, subscriber_id=sub.id, product_type="subscription",
                   amount_cents=10000, source_table="subscription_invoices",
                   source_id=sub.id * 2000, occurred_at=_AS_OF)
    mark_ledger_refunded(db, source_table="subscription_invoices", source_id=sub.id * 2000, refunded_at=_AS_OF)

    metrics = get_current_enrichment_spend_ratio(db, as_of=_AS_OF)
    assert metrics["revenue_cents"] == 0


def test_non_subscription_ledger_rows_excluded_from_denominator(fresh_db):
    """A lead_unlock/lead_pack/premium ledger row must not count as
    subscription revenue — the ratio's denominator is subscription
    revenue specifically, per the task's literal wording."""
    db = fresh_db
    sub = _subscriber(db, "cus_t62_onetime")
    record_revenue(db, subscriber_id=sub.id, product_type="lead_unlock",
                   amount_cents=100000, source_table="sent_leads",
                   source_id=sub.id * 3000, occurred_at=_AS_OF)
    metrics = get_current_enrichment_spend_ratio(db, as_of=_AS_OF)
    assert metrics["revenue_cents"] == 0


# ── execute_free_voter_registry_cross_match ────────────────────────────────

def test_free_match_success(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-freematch")
    owner = _owner(db, prop)
    _voter(db, prop, "8135551234")

    result = execute_free_voter_registry_cross_match(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["found"] is True
    assert result["source"] == "voters"
    assert result["mobile_phone"] == "+18135551234"

    db.refresh(owner)
    assert owner.phone_1 == "+18135551234"


def test_free_match_failure_no_voter_row(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-novoter")
    owner = _owner(db, prop)
    result = execute_free_voter_registry_cross_match(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["found"] is False
    assert result["reason"] == "no_voter_match"


def test_free_match_failure_dnc_blocked(fresh_db):
    db = fresh_db
    from sqlalchemy import text as sa_text
    prop = _prop(db, "T62-dnc")
    owner = _owner(db, prop)
    _voter(db, prop, "8135559999")
    db.execute(sa_text("INSERT INTO sms_opt_outs (phone, source, opted_out_at) VALUES ('+18135559999', 'test', now())"))
    db.flush()

    result = execute_free_voter_registry_cross_match(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["found"] is False
    assert result["reason"] == "dnc_blocked"
    db.refresh(owner)
    assert owner.phone_1 is None  # never written when DNC-blocked


# ── EnrichmentRouter.fetch_contact_profile (gate decision mocked) ──────────

def test_missing_telemetry_blocks(fresh_db):
    """A DB failure in the ratio query (not a clean zero) must be
    distinguished from zero_revenue_guard — the router, not BudgetManager
    itself, is responsible for catching the exception. Uses a real session
    (not a bare mock) so the free-match fallback the router takes after
    catching the exception runs for real — finding no voter row cleanly —
    instead of operating on garbage mock data."""
    db = fresh_db
    prop = _prop(db, "T62-missingtelemetry")
    owner = _owner(db, prop)
    with patch(_GATE_SRC, side_effect=RuntimeError("db exploded")):
        router = EnrichmentRouter()
        result = router.fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["routing_reason"] == "missing_telemetry_guard"
    assert result["contact_found"] is False


def test_free_match_failure_paid_not_called_without_override(fresh_db):
    """The exact bug fixed from the task's reference pseudocode: when the
    ratio is over threshold and the free match fails, the paid path must
    NOT be called unless override=True."""
    db = fresh_db
    prop = _prop(db, "T62-nofallthrough")
    owner = _owner(db, prop)

    with patch(_GATE_SRC, return_value=_mock_decision(False)), patch(_RUN_CASCADE_SRC) as mock_cascade:
        router = EnrichmentRouter()
        result = router.fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
        mock_cascade.assert_not_called()

    assert result["contact_found"] is False
    assert result["selected_path"] == "blocked"


def test_override_true_calls_paid_path(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-override")
    owner = _owner(db, prop)

    mock_stats = MagicMock(hits=1, misses=0, total_cost_cents=7)
    decision = _mock_decision(True, routing_reason="manual_override", selected_path="override_paid")
    with patch(_GATE_SRC, return_value=decision), patch(_RUN_CASCADE_SRC, return_value=mock_stats) as mock_cascade:
        router = EnrichmentRouter()
        result = router.fetch_contact_profile(
            LeadRecord(property_id=prop.id, owner_id=owner.id), db, override=True,
        )
        mock_cascade.assert_called_once()

    assert result["contact_found"] is True
    assert result["source"] == "paid"
    assert result["routing_reason"] == "manual_override"


def test_allowed_calls_paid_path_with_same_args_as_direct_call(fresh_db):
    """Pass-through equivalence: the router with allowed=True must call
    run_cascade with the exact arguments the real callers already pass."""
    db = fresh_db
    prop = _prop(db, "T62-passthrough")
    owner = _owner(db, prop)

    mock_stats = MagicMock(hits=1, misses=0, total_cost_cents=7)
    with patch(_GATE_SRC, return_value=_mock_decision(True)), \
         patch(_RUN_CASCADE_SRC, return_value=mock_stats) as mock_cascade:
        router = EnrichmentRouter()
        router.fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id, county_id="hillsborough"), db)
        mock_cascade.assert_called_once_with(county_id="hillsborough", owner_ids=[owner.id])


def test_log_written_on_allow(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-logallow")
    owner = _owner(db, prop)

    mock_stats = MagicMock(hits=1, misses=0, total_cost_cents=7)
    with patch(_GATE_SRC, return_value=_mock_decision(True)), patch(_RUN_CASCADE_SRC, return_value=mock_stats):
        EnrichmentRouter().fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)

    from sqlalchemy import text as sa_text
    row = db.execute(sa_text(
        "SELECT selected_path, paid_lookup_allowed, routing_reason FROM algorithmic_variance_log "
        "WHERE property_id = :pid"
    ), {"pid": prop.id}).fetchone()
    assert row is not None
    assert row.selected_path == "paid_trace"
    assert row.paid_lookup_allowed is True
    assert row.routing_reason == "spend_ratio_safe"


def test_log_written_on_block(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-logblock")
    owner = _owner(db, prop)

    with patch(_GATE_SRC, return_value=_mock_decision(False)):
        EnrichmentRouter().fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)

    from sqlalchemy import text as sa_text
    row = db.execute(sa_text(
        "SELECT selected_path, paid_lookup_allowed, routing_reason, lookup_success FROM algorithmic_variance_log "
        "WHERE property_id = :pid"
    ), {"pid": prop.id}).fetchone()
    assert row is not None
    assert row.selected_path == "blocked"
    assert row.paid_lookup_allowed is False
    assert row.routing_reason == "spend_ratio_exceeded"
    assert row.lookup_success is False


def test_free_match_success_logs_zero_cost(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-freelog")
    owner = _owner(db, prop)
    _voter(db, prop, "8135550001")

    with patch(_GATE_SRC, return_value=_mock_decision(False)):
        result = EnrichmentRouter().fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["contact_found"] is True
    assert result["source"] == "free"

    from sqlalchemy import text as sa_text
    row = db.execute(sa_text(
        "SELECT provider, lookup_success, cost_cents FROM algorithmic_variance_log WHERE property_id = :pid"
    ), {"pid": prop.id}).fetchone()
    assert row.provider == "voters"
    assert row.lookup_success is True
    assert row.cost_cents == 0


# ── Free fallback must be a full replacement path, not just a data write ──
# (bug: previously only wrote owners.phone_1 + enriched_contacts, and never
# stamped prospects.contactability_state, emitted enrichment.completed, or
# ran dedupe_after_cascade — all of which run_cascade()'s own M2 block does
# for a paid hit.)

def test_free_match_hit_stamps_prospect_and_emits_completed(fresh_db):
    db = fresh_db
    from sqlalchemy import text as sa_text
    prop = _prop(db, "T62-freehit-m2")
    owner = _owner(db, prop)
    _voter(db, prop, "8135550010")

    with patch(_GATE_SRC, return_value=_mock_decision(False)):
        result = EnrichmentRouter().fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["contact_found"] is True

    prospect = db.execute(sa_text(
        "SELECT prospect_id, contactability_state FROM prospects WHERE property_id = :pid"
    ), {"pid": prop.id}).fetchone()
    assert prospect is not None
    assert prospect.contactability_state == "contactable"

    event = db.execute(sa_text(
        "SELECT event_type, payload FROM events WHERE prospect_id = :pid AND event_type = 'enrichment.completed'"
    ), {"pid": prospect.prospect_id}).fetchone()
    assert event is not None
    assert event.payload["source"] == "voters"
    assert event.payload["total_cost_cents"] == 0


def test_free_match_miss_does_not_mark_exhausted_or_emit_failed(fresh_db):
    """A free-fallback miss (budget-blocked, voter registry has nothing) is
    not the same as a paid cascade exhausting every provider — the lead
    still has real options once the spend ratio recovers. Marking it
    exhausted / emitting enrichment.failed would trigger permanent
    recycle-suppression in truth_engine_batch.py for a lead that hasn't
    actually run out of options."""
    db = fresh_db
    from sqlalchemy import text as sa_text
    prop = _prop(db, "T62-freemiss-m2")
    owner = _owner(db, prop)

    with patch(_GATE_SRC, return_value=_mock_decision(False)):
        result = EnrichmentRouter().fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
    assert result["contact_found"] is False

    prospect = db.execute(sa_text(
        "SELECT prospect_id FROM prospects WHERE property_id = :pid"
    ), {"pid": prop.id}).fetchone()
    assert prospect is None  # no prospect row created for an unresolved lead

    event = db.execute(sa_text(
        "SELECT 1 FROM events WHERE event_type = 'enrichment.failed' AND payload->>'property_id' = :pid"
    ), {"pid": str(prop.id)}).fetchone()
    assert event is None


def test_free_match_hit_runs_dedupe(fresh_db):
    db = fresh_db
    prop = _prop(db, "T62-freehit-dedupe")
    owner = _owner(db, prop)
    _voter(db, prop, "8135550011")

    with patch(_GATE_SRC, return_value=_mock_decision(False)), \
         patch("src.services.prospect_service.dedupe_after_cascade") as mock_dedupe:
        EnrichmentRouter().fetch_contact_profile(LeadRecord(property_id=prop.id, owner_id=owner.id), db)
        mock_dedupe.assert_called_once_with(db, [prop.id])


def test_batch_free_match_hits_stamped_dedupe_called_once_for_batch(fresh_db):
    db = fresh_db
    from sqlalchemy import text as sa_text
    prop_hit = _prop(db, "T62-batchhit-m2")
    owner_hit = _owner(db, prop_hit)
    _voter(db, prop_hit, "8135550012")
    prop_miss = _prop(db, "T62-batchmiss-m2")
    owner_miss = _owner(db, prop_miss)

    lead_records = [
        LeadRecord(property_id=prop_hit.id, owner_id=owner_hit.id),
        LeadRecord(property_id=prop_miss.id, owner_id=owner_miss.id),
    ]
    with patch(_GATE_SRC, return_value=_mock_decision(False)), \
         patch("src.services.prospect_service.dedupe_after_cascade") as mock_dedupe:
        EnrichmentRouter().fetch_contact_profiles_batch(lead_records, db)
        mock_dedupe.assert_called_once_with(db, [prop_hit.id])

    hit_prospect = db.execute(sa_text(
        "SELECT contactability_state FROM prospects WHERE property_id = :pid"
    ), {"pid": prop_hit.id}).fetchone()
    assert hit_prospect.contactability_state == "contactable"

    miss_prospect = db.execute(sa_text(
        "SELECT 1 FROM prospects WHERE property_id = :pid"
    ), {"pid": prop_miss.id}).fetchone()
    assert miss_prospect is None


# ── EnrichmentRouter.fetch_contact_profiles_batch ──────────────────────────

def test_batch_computes_ratio_once_calls_cascade_once(fresh_db):
    db = fresh_db
    props = [_prop(db, f"T62-batch{i}") for i in range(3)]
    owners = [_owner(db, p) for p in props]
    lead_records = [LeadRecord(property_id=p.id, owner_id=o.id) for p, o in zip(props, owners)]

    mock_stats = MagicMock(hits=3, misses=0, total_cost_cents=21)
    with patch(_GATE_SRC, return_value=_mock_decision(True)) as mock_gate, \
         patch(_RUN_CASCADE_SRC, return_value=mock_stats) as mock_cascade:
        result = EnrichmentRouter().fetch_contact_profiles_batch(lead_records, db)
        assert mock_gate.call_count == 1     # ratio computed once for the whole batch
        assert mock_cascade.call_count == 1  # one cascade call for the whole batch, not one per lead

    assert result["selected_path"] == "paid_trace"

    from sqlalchemy import text as sa_text
    n_logs = db.execute(sa_text(
        "SELECT count(*) FROM algorithmic_variance_log WHERE property_id = ANY(:pids)"
    ), {"pids": [p.id for p in props]}).scalar()
    assert n_logs == 3  # one log row per lead, even though the ratio/cascade ran once


def test_batch_blocked_runs_free_match_per_lead(fresh_db):
    db = fresh_db
    prop_hit = _prop(db, "T62-batchfree-hit")
    owner_hit = _owner(db, prop_hit)
    _voter(db, prop_hit, "8135550002")
    prop_miss = _prop(db, "T62-batchfree-miss")
    owner_miss = _owner(db, prop_miss)

    lead_records = [
        LeadRecord(property_id=prop_hit.id, owner_id=owner_hit.id),
        LeadRecord(property_id=prop_miss.id, owner_id=owner_miss.id),
    ]
    with patch(_GATE_SRC, return_value=_mock_decision(False)), patch(_RUN_CASCADE_SRC) as mock_cascade:
        result = EnrichmentRouter().fetch_contact_profiles_batch(lead_records, db)
        mock_cascade.assert_not_called()

    assert result["free_results"][prop_hit.id]["found"] is True
    assert result["free_results"][prop_miss.id]["found"] is False
