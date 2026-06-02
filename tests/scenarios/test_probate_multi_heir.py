"""
Scenario — Probate multi-heir enrichment fan-out.

Validates the path that fires when the multi-heir prevalence gate is breached
(>20% of probate properties name 2+ traceable heirs), which real Hillsborough
data does NOT currently hit (~3%). We seed a synthetic county that breaches the
gate so the fan-out, the audit, and the guardrails can be exercised
deterministically with BatchData mocked (no credits, no network).

Isolation: every row is seeded under a unique throwaway county_id
(`test_mh_<uuid>`) so teardown is exact and cannot touch real county data,
even though scenario tests run against the configured DATABASE_URL.

Coverage:
  - audit prevalence > 20% gate (breached) and < 20% (negative)
  - fan-out creates one EnrichedContact per valid, non-entity, deduped heir
  - entity-only heirs → no fan-out (falls back to single address-only trace)
  - duplicate heirs deduped case-insensitively
  - MAX_HEIRS_PER_PROPERTY clamp ([1,10] at construction) + functional cap
  - regression: 400-retry path unpacks index_map 3-tuples (was a 2-tuple bug)
"""

from __future__ import annotations

import uuid
from datetime import date

import pytest

from config.settings import settings, AppSettings
from src.core.database import db, get_db_context
from src.core.models import Property, Owner, LegalProceeding, EnrichedContact
from src.services import skip_trace
from scripts._probate_multi_heir_audit import run_audit


pytestmark = pytest.mark.scenario_platform


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _match_person(seq: int) -> dict:
    """A BatchData 'person' entry that _parse_result reads as a successful match."""
    return {
        "phoneNumbers": [
            {"number": f"+1813555{seq:04d}", "type": "Mobile",
             "score": 90, "reachable": True, "tested": True},
        ],
        "emails": [{"email": f"heir{seq}@example.test"}],
        "mailingAddress": {"street": "1 Test St", "city": "Tampa", "state": "FL", "zip": "33601"},
        "name": {"first": "Traced", "last": f"Person{seq}"},
    }


def _mock_batch_data(payloads, api_key):
    """Index-aligned successful match for every payload."""
    return [_match_person(i) for i in range(len(payloads))]


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mh_env(monkeypatch):
    """Throwaway county + seeding factory + bulletproof teardown + mocks."""
    county = f"test_mh_{uuid.uuid4().hex[:8]}"

    # No network / no credits / no cost-log writes.
    monkeypatch.setattr(skip_trace, "_call_batch_data", _mock_batch_data)
    monkeypatch.setattr(skip_trace, "send_alert", lambda *a, **k: None)
    import src.services.enrichment_log as _el
    monkeypatch.setattr(_el, "log_usage", lambda *a, **k: None)

    # Save/restore the flag + cap (assignment bypasses the validator, which is
    # fine here — the clamp itself is tested separately at construction time).
    prev_flag = settings.multi_heir_enrichment_enabled
    prev_cap = settings.max_heirs_per_property

    _seq = {"n": 0}

    def seed(heirs: list[str], *, owner_type: str = "Individual") -> tuple[int, int]:
        """Seed one probate property+owner with the given heir list. Returns (property_id, owner_id)."""
        _seq["n"] += 1
        n = _seq["n"]
        with db.session_scope() as s:
            prop = Property(
                parcel_id=f"{county}-P{n:04d}",
                address=f"{100 + n} Heir Ave",
                city="Tampa", state="FL", zip="33602",
                county_id=county, sync_status="pending_sync",
            )
            s.add(prop)
            s.flush()
            pid = prop.id

            owner = Owner(
                property_id=pid,
                owner_name=heirs[0] if heirs else "UNKNOWN OWNER",
                owner_type=owner_type,
                county_id=county,
                sunbiz_status="pending",
            )
            s.add(owner)
            s.flush()
            oid = owner.id

            s.add(LegalProceeding(
                property_id=pid,
                record_type="Probate",
                case_number=f"{county}-CP-{n:05d}",
                filing_date=date(2026, 5, 1),
                date_added=date(2026, 5, 2),
                associated_party="DECEDENT NAME",
                secondary_party=heirs[0] if heirs else None,
                county_id=county,
                meta_data={"heirs": heirs},
            ))
        return pid, oid

    yield {"county": county, "seed": seed}

    # ── Teardown — delete every seeded row by throwaway county_id ──────────────
    settings.multi_heir_enrichment_enabled = prev_flag
    settings.max_heirs_per_property = prev_cap
    with db.session_scope() as s:
        s.query(EnrichedContact).filter(EnrichedContact.county_id == county).delete(synchronize_session=False)
        s.query(LegalProceeding).filter(LegalProceeding.county_id == county).delete(synchronize_session=False)
        s.query(Owner).filter(Owner.county_id == county).delete(synchronize_session=False)
        s.query(Property).filter(Property.county_id == county).delete(synchronize_session=False)


def _contacts_for(property_id: int) -> list[EnrichedContact]:
    with get_db_context() as s:
        rows = (
            s.query(EnrichedContact)
            .filter(EnrichedContact.property_id == property_id,
                    EnrichedContact.source == "batch_skip_tracing")
            .all()
        )
        s.expunge_all()
        return rows


# ──────────────────────────────────────────────────────────────────────────────
# Audit prevalence gate
# ──────────────────────────────────────────────────────────────────────────────

def test_audit_prevalence_above_threshold(mh_env):
    """100 deduped probate properties, 25 with 2+ traceable heirs → 25% > 20% gate."""
    seed, county = mh_env["seed"], mh_env["county"]
    for i in range(25):
        seed([f"First{i} MultiA{i}", f"First{i} MultiB{i}"])
    for i in range(75):
        seed([f"Solo{i} Heir{i}"])

    result = run_audit(county_id=county)
    assert result["total"] == 100
    assert result["multi_heir"] == 25
    assert result["prevalence_pct"] == 25.0
    assert result["above_threshold"] is True


def test_audit_prevalence_below_threshold(mh_env):
    """100 properties, only 10 multi-heir → 10% < 20% gate."""
    seed, county = mh_env["seed"], mh_env["county"]
    for i in range(10):
        seed([f"First{i} MultiA{i}", f"First{i} MultiB{i}"])
    for i in range(90):
        seed([f"Solo{i} Heir{i}"])

    result = run_audit(county_id=county)
    assert result["total"] == 100
    assert result["multi_heir"] == 10
    assert result["prevalence_pct"] == 10.0
    assert result["above_threshold"] is False


# ──────────────────────────────────────────────────────────────────────────────
# Fan-out behaviour
# ──────────────────────────────────────────────────────────────────────────────

def test_fanout_creates_one_contact_per_heir(mh_env):
    """Flag ON + 3 distinct non-entity heirs → 3 EnrichedContact rows, distinct traced_name."""
    seed, county = mh_env["seed"], mh_env["county"]
    settings.multi_heir_enrichment_enabled = True
    settings.max_heirs_per_property = 5

    heirs = ["Kevin Flynn", "Yuling Yu", "Amanda Kling"]
    pid, oid = seed(heirs)

    skip_trace.run_skip_trace(owner_ids=[oid], county_id=county)

    rows = _contacts_for(pid)
    assert len(rows) == 3
    traced = sorted(r.traced_name for r in rows)
    assert traced == sorted(heirs)
    assert all(r.match_success for r in rows)


def test_entity_only_heirs_no_fanout(mh_env):
    """All heirs are entities → valid_heirs<2 → single address-only trace (1 row, traced_name None)."""
    seed, county = mh_env["seed"], mh_env["county"]
    settings.multi_heir_enrichment_enabled = True
    settings.max_heirs_per_property = 5

    pid, oid = seed(["ACME PROPERTIES LLC", "SMITH FAMILY TRUST", "JONES HOLDINGS"])

    skip_trace.run_skip_trace(owner_ids=[oid], county_id=county)

    rows = _contacts_for(pid)
    assert len(rows) == 1
    assert rows[0].traced_name is None


def test_duplicate_heirs_deduped(mh_env):
    """Case-insensitive duplicates collapse: 3 listed, 2 unique → 2 contacts."""
    seed, county = mh_env["seed"], mh_env["county"]
    settings.multi_heir_enrichment_enabled = True
    settings.max_heirs_per_property = 5

    pid, oid = seed(["JOHN DOE", "john doe", "JANE ROE"])

    skip_trace.run_skip_trace(owner_ids=[oid], county_id=county)

    rows = _contacts_for(pid)
    assert len(rows) == 2
    assert sorted(r.traced_name.upper() for r in rows) == ["JANE ROE", "JOHN DOE"]


# ──────────────────────────────────────────────────────────────────────────────
# MAX_HEIRS_PER_PROPERTY clamp
# ──────────────────────────────────────────────────────────────────────────────

def test_max_heirs_clamp_at_construction():
    """The [1,10] clamp runs in the field validator at settings construction."""
    base = dict(anthropic_api_key="x", firecrawl_api_key="x", court_listener_api_key="x")
    assert AppSettings(max_heirs_per_property=50, **base).max_heirs_per_property == 10
    assert AppSettings(max_heirs_per_property=0, **base).max_heirs_per_property == 1
    assert AppSettings(max_heirs_per_property=5, **base).max_heirs_per_property == 5


def test_max_heirs_cap_applied_in_fanout(mh_env):
    """6 valid heirs but cap=3 → only 3 contacts created."""
    seed, county = mh_env["seed"], mh_env["county"]
    settings.multi_heir_enrichment_enabled = True
    settings.max_heirs_per_property = 3

    heirs = [f"Heir{i} Surname{i}" for i in range(6)]
    pid, oid = seed(heirs)

    skip_trace.run_skip_trace(owner_ids=[oid], county_id=county)

    rows = _contacts_for(pid)
    assert len(rows) == 3


# ──────────────────────────────────────────────────────────────────────────────
# Regression — 400 retry path must unpack index_map 3-tuples
# ──────────────────────────────────────────────────────────────────────────────

def test_400_retry_unpacks_3tuple(mh_env, monkeypatch):
    """
    Multi-heir fan-out emits 2 payloads for one property; the batched call 400s,
    forcing the per-record retry loop at skip_trace.py:891. That line previously
    unpacked index_map[i] as a 2-tuple while it holds 3-tuples → ValueError.
    Assert the run completes and still persists a contact per heir.
    """
    seed, county = mh_env["seed"], mh_env["county"]
    settings.multi_heir_enrichment_enabled = True
    settings.max_heirs_per_property = 5

    def batch_400_then_individual(payloads, api_key):
        if len(payloads) > 1:
            raise Exception("400 Bad Request from BatchData")
        return [_match_person(0)]

    monkeypatch.setattr(skip_trace, "_call_batch_data", batch_400_then_individual)

    pid, oid = seed(["Pamela Bartlett", "Larry Bartlett"])

    # Must not raise ValueError (the old 2-tuple unpack bug).
    skip_trace.run_skip_trace(owner_ids=[oid], county_id=county)

    rows = _contacts_for(pid)
    assert len(rows) == 2
    assert sorted(r.traced_name for r in rows) == ["Larry Bartlett", "Pamela Bartlett"]
