"""
Contact triangulation staging E2E (ADR 0015).

Runs against the real Postgres DB (get_db_context / DATABASE_URL) but every
test seeds its own rows under a sentinel county_id so the sweep is naturally
scoped — nothing outside the seeded fixtures is ever read into the sweep or
written.  All seeded rows are deleted in teardown.

Coverage:
  - sweep writes label + contactability_detail (strong corroboration path)
  - aggregate Lifecycle event published once per sweep, house payload shape
  - dry-run writes nothing, publishes nothing
  - contradiction → downgrade → contact_refresh_status='due' (plan step 8)
  - waterfall inline hook: writes label, never publishes; no-op when disabled
  - check_pack_contactability gate (pass / fail / empty)
  - CDS tiered phone bonus: flag off vs on with 'low' label → -10 per vertical
  - delta-rescore dispatched with changed property_ids when flag 2 on

Write-path tests require owners.contactability_detail (fa078) — they skip
with a clear message until scripts/apply_contactability_detail_migration.py
has been run.

Run:
    pytest tests/scenarios/test_contact_triangulation_e2e.py -v -m scenario
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest
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
from src.services.contact_triangulation import TriangulationService
from src.services.lead_pool_service import check_pack_contactability
from src.services.skip_trace_waterfall import _triangulate_owner

pytestmark = pytest.mark.scenario

_PHONE = "+18135551234"
_OTHER_PHONE = "+18139998888"
_COUNTY = "ztest-triang"          # sentinel — scopes the sweep to seeded rows only
_PUBLISH_TARGET = "src.agents.events.ingestion.publish_lifecycle_event"


def _utcnow_naive() -> datetime:
    # enriched_at / score_date are TIMESTAMP WITHOUT TIME ZONE — store naive UTC
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _fa078_applied() -> bool:
    with get_db_context() as session:
        return bool(session.execute(text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'owners' AND column_name = 'contactability_detail'"
        )).scalar())


_HAS_FA078 = _fa078_applied()
require_fa078 = pytest.mark.skipif(
    not _HAS_FA078,
    reason="owners.contactability_detail missing — run "
           "scripts/apply_contactability_detail_migration.py first",
)


# ── seed / teardown ───────────────────────────────────────────────────────────

def _seed(session, *, voter_phone: str = _PHONE, voter_name: str = "Gonzalez, Ana R",
          ec_age_sql: str | None = None, with_voter: bool = True) -> dict:
    """Seed one Gold+ property + owner + tracerfy EC (+ optional voter)."""
    uid = uuid.uuid4().hex[:10]
    prop = Property(parcel_id=f"ZTEST-TRIANG-{uid}", address="100 Test St",
                    city="Tampa", state="FL", zip="33601", county_id=_COUNTY)
    session.add(prop)
    session.flush()

    owner = Owner(
        property_id=prop.id,
        owner_name="ANA R GONZALEZ",
        county_id=_COUNTY,
        phone_1=_PHONE,
        phone_metadata={"phone_1": {"score": 85, "reachable": True, "type": "mobile"}},
    )
    session.add(owner)
    session.add(DistressScore(
        property_id=prop.id, lead_tier="Gold", final_cds_score=60.0,
        score_date=_utcnow_naive(), county_id=_COUNTY,
    ))
    session.add(EnrichedContact(
        property_id=prop.id, county_id=_COUNTY, source="tracerfy",
        match_success=True, mobile_phone=_PHONE, confidence=0.80,
        enriched_at=_utcnow_naive(),
    ))
    if with_voter:
        session.add(Voter(
            property_id=prop.id, county_id=_COUNTY,
            source_voter_id=f"ZT{uid[:8]}", voter_name=voter_name,
            registration_status="ACT", phone_1=voter_phone,
        ))
    session.flush()

    if ec_age_sql:
        session.execute(
            text(f"UPDATE enriched_contacts SET enriched_at = NOW() - INTERVAL '{ec_age_sql}'"
                 " WHERE property_id = :pid"),
            {"pid": prop.id},
        )

    return {"property_id": prop.id, "owner_id": owner.id}


def _cleanup(property_id: int) -> None:
    with get_db_context() as session:
        for table in ("voters", "enriched_contacts", "distress_scores",
                      "foreclosures", "owners"):
            session.execute(
                text(f"DELETE FROM {table} WHERE property_id = :pid"),  # noqa: S608 — fixed table names
                {"pid": property_id},
            )
        session.execute(text("DELETE FROM properties WHERE id = :pid"), {"pid": property_id})
        session.commit()


@pytest.fixture
def seeded():
    """Default seed: fresh EC + matching active voter → strong corroboration."""
    with get_db_context() as session:
        ids = _seed(session)
        session.commit()
    yield ids
    _cleanup(ids["property_id"])


def _sweep(**kwargs):
    with get_db_context() as session:
        return TriangulationService(session).run_sweep(county_id=_COUNTY, **kwargs)


def _read_owner(owner_id: int) -> dict:
    with get_db_context() as session:
        row = session.execute(text("""
            SELECT contact_info_confidence, contact_info_confidence_score,
                   contact_refresh_status, contact_refresh_reason,
                   contactability_detail
            FROM owners WHERE id = :oid
        """), {"oid": owner_id}).fetchone()
    return dict(row._mapping)


# ── sweep write path ──────────────────────────────────────────────────────────

@require_fa078
def test_sweep_writes_label_and_detail(seeded):
    """Fresh EC + voter phone + name agreement → strong → high, detail persisted."""
    with patch(_PUBLISH_TARGET):
        stats = _sweep()

    assert stats["evaluated"] == 1
    assert stats["corroboration"]["strong"] == 1

    owner = _read_owner(seeded["owner_id"])
    assert owner["contact_info_confidence"] == "high"
    detail = owner["contactability_detail"]
    assert detail["rule_fired"] == "cross_source_name_match"
    assert detail["corroboration"] == "strong"
    assert detail["matched_phone"] == _PHONE
    assert "voter_current" in detail["sources"]
    assert detail["prev_label"] is None


@require_fa078
def test_sweep_publishes_aggregate_lifecycle_event(seeded):
    with patch(_PUBLISH_TARGET) as mock_publish:
        _sweep()

    mock_publish.assert_called_once()
    event = mock_publish.call_args.args[0]
    assert event["event_type"] == "contactability_sweep_completed"
    payload = event["payload"]          # house shape: data under 'payload'
    assert payload["county_id"] == _COUNTY
    assert payload["distribution"] == {"high": 1}
    assert payload["changed"] == 1
    assert payload["scoring_delta_dispatched"] is False   # flag 2 off


def test_sweep_dry_run_writes_nothing(seeded):
    with patch(_PUBLISH_TARGET) as mock_publish:
        stats = _sweep(dry_run=True)

    mock_publish.assert_not_called()
    assert stats["evaluated"] == 1
    with get_db_context() as session:
        label = session.execute(
            text("SELECT contact_info_confidence FROM owners WHERE id = :oid"),
            {"oid": seeded["owner_id"]},
        ).scalar()
    assert label is None


@require_fa078
def test_contradiction_downgrades_and_marks_due():
    """
    Aged EC (200d): strong corroboration holds it at medium (eff_age 100d).
    Voter phone then changes → corroboration gone → label drops to low AND
    the downgrade forces contact_refresh_status='due' (plan step 8).
    """
    with get_db_context() as session:
        ids = _seed(session, ec_age_sql="200 days")
        session.commit()
    try:
        with patch(_PUBLISH_TARGET):
            _sweep()
        first = _read_owner(ids["owner_id"])
        assert first["contact_info_confidence"] == "medium"

        with get_db_context() as session:
            session.execute(
                text("UPDATE voters SET phone_1 = :p WHERE property_id = :pid"),
                {"p": _OTHER_PHONE, "pid": ids["property_id"]},
            )
            session.commit()

        with patch(_PUBLISH_TARGET):
            stats = _sweep()

        assert stats["downgrades"] == 1
        second = _read_owner(ids["owner_id"])
        assert second["contact_info_confidence"] == "low"
        assert second["contact_refresh_status"] == "due"
        assert second["contact_refresh_reason"] == "label_downgrade"
        assert second["contactability_detail"]["prev_label"] == "medium"
    finally:
        _cleanup(ids["property_id"])


# ── waterfall inline hook ─────────────────────────────────────────────────────

@require_fa078
def test_inline_hook_writes_label_without_event(seeded):
    prev = settings.triangulation_enabled
    settings.triangulation_enabled = True
    try:
        with patch(_PUBLISH_TARGET) as mock_publish:
            with get_db_context() as session:
                _triangulate_owner(session, seeded["owner_id"])
        mock_publish.assert_not_called()    # single-owner path never publishes
        owner = _read_owner(seeded["owner_id"])
        assert owner["contact_info_confidence"] == "high"
    finally:
        settings.triangulation_enabled = prev


def test_inline_hook_noop_when_disabled(seeded):
    prev = settings.triangulation_enabled
    settings.triangulation_enabled = False
    try:
        with get_db_context() as session:
            _triangulate_owner(session, seeded["owner_id"])
        with get_db_context() as session:
            label = session.execute(
                text("SELECT contact_info_confidence FROM owners WHERE id = :oid"),
                {"oid": seeded["owner_id"]},
            ).scalar()
        assert label is None
    finally:
        settings.triangulation_enabled = prev


# ── pack contactability gate ──────────────────────────────────────────────────

def test_pack_contactability_gate(seeded):
    with get_db_context() as session:
        session.execute(
            text("UPDATE owners SET contact_info_confidence = 'high' WHERE id = :oid"),
            {"oid": seeded["owner_id"]},
        )
        session.commit()

    with get_db_context() as session:
        result = check_pack_contactability(session, [seeded["property_id"]])
    assert result["passes"] is True
    assert result["pct_contactable"] == 1.0
    assert result["counts"] == {"high": 1}

    with get_db_context() as session:
        session.execute(
            text("UPDATE owners SET contact_info_confidence = 'stale' WHERE id = :oid"),
            {"oid": seeded["owner_id"]},
        )
        session.commit()
        result = check_pack_contactability(session, [seeded["property_id"]])
    assert result["passes"] is False
    assert result["counts"] == {"stale": 1}

    with get_db_context() as session:
        empty = check_pack_contactability(session, [])
    assert empty == {"pct_contactable": 0.0, "passes": False, "counts": {}, "total": 0}


# ── CDS tiered bonus ──────────────────────────────────────────────────────────

def _score_property(property_id: int) -> dict:
    import src.services.cds_engine as cds_mod
    from src.services.cds_engine import MultiVerticalScorer

    with patch.object(cds_mod, "_GHL_PUSH_ENABLED", False):
        with get_db_context() as session:
            results = MultiVerticalScorer(session).score_properties_by_ids(
                [property_id], save_to_db=False,
            )
    assert results, "CDS returned no result for seeded property"
    return results[0]


def test_cds_tiered_bonus_flag_on_vs_off(seeded):
    """'low' label: flag off → flat +15; flag on → +5. Vertical delta == 10."""
    with get_db_context() as session:
        session.add(Foreclosure(
            property_id=seeded["property_id"],
            case_number=f"ZTEST-FC-{uuid.uuid4().hex[:10]}",
            filing_date=date.today(),
            county_id=_COUNTY,
        ))
        session.execute(
            text("UPDATE owners SET contact_info_confidence = 'low' WHERE id = :oid"),
            {"oid": seeded["owner_id"]},
        )
        session.commit()

    prev = settings.cds_use_contactability
    try:
        settings.cds_use_contactability = False
        flat = _score_property(seeded["property_id"])
        settings.cds_use_contactability = True
        tiered = _score_property(seeded["property_id"])
    finally:
        settings.cds_use_contactability = prev

    flat_w = flat["vertical_scores"]["wholesalers"]
    tiered_w = tiered["vertical_scores"]["wholesalers"]
    assert flat_w - tiered_w == 10, f"expected -10 for 'low', got {flat_w} -> {tiered_w}"
    # label is snapshotted into factor_scores for audit
    assert tiered["factor_scores"]["contact_info_confidence"] == "low"


# ── delta-rescore dispatch ────────────────────────────────────────────────────

@require_fa078
def test_delta_rescore_dispatched_when_flag_on(seeded):
    prev = settings.cds_use_contactability
    settings.cds_use_contactability = True
    try:
        with patch(_PUBLISH_TARGET) as mock_publish, \
             patch("src.services.cds_engine.MultiVerticalScorer") as mock_engine:
            _sweep()

        mock_engine.return_value.score_properties_by_ids.assert_called_once_with(
            [seeded["property_id"]]
        )
        payload = mock_publish.call_args.args[0]["payload"]
        assert payload["scoring_delta_dispatched"] is True
    finally:
        settings.cds_use_contactability = prev
