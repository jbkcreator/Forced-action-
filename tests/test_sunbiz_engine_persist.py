"""
Integration tests for `sunbiz_engine` DB writers — `_persist_snapshot_and_owner`
and `_mark_status`. Exercises the parser → ORM write path against a real Postgres
session (rolled back per test). Does NOT touch Playwright, the network, or the
AI fallback — those are deferred to manual smoke.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from src.core.models import Owner, Property, SunbizSnapshot as SunbizSnapshotRow
from src.scrappers.sunbiz.parser import parse_sunbiz_detail
from src.scrappers.sunbiz.sunbiz_engine import (
    _mark_status,
    _persist_snapshot_and_owner,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sunbiz"


def _load(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def _make_pending_llc_owner(session) -> Owner:
    """Create a Property + LLC Owner pending Sunbiz enrichment."""
    prop = Property(
        parcel_id=f"TEST-{datetime.utcnow().timestamp()}",
        address="1 TEST ST, TAMPA, FL",
        county_id="hillsborough",
    )
    session.add(prop)
    session.flush()
    owner = Owner(
        property_id=prop.id,
        owner_name="ACME HOLDINGS LLC",
        owner_type="LLC",
        sunbiz_status="pending",
    )
    session.add(owner)
    session.flush()
    return owner


def test_persist_full_snapshot_writes_owner_and_snapshot_row(fresh_db):
    owner = _make_pending_llc_owner(fresh_db)
    snap = parse_sunbiz_detail(_load("active_member_managed.html"))
    assert snap.status == "ok"

    _persist_snapshot_and_owner(fresh_db, owner, _load("active_member_managed.html"), snap)
    fresh_db.flush()

    fresh_db.refresh(owner)
    assert owner.sunbiz_status == "matched"
    assert owner.sunbiz_doc_number == "L21000123456"
    assert owner.registered_agent_email == "john.smith@example.com"
    assert owner.entity_status == "ACTIVE"
    assert owner.formation_date.isoformat() == "2021-03-15"
    assert owner.principal_address and "TAMPA" in owner.principal_address
    assert owner.managing_members and len(owner.managing_members) == 2
    assert {m["name"] for m in owner.managing_members} == {"SMITH, JOHN A", "DOE, JANE"}
    assert owner.sunbiz_enriched_at is not None

    snap_rows = (
        fresh_db.query(SunbizSnapshotRow)
        .filter_by(sunbiz_doc_number="L21000123456")
        .all()
    )
    assert len(snap_rows) == 1
    row = snap_rows[0]
    assert row.status == "ok"
    assert row.parser_version.startswith("sunbiz-parser/")
    assert row.raw_html and "ACME HOLDINGS LLC" in row.raw_html
    assert row.raw_jsonb["doc_number"] == "L21000123456"


def test_persist_partial_status_still_writes_snapshot_and_marks_matched(fresh_db):
    """status='partial' is useful data — we have a doc match, just incomplete fields."""
    owner = _make_pending_llc_owner(fresh_db)
    snap = parse_sunbiz_detail(_load("partial_no_principal.html"))
    assert snap.status == "partial"

    _persist_snapshot_and_owner(fresh_db, owner, _load("partial_no_principal.html"), snap)
    fresh_db.flush()
    fresh_db.refresh(owner)

    assert owner.sunbiz_status == "matched"
    assert owner.sunbiz_doc_number == "L24000222111"
    assert owner.principal_address is None
    snap_rows = fresh_db.query(SunbizSnapshotRow).filter_by(sunbiz_doc_number="L24000222111").all()
    assert len(snap_rows) == 1
    assert snap_rows[0].status == "partial"


def test_persist_parser_failed_marks_owner_but_writes_no_snapshot(fresh_db):
    owner = _make_pending_llc_owner(fresh_db)
    snap = parse_sunbiz_detail(_load("garbage_html.html"))
    assert snap.status == "parser_failed"

    _persist_snapshot_and_owner(fresh_db, owner, "<html>garbage</html>", snap)
    fresh_db.flush()
    fresh_db.refresh(owner)

    assert owner.sunbiz_status == "parser_failed"
    assert owner.sunbiz_doc_number is None
    assert owner.sunbiz_enriched_at is not None  # don't rescrape immediately
    assert fresh_db.query(SunbizSnapshotRow).count() == 0


def test_mark_status_not_found_sets_status_and_timestamp(fresh_db):
    owner = _make_pending_llc_owner(fresh_db)

    _mark_status(fresh_db, owner, "not_found")
    fresh_db.flush()
    fresh_db.refresh(owner)

    assert owner.sunbiz_status == "not_found"
    assert owner.sunbiz_enriched_at is not None
    assert owner.sunbiz_doc_number is None


def test_persist_overrides_owner_type_to_llc_when_misclassified(fresh_db):
    """If assessor data marked an LLC as 'Individual', a Sunbiz match corrects it."""
    owner = _make_pending_llc_owner(fresh_db)
    owner.owner_type = "Individual"  # simulate bad upstream classification
    fresh_db.flush()

    snap = parse_sunbiz_detail(_load("active_member_managed.html"))
    _persist_snapshot_and_owner(fresh_db, owner, _load("active_member_managed.html"), snap)
    fresh_db.flush()
    fresh_db.refresh(owner)

    assert owner.owner_type == "LLC"
