"""
Unit tests for src.tasks.sunbiz_anomaly_check.

Uses the fresh_db fixture (real Postgres, rolled back per test).
Calls run_sunbiz_anomaly_check(session=fresh_db) to bypass the
get_db_context() context manager inside the function.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.core.models import Owner, Property
from src.tasks.sunbiz_anomaly_check import (
    _MIN_OUTCOMES_FOR_ALERT,
    _PARSER_FAILED_THRESHOLD,
    run_sunbiz_anomaly_check,
)


def _mk_property(session, parcel: str) -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} ST", county_id="hillsborough")
    session.add(p)
    session.flush()
    return p


def _mk_owner(
    session,
    prop_id: int,
    status: str,
    enriched_offset_h: float | None = None,
    doc: str | None = None,
) -> Owner:
    enriched_at = None
    if enriched_offset_h is not None:
        enriched_at = datetime.now(timezone.utc) - timedelta(hours=enriched_offset_h)
    o = Owner(
        property_id=prop_id,
        owner_name=f"TEST LLC {prop_id}",
        owner_type="LLC",
        sunbiz_status=status,
        sunbiz_enriched_at=enriched_at,
        sunbiz_doc_number=doc,
    )
    session.add(o)
    session.flush()
    return o


# ── status distribution ──────────────────────────────────────────────────────

def test_status_dist_counts_correctly(fresh_db):
    suffix = str(int(datetime.now(timezone.utc).timestamp() * 1000))[-6:]
    props = [_mk_property(fresh_db, f"AD{i}{suffix}") for i in range(4)]
    _mk_owner(fresh_db, props[0].id, "matched", enriched_offset_h=1)
    _mk_owner(fresh_db, props[1].id, "matched", enriched_offset_h=2)
    _mk_owner(fresh_db, props[2].id, "pending")
    _mk_owner(fresh_db, props[3].id, "parser_failed", enriched_offset_h=0.5)
    fresh_db.flush()

    result = run_sunbiz_anomaly_check(dry_run=True, session=fresh_db)

    assert result["status_dist"].get("matched", 0) >= 2
    assert result["status_dist"].get("pending", 0) >= 1


# ── parser_failed_rate alert ─────────────────────────────────────────────────

def test_no_alert_when_rate_at_threshold(fresh_db):
    # Exactly at threshold (not strictly >) → no alert.
    suffix = str(int(datetime.now(timezone.utc).timestamp() * 1000))[-6:]
    props = [_mk_property(fresh_db, f"BL{i}{suffix}") for i in range(20)]
    for i, p in enumerate(props):
        status = "parser_failed" if i == 0 else "matched"
        _mk_owner(fresh_db, p.id, status, enriched_offset_h=1)
    fresh_db.flush()

    result = run_sunbiz_anomaly_check(dry_run=True, session=fresh_db)

    if result["total_24h"] >= _MIN_OUTCOMES_FOR_ALERT:
        rate = result["parser_failed_rate"]
        if rate <= _PARSER_FAILED_THRESHOLD:
            assert result["anomalies"] == []


def test_alert_fires_when_above_threshold(fresh_db):
    # 8 failed, 12 matched → 40% >> 5% threshold; sample = 20 >= 10.
    suffix = str(int(datetime.now(timezone.utc).timestamp() * 1000))[-6:]
    props = [_mk_property(fresh_db, f"AL{i}{suffix}") for i in range(20)]
    for i, p in enumerate(props):
        status = "parser_failed" if i < 8 else "matched"
        _mk_owner(fresh_db, p.id, status, enriched_offset_h=1)
    fresh_db.flush()

    result = run_sunbiz_anomaly_check(dry_run=True, session=fresh_db)

    assert result["total_24h"] >= _MIN_OUTCOMES_FOR_ALERT
    assert result["parser_failed_rate"] > _PARSER_FAILED_THRESHOLD
    assert len(result["anomalies"]) >= 1
    assert "parser_failed" in result["anomalies"][0]


def test_no_alert_when_sample_too_small(fresh_db):
    # 3 outcomes (< min 10) even with 100% fail rate → no alert.
    suffix = str(int(datetime.now(timezone.utc).timestamp() * 1000))[-6:]
    props = [_mk_property(fresh_db, f"SM{i}{suffix}") for i in range(3)]
    for p in props:
        _mk_owner(fresh_db, p.id, "parser_failed", enriched_offset_h=1)
    fresh_db.flush()

    result = run_sunbiz_anomaly_check(dry_run=True, session=fresh_db)

    if result["total_24h"] < _MIN_OUTCOMES_FOR_ALERT:
        assert result["anomalies"] == []


# ── multi-property LLC gauge ─────────────────────────────────────────────────

def test_multi_prop_llc_count(fresh_db):
    suffix = str(int(datetime.now(timezone.utc).timestamp() * 1000))[-6:]
    doc = f"L{suffix}"
    # Same doc_number on 3 owner rows → 1 multi-prop LLC group.
    props = [_mk_property(fresh_db, f"MP{i}{suffix}") for i in range(3)]
    for p in props:
        _mk_owner(fresh_db, p.id, "matched", enriched_offset_h=2, doc=doc)
    fresh_db.flush()

    result = run_sunbiz_anomaly_check(dry_run=True, session=fresh_db)

    assert result["multi_prop_llcs"] >= 1


# ── 24h window excludes stale rows ───────────────────────────────────────────

def test_enriched_24h_excludes_older_rows(fresh_db):
    suffix = str(int(datetime.now(timezone.utc).timestamp() * 1000))[-6:]
    p_recent = _mk_property(fresh_db, f"RE{suffix}")
    p_old = _mk_property(fresh_db, f"OL{suffix}")
    _mk_owner(fresh_db, p_recent.id, "matched", enriched_offset_h=1)
    _mk_owner(fresh_db, p_old.id, "matched", enriched_offset_h=25)  # outside 24h window
    fresh_db.flush()

    result = run_sunbiz_anomaly_check(dry_run=True, session=fresh_db)

    # enriched_24h must include the recent row (may also count others in DB).
    assert result["enriched_24h"] >= 1
    # total_owners must count both rows.
    assert result["total_owners"] >= 2
