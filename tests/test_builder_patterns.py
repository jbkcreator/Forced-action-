"""WP-T2-8 Stage C — builder pattern detectors + Stage E adapter (fresh_db).

Seeds isolated buyer_entities + permit_staging (+ deeds/properties for
land_to_permit) and asserts each detector fires on a positive fixture and stays
silent on a negative one. Uses fresh_db (real Postgres, rolled back per test).
"""
from datetime import date, timedelta

import pytest
from sqlalchemy import text

from src.services.builder_patterns import (
    detect_concurrent_builders,
    detect_land_to_permit,
    detect_repeat_builders,
    detect_spec_cadence,
    detect_townhome_infill,
    map_hits_to_property_signals,
)

_TODAY = date.today()


def _mk_entity(db, name: str) -> int:
    row = db.execute(
        text("""
            INSERT INTO buyer_entities (canonical_name, entity_type, confidence_score, verification_status)
            VALUES (:n, 'LLC', 90, 'verified') RETURNING id
        """),
        {"n": name},
    ).fetchone()
    return row.id


def _mk_staging_permit(db, permit_number, *, issue_date=None, permit_type="New Construction",
                       completion_status="issued", matched_property_id=None, job_value=300000,
                       county_id="hillsborough") -> int:
    row = db.execute(
        text("""
            INSERT INTO permit_staging
                (permit_number, permit_type, county_id, holder_name, completion_status,
                 status, job_value, issue_date, matched, matched_property_id)
            VALUES (:pn, :pt, :cty, 'HOLDER', :cs, :cs, :jv, :issue, FALSE, :mpid)
            RETURNING id
        """),
        {"pn": permit_number, "pt": permit_type, "cty": county_id, "cs": completion_status,
         "jv": job_value, "issue": issue_date, "mpid": matched_property_id},
    ).fetchone()
    return row.id


def _link(db, entity_id, source_table, source_id):
    db.execute(
        text("""
            INSERT INTO buyer_entity_links
                (buyer_entity_id, source_table, source_id, match_confidence, match_method)
            VALUES (:e, :t, :s, 90, 'manual')
        """),
        {"e": entity_id, "t": source_table, "s": source_id},
    )


def _entity_ids_in(hits, entity_id):
    return any(h.buyer_entity_id == entity_id for h in hits)


# ── repeat_builder ────────────────────────────────────────────────────────────

def test_repeat_builder_positive_and_negative(fresh_db):
    db = fresh_db
    hot = _mk_entity(db, "REPEAT BUILDER LLC")
    p1 = _mk_staging_permit(db, "RB-1", issue_date=_TODAY - timedelta(days=30))
    p2 = _mk_staging_permit(db, "RB-2", issue_date=_TODAY - timedelta(days=200))
    _link(db, hot, "permit_staging", p1)
    _link(db, hot, "permit_staging", p2)

    cold = _mk_entity(db, "ONE PERMIT LLC")
    p3 = _mk_staging_permit(db, "RB-3", issue_date=_TODAY - timedelta(days=30))
    _link(db, cold, "permit_staging", p3)
    db.flush()

    hits = detect_repeat_builders(db)
    assert _entity_ids_in(hits, hot)         # 2 permits / 24mo → fires
    assert not _entity_ids_in(hits, cold)    # 1 permit → silent


def test_repeat_builder_ignores_permits_outside_window(fresh_db):
    db = fresh_db
    e = _mk_entity(db, "STALE REPEAT LLC")
    p1 = _mk_staging_permit(db, "SR-1", issue_date=_TODAY - timedelta(days=30))
    p2 = _mk_staging_permit(db, "SR-2", issue_date=_TODAY - timedelta(days=900))  # >24mo
    _link(db, e, "permit_staging", p1)
    _link(db, e, "permit_staging", p2)
    db.flush()

    hits = detect_repeat_builders(db)
    assert not _entity_ids_in(hits, e)       # only 1 permit inside window


# ── concurrent_builder ────────────────────────────────────────────────────────

def test_concurrent_builder_positive_and_negative(fresh_db):
    db = fresh_db
    hot = _mk_entity(db, "CONCURRENT LLC")
    p1 = _mk_staging_permit(db, "CB-1", issue_date=_TODAY, completion_status="active")
    p2 = _mk_staging_permit(db, "CB-2", issue_date=_TODAY, completion_status="issued")
    _link(db, hot, "permit_staging", p1)
    _link(db, hot, "permit_staging", p2)

    cold = _mk_entity(db, "ONE ACTIVE LLC")
    p3 = _mk_staging_permit(db, "CB-3", issue_date=_TODAY, completion_status="completed")
    p4 = _mk_staging_permit(db, "CB-4", issue_date=_TODAY, completion_status="active")
    _link(db, cold, "permit_staging", p3)
    _link(db, cold, "permit_staging", p4)
    db.flush()

    hits = detect_concurrent_builders(db)
    assert _entity_ids_in(hits, hot)         # 2 active → fires
    assert not _entity_ids_in(hits, cold)    # only 1 active (other completed) → silent


# ── townhome_infill ───────────────────────────────────────────────────────────

def test_townhome_infill_matches_permit_type(fresh_db):
    db = fresh_db
    hot = _mk_entity(db, "TOWNHOME DEV LLC")
    p1 = _mk_staging_permit(db, "TH-1", permit_type="Townhome - 6 unit")
    _link(db, hot, "permit_staging", p1)

    cold = _mk_entity(db, "SFR ONLY LLC")
    p2 = _mk_staging_permit(db, "TH-2", permit_type="Single Family Residence")
    _link(db, cold, "permit_staging", p2)
    db.flush()

    hits = detect_townhome_infill(db)
    assert _entity_ids_in(hits, hot)
    assert not _entity_ids_in(hits, cold)


# ── spec_cadence ──────────────────────────────────────────────────────────────

def test_spec_cadence_detects_repeating_60_90d_gaps(fresh_db):
    db = fresh_db
    hot = _mk_entity(db, "SPEC CADENCE LLC")
    # three permits ~75 days apart → 2 in-cadence gaps
    for i, days in enumerate((0, 75, 150)):
        pid = _mk_staging_permit(db, f"SC-{i}", issue_date=_TODAY - timedelta(days=days))
        _link(db, hot, "permit_staging", pid)

    cold = _mk_entity(db, "IRREGULAR LLC")
    for i, days in enumerate((0, 10, 400)):   # gaps 10d and 390d → neither in band
        pid = _mk_staging_permit(db, f"IR-{i}", issue_date=_TODAY - timedelta(days=days))
        _link(db, cold, "permit_staging", pid)
    db.flush()

    hits = detect_spec_cadence(db)
    assert _entity_ids_in(hits, hot)
    assert not _entity_ids_in(hits, cold)


# ── land_to_permit (needs deeds + property) ───────────────────────────────────

def test_land_to_permit_deed_then_permit_within_90d(fresh_db):
    db = fresh_db
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) VALUES ('L2P-PARCEL', false, now(), now()) RETURNING id")
    ).fetchone().id
    e = _mk_entity(db, "LAND TO PERMIT LLC")

    deed_id = db.execute(
        text("""
            INSERT INTO deeds (property_id, instrument_number, record_date, county_id)
            VALUES (:pid, 'INSTR-L2P', :rd, 'hillsborough') RETURNING id
        """),
        {"pid": prop, "rd": _TODAY - timedelta(days=120)},
    ).fetchone().id
    _link(db, e, "deeds", deed_id)

    # permit issued 60 days after the deed → within 90d window
    permit_id = _mk_staging_permit(
        db, "L2P-1", issue_date=_TODAY - timedelta(days=60), matched_property_id=prop,
    )
    _link(db, e, "permit_staging", permit_id)
    db.flush()

    hits = detect_land_to_permit(db)
    assert _entity_ids_in(hits, e)


# ── Stage E adapter ───────────────────────────────────────────────────────────

def test_adapter_fans_matched_permit_to_property_signal(fresh_db):
    db = fresh_db
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) VALUES ('ADAPT-PARCEL', false, now(), now()) RETURNING id")
    ).fetchone().id
    hot = _mk_entity(db, "ADAPTER LLC")
    p1 = _mk_staging_permit(db, "AD-1", issue_date=_TODAY - timedelta(days=10), matched_property_id=prop)
    p2 = _mk_staging_permit(db, "AD-2", issue_date=_TODAY - timedelta(days=100), matched_property_id=prop)
    _link(db, hot, "permit_staging", p1)
    _link(db, hot, "permit_staging", p2)
    db.flush()

    hits = detect_repeat_builders(db)
    my = [h for h in hits if h.buyer_entity_id == hot]
    assert my
    signals = map_hits_to_property_signals(db, my)
    assert prop in signals
    sig = signals[prop]
    assert sig.trigger == "builder"
    assert "repeat_builder" in sig.patterns
    assert sig.urgency_date == _TODAY - timedelta(days=10)  # newest


def test_adapter_skips_unmatched_staging_permit(fresh_db):
    db = fresh_db
    hot = _mk_entity(db, "UNMATCHED LLC")
    p1 = _mk_staging_permit(db, "UM-1", issue_date=_TODAY - timedelta(days=10), matched_property_id=None)
    p2 = _mk_staging_permit(db, "UM-2", issue_date=_TODAY - timedelta(days=100), matched_property_id=None)
    _link(db, hot, "permit_staging", p1)
    _link(db, hot, "permit_staging", p2)
    db.flush()

    hits = [h for h in detect_repeat_builders(db) if h.buyer_entity_id == hot]
    assert hits
    signals = map_hits_to_property_signals(db, hits)
    # no property key available → no signal emitted (grain limitation)
    assert signals == {}
