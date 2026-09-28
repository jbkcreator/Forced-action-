"""Lead ownership: one owning campaign per person. Real Postgres via fresh_db (rolled back)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

import config.lead_ownership as cfg
from src.services.lead_ownership import claim_ownership, owning_campaign

HIGH, LOW = "test_high_campaign", "test_low_campaign"
SRC = "property_radar"


@pytest.fixture(autouse=True)
def _priority(monkeypatch):
    monkeypatch.setattr(cfg, "CAMPAIGN_PRIORITY", ("exit_desk", HIGH, "capital_desk_loop", LOW))


def _person(session) -> str:
    return str(session.execute(text(
        "INSERT INTO fa_max_persons (source, source_reference) VALUES ('maturity', 'test-lead-ownership-' || gen_random_uuid()) "
        "RETURNING person_id")).scalar_one())


def _rows(session, person_id: str) -> dict[str, tuple[str, str | None]]:
    rows = session.execute(text(
        "SELECT campaign, status, displaced_by FROM lead_campaign_assignments "
        "WHERE person_id = CAST(:p AS uuid)"), {"p": person_id}).all()
    return {c: (s, d) for c, s, d in rows}


def _engine_enrollment(session, person_id: str, campaign: str, status: str = "active") -> None:
    session.execute(text(
        "CREATE TABLE IF NOT EXISTS fa_max_campaign_enrollments "
        "(person_id uuid, campaign_key text, status text)"))
    session.execute(text(
        "INSERT INTO fa_max_campaign_enrollments VALUES (CAST(:p AS uuid), :c, :s)"),
        {"p": person_id, "c": campaign, "s": status})


def test_first_claim_owns(fresh_db):
    p = _person(fresh_db)
    r = claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC, radar_id="TEST-R1")
    assert (r.status, r.owning_campaign) == ("active", LOW)
    assert owning_campaign(fresh_db, p) == LOW


def test_reclaim_is_idempotent(fresh_db):
    p = _person(fresh_db)
    claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC, radar_id="TEST-R1")
    r = claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC, radar_id="TEST-R1")
    assert r.status == "active"
    assert _rows(fresh_db, p) == {LOW: ("active", None)}


def test_tag_is_stored_on_the_claim(fresh_db):
    p = _person(fresh_db)
    claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC, radar_id="TEST-R1")
    row = fresh_db.execute(text(
        "SELECT source, radar_id FROM lead_campaign_assignments WHERE person_id = CAST(:p AS uuid)"),
        {"p": p}).one()
    assert tuple(row) == (SRC, "TEST-R1")


def test_lower_priority_second_campaign_is_blocked(fresh_db):
    p = _person(fresh_db)
    claim_ownership(fresh_db, person_id=p, campaign=HIGH, source=SRC)
    r = claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC)
    assert (r.status, r.owning_campaign) == ("blocked", HIGH)
    assert _rows(fresh_db, p) == {HIGH: ("active", None), LOW: ("blocked", HIGH)}


def test_higher_priority_later_preempts(fresh_db):
    p = _person(fresh_db)
    claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC)
    r = claim_ownership(fresh_db, person_id=p, campaign=HIGH, source=SRC)
    assert (r.status, r.owning_campaign, r.preempted_campaign) == ("active", HIGH, LOW)
    assert _rows(fresh_db, p) == {HIGH: ("active", None), LOW: ("preempted", HIGH)}


def test_exactly_one_active_owner(fresh_db):
    p = _person(fresh_db)
    for c in (LOW, HIGH, LOW):
        claim_ownership(fresh_db, person_id=p, campaign=c, source=SRC, radar_id=f"TEST-{c}")
    active = fresh_db.execute(text(
        "SELECT COUNT(*) FROM lead_campaign_assignments WHERE person_id = CAST(:p AS uuid) AND status = 'active'"),
        {"p": p}).scalar()
    assert active == 1


def test_higher_engine_enrollment_takes_ownership(fresh_db):
    p = _person(fresh_db)
    claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC, radar_id="TEST-R1")
    _engine_enrollment(fresh_db, p, "capital_desk_loop")
    assert owning_campaign(fresh_db, p) == "capital_desk_loop"
    r = claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC, radar_id="TEST-R2")
    assert (r.status, r.owning_campaign) == ("blocked", "capital_desk_loop")
    assert _rows(fresh_db, p)[LOW][0] == "blocked"


def test_claim_outranking_engine_is_blocked_not_cancelling_it(fresh_db):
    p = _person(fresh_db)
    _engine_enrollment(fresh_db, p, "capital_desk_loop")
    r = claim_ownership(fresh_db, person_id=p, campaign=HIGH, source=SRC)
    assert (r.status, r.owning_campaign) == ("blocked", "capital_desk_loop")
    engine_status = fresh_db.execute(text(
        "SELECT status FROM fa_max_campaign_enrollments WHERE person_id = CAST(:p AS uuid)"), {"p": p}).scalar()
    assert engine_status == "active"


def test_finished_engine_enrollment_is_not_an_owner(fresh_db):
    p = _person(fresh_db)
    _engine_enrollment(fresh_db, p, "exit_desk", status="completed")
    r = claim_ownership(fresh_db, person_id=p, campaign=LOW, source=SRC)
    assert (r.status, r.owning_campaign) == ("active", LOW)


def test_no_owner_for_unclaimed_person(fresh_db):
    assert owning_campaign(fresh_db, _person(fresh_db)) is None


def test_engine_campaign_cannot_be_claimed_here(fresh_db):
    with pytest.raises(ValueError):
        claim_ownership(fresh_db, person_id=_person(fresh_db), campaign="exit_desk", source=SRC)


def test_unknown_campaign_is_rejected(fresh_db):
    with pytest.raises(ValueError):
        claim_ownership(fresh_db, person_id=_person(fresh_db), campaign="not_configured", source=SRC)
