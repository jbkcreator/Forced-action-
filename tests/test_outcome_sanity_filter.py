"""
B0-02 — Outcome Sanity Filter.

The pure helper `classify_realized_outcome` holds the entire shield decision and
is tested exhaustively without a DB. The behavioral tests exercise `post_outcome`
end-to-end against the real consumers (Postgres-only; skip without DATABASE_URL).
"""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.core.models import DistressScore, Property
from src.services.score_feedback_service import (
    classify_realized_outcome,
    get_feedback_rows,
    post_outcome,
)


def _gold_prospect(db) -> str:
    """A Gold-tier prospect (predicted_rate 0.12 via distress_scores fallback)."""
    parcel = "B002-" + db.execute(text("SELECT gen_random_uuid()")).scalar().hex
    prop = Property(parcel_id=parcel, zip="33601", county_id="hillsborough")
    db.add(prop)
    db.flush()
    db.add(DistressScore(property_id=prop.id, lead_tier="Gold", qualified=True,
                         vertical_scores={"roofing": 80}, county_id="hillsborough",
                         score_date=datetime(2099, 6, 1, tzinfo=timezone.utc)))
    db.flush()
    return db.execute(text(
        "INSERT INTO prospects (prospect_id, property_id, contactability_state) "
        "VALUES (gen_random_uuid(), :pid, 'contactable') RETURNING prospect_id"
    ), {"pid": prop.id}).scalar()


def test_no_capital_death_is_shielded():
    """A dead outcome caused by the buyer having no capital nulls the realized
    outcome and preserves the reason — the lead is not down-rated."""
    assert classify_realized_outcome("dead", "no_capital") == (None, "no_capital")


def test_genuine_death_is_not_shielded():
    """A dead with no reason writes 'dead' and still down-rates the lead."""
    assert classify_realized_outcome("dead", None) == ("dead", None)


def test_low_fico_death_is_shielded():
    assert classify_realized_outcome("dead", "low_fico") == (None, "low_fico")


def test_unknown_reason_is_ignored():
    """A reason outside the canonical set behaves exactly as today."""
    assert classify_realized_outcome("dead", "foobar") == ("dead", None)


def test_capacity_reason_on_a_positive_is_ignored():
    """A stray capacity reason must never erase a genuine win."""
    assert classify_realized_outcome("converted", "no_capital") == ("converted", None)


@pytest.mark.parametrize("variant", ["No_Capital", " no_capital ", "NO_CAPITAL", "\tLow_FICO\n"])
def test_case_and_whitespace_variants_still_shield(variant):
    """A case/whitespace variant of a canonical reason must shield, and the
    stored reason is normalised — never a silent no-op."""
    realized, stored = classify_realized_outcome("dead", variant)
    assert realized is None
    assert stored == variant.strip().lower()


def test_blank_reason_is_treated_as_no_reason():
    assert classify_realized_outcome("dead", "   ") == ("dead", None)


# --- Behavioral, against the real consumers (Postgres-only) ---------------

def test_buyer_capacity_death_is_invisible_to_rate_consumers(fresh_db):
    """A genuine dead down-rates the lead; a no_capital dead does not.

    Proven through the consumer-facing read (get_feedback_rows), which filters
    `realized_outcome IS NOT NULL` — the exact guard scoring_training_data and
    check_tier_inversion share. The shielded row must not appear there.
    """
    genuine = _gold_prospect(fresh_db)
    shielded = _gold_prospect(fresh_db)

    post_outcome(fresh_db, prospect_id=genuine, outcome="dead")
    post_outcome(fresh_db, prospect_id=shielded, outcome="dead", reason="no_capital")

    rows = {str(r["prospect_id"]): r for r in fresh_db.execute(text(
        "SELECT prospect_id, realized_outcome, buyer_could_not_act_reason, delta "
        "FROM score_feedback WHERE prospect_id IN (:a, :b)"
    ), {"a": genuine, "b": shielded}).mappings()}

    # Genuine dead: scored, down-rated.
    assert rows[str(genuine)]["realized_outcome"] == "dead"
    assert rows[str(genuine)]["buyer_could_not_act_reason"] is None
    assert rows[str(genuine)]["delta"] is not None

    # Buyer-capacity dead: shielded — no scored outcome, reason preserved.
    assert rows[str(shielded)]["realized_outcome"] is None
    assert rows[str(shielded)]["buyer_could_not_act_reason"] == "no_capital"
    assert rows[str(shielded)]["delta"] is None

    # The shielded row is excluded from the shared `realized_outcome IS NOT NULL`
    # consumer path; the genuine one is not.
    visible = {str(r["prospect_id"]) for r in get_feedback_rows(fresh_db, limit=1000)}
    assert str(genuine) in visible
    assert str(shielded) not in visible


def test_reposting_flips_the_shield_both_ways(fresh_db):
    """Idempotent per prospect: a shield can be applied and removed by re-posting."""
    p = _gold_prospect(fresh_db)

    def state():
        return fresh_db.execute(text(
            "SELECT realized_outcome, buyer_could_not_act_reason "
            "FROM score_feedback WHERE prospect_id = :p"
        ), {"p": p}).mappings().first()

    # genuine dead → shielded
    post_outcome(fresh_db, prospect_id=p, outcome="dead")
    assert state()["realized_outcome"] == "dead"

    post_outcome(fresh_db, prospect_id=p, outcome="dead", reason="no_capital")
    s = state()
    assert s["realized_outcome"] is None and s["buyer_could_not_act_reason"] == "no_capital"

    # re-post without a reason → shield cleared, back to genuine dead
    post_outcome(fresh_db, prospect_id=p, outcome="dead")
    s = state()
    assert s["realized_outcome"] == "dead" and s["buyer_could_not_act_reason"] is None


_TEST_COUNTY = "b0-02-test-county"


def _prospect_with_foreclosure(db, score_date: datetime, filing_date):
    """Gold prospect in an isolated county whose property also has a foreclosure
    filing inside the outcome window — so scoring_training_data emits a training
    row for it (a lone dead alone would be filtered out)."""
    parcel = "B002FC-" + db.execute(text("SELECT gen_random_uuid()")).scalar().hex
    prop = Property(parcel_id=parcel, zip="33601", county_id=_TEST_COUNTY)
    db.add(prop)
    db.flush()
    db.add(DistressScore(property_id=prop.id, lead_tier="Gold", qualified=True,
                         vertical_scores={"roofing": 80}, county_id=_TEST_COUNTY,
                         score_date=score_date))
    db.execute(text(
        "INSERT INTO foreclosures (property_id, case_number, filing_date) "
        "VALUES (:pid, :case, :fdate)"
    ), {"pid": prop.id, "case": parcel, "fdate": filing_date})
    db.flush()
    pid = db.execute(text(
        "INSERT INTO prospects (prospect_id, property_id, contactability_state) "
        "VALUES (gen_random_uuid(), :pid, 'contactable') RETURNING prospect_id"
    ), {"pid": prop.id}).scalar()
    return prop.id, pid


def test_shielded_death_emits_no_negative_training_label(fresh_db):
    """The real scoring_training_data consumer: a genuine dead labels the training
    row feedback_outcome_positive=0 (a negative signal); a buyer-capacity dead
    leaves it NULL (no signal). Isolated by a unique county."""
    from src.services.scoring_training_data import _OUTCOMES_SQL

    now = datetime.now(timezone.utc)
    score_date = now - timedelta(days=20)
    filing_date = (now - timedelta(days=10)).date()

    genuine_prop, genuine_pid = _prospect_with_foreclosure(fresh_db, score_date, filing_date)
    shielded_prop, shielded_pid = _prospect_with_foreclosure(fresh_db, score_date, filing_date)

    post_outcome(fresh_db, prospect_id=genuine_pid, outcome="dead")
    post_outcome(fresh_db, prospect_id=shielded_pid, outcome="dead", reason="no_capital")

    rows = {r["property_id"]: r for r in fresh_db.execute(text(_OUTCOMES_SQL), {
        "since": (now - timedelta(days=40)).date(),
        "fully_observed_cutoff": (now + timedelta(days=1)).date(),
        "county_id": _TEST_COUNTY,
        "outcome_window_days": 30,
    }).mappings()}

    # Both properties appear (foreclosure event), but only the genuine dead carries
    # the negative feedback label; the shielded one carries none.
    assert rows[genuine_prop]["feedback_outcome_positive"] == 0
    assert rows[shielded_prop]["feedback_outcome_positive"] is None


def test_endpoint_shields_over_http(fresh_db, monkeypatch):
    """Full HTTP round-trip: POST with a buyer-capacity reason → 200 and a
    shielded row, exercising request body → service → response."""
    from src.api.admin_router import get_current_admin
    from src.api.deps import get_db
    from src.api.main import app

    prospect = _gold_prospect(fresh_db)
    # Keep the route's commit inside the fixture's rollback-able transaction.
    monkeypatch.setattr(fresh_db, "commit", fresh_db.flush)
    app.dependency_overrides[get_db] = lambda: fresh_db
    app.dependency_overrides[get_current_admin] = lambda: {"sub": "admin"}
    try:
        resp = TestClient(app).post(
            "/api/admin/score-feedback/outcome",
            json={"prospect_id": str(prospect), "outcome": "dead", "reason": "no_capital"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["realized_outcome"] is None
        assert body["buyer_could_not_act_reason"] == "no_capital"
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_admin, None)
