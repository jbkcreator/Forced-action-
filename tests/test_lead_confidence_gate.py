"""A2 integration — the Guess Lead gate actually suppresses on paid surfaces.

Seeds real Property + DistressScore rows in Postgres (fresh_db, rolled back) and
asserts a high-scoring guess lead is withheld from the lead pool and the Lifecycle
sample-leads teaser, while a non-guess lead still sells.
"""
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.core.models import DistressScore, Foreclosure, Owner, Property

ZIP = "00231"  # isolated test ZIP


def _mk_lead(db, parcel, *, is_guess, score=90.0, tier="Gold", vertical="roofing"):
    p = Property(parcel_id=parcel, zip=ZIP, county_id="hillsborough", address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(
        DistressScore(
            property_id=p.id,
            qualified=True,
            final_cds_score=score,
            lead_tier=tier,
            vertical_scores={vertical: score},
            score_date=datetime.now(timezone.utc),
            is_guess_lead=is_guess,
            lead_confidence=0.11 if is_guess else 0.92,
        )
    )
    db.flush()
    return p.id


def test_guess_lead_absent_from_lead_pool(fresh_db):
    from src.agents.tools import read_tools

    guess_id = _mk_lead(fresh_db, "GUESS-1", is_guess=True)
    real_id = _mk_lead(fresh_db, "REAL-1", is_guess=False)

    pool = read_tools.get_lead_pool(zip_code=ZIP, session=fresh_db)
    ids = {row["property_id"] for row in pool}

    assert real_id in ids  # non-guess still sells
    assert guess_id not in ids  # guess lead suppressed


def test_guess_lead_excluded_from_sample_leads(fresh_db):
    from src.services import sample_leads_sms

    _mk_lead(fresh_db, "GUESS-2", is_guess=True, score=95.0, tier="Platinum")
    _mk_lead(fresh_db, "REAL-2", is_guess=False, score=80.0, tier="Gold")

    leads = sample_leads_sms.get_sample_leads(ZIP, "roofing", session=fresh_db)
    parcels = {lead["address"] for lead in leads}

    assert "REAL-2 Test St" in parcels  # non-guess teaser shows
    assert "GUESS-2 Test St" not in parcels  # guess lead withheld even though higher score


def test_evaluate_persists_flag_marks_direct_mail_and_preserves_score(fresh_db):
    from src.services.lead_confidence import evaluate_lead_confidence

    p = Property(parcel_id="EVAL-1", zip=ZIP, county_id="hillsborough", address="EVAL-1 Test St")
    fresh_db.add(p)
    fresh_db.flush()
    # One thin, pending-band signal -> Lead Confidence ~0.11 -> guess.
    fresh_db.add(Foreclosure(
        property_id=p.id, case_number="CASE-EVAL-1",
        filing_date=date.today() - timedelta(days=10), match_confidence=0.78,
    ))
    fresh_db.add(Owner(property_id=p.id, mailing_address="123 Mail St, Tampa FL"))
    fresh_db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=70.0, lead_tier="Gold",
        vertical_scores={"roofing": 70.0}, score_date=datetime.now(timezone.utc),
        is_guess_lead=False,
    ))
    fresh_db.flush()

    result = evaluate_lead_confidence(p, fresh_db, as_of=date.today())

    assert result.is_guess_lead is True
    assert round(result.lead_confidence, 2) == 0.11

    row = fresh_db.execute(
        text("SELECT final_cds_score, lead_confidence, is_guess_lead "
             "FROM distress_scores WHERE property_id = :pid"),
        {"pid": p.id},
    ).one()
    assert float(row.final_cds_score) == 70.0  # invariant: A2 does NOT touch the CDS score
    assert round(float(row.lead_confidence), 2) == 0.11
    assert row.is_guess_lead is True

    dm = fresh_db.execute(
        text("SELECT direct_mail_eligible FROM owners WHERE property_id = :pid"),
        {"pid": p.id},
    ).scalar()
    assert dm is True  # guess lead with a mailing address marked for direct mail


def test_pass_flags_run_and_leaves_solid_lead_sellable(fresh_db):
    from src.services.lead_confidence import run_lead_confidence_pass

    run_id = 778899

    # Thin, fuzzy-matched -> guess.
    guess = Property(parcel_id="PASS-G", zip=ZIP, county_id="hillsborough", address="PASS-G Test St")
    fresh_db.add(guess)
    fresh_db.flush()
    fresh_db.add(Foreclosure(property_id=guess.id, case_number="PASS-G-C",
                             filing_date=date.today() - timedelta(days=5), match_confidence=0.78))
    fresh_db.add(DistressScore(property_id=guess.id, qualified=True, final_cds_score=60.0,
                               lead_tier="Gold", vertical_scores={"roofing": 60.0},
                               score_date=datetime.now(timezone.utc), scoring_run_id=run_id))

    # Strong match + a corroborating second signal -> solid, sells.
    solid = Property(parcel_id="PASS-S", zip=ZIP, county_id="hillsborough", address="PASS-S Test St")
    fresh_db.add(solid)
    fresh_db.flush()
    fresh_db.add(Foreclosure(property_id=solid.id, case_number="PASS-S-C",
                             filing_date=date.today() - timedelta(days=5), match_confidence=0.95))
    fresh_db.add(Owner(property_id=solid.id, owner_name="Jane Doe"))
    fresh_db.add(DistressScore(property_id=solid.id, qualified=True, final_cds_score=85.0,
                               lead_tier="Platinum", vertical_scores={"roofing": 85.0},
                               score_date=datetime.now(timezone.utc), scoring_run_id=run_id))
    fresh_db.flush()

    evaluated = run_lead_confidence_pass(fresh_db, scoring_run_id=run_id)
    assert evaluated == 2

    flags = dict(fresh_db.execute(
        text("SELECT property_id, is_guess_lead FROM distress_scores WHERE scoring_run_id = :r"),
        {"r": run_id},
    ).all())
    assert flags[guess.id] is True
    assert flags[solid.id] is False
