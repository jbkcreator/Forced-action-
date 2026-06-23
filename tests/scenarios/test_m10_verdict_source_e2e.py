"""M10 ↔ M6 (Option B) — verdict-sourced delivery (real Postgres via fresh_db).

M6's `verdicts` table doesn't exist on this branch yet, so the "present" tests
CREATE it inside the rolled-back fresh_db transaction (transactional DDL — dropped
on rollback, no shared-DB pollution). `prospects` already exists (M1 backbone).
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.core.models import CustomerAccount, DistressScore, Property, Subscriber, ZipTerritory
from src.services.lead_delivery import claim
from src.tasks.lead_delivery_sweep import (
    _pending_leads_from_verdicts,
    _select_pending,
    _verdicts_available,
)

pytestmark = pytest.mark.scenario_platform

_CREATE_VERDICTS = """
CREATE TABLE verdicts (
    verdict_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prospect_id          UUID NOT NULL,
    grade                VARCHAR NOT NULL,
    contributing_factors JSONB NOT NULL DEFAULT '{}'::jsonb,
    contactability_flag  BOOLEAN NOT NULL DEFAULT FALSE,
    routed_channel       VARCHAR NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _property(db, parcel, *, zip_code="33601", county="hillsborough"):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county)
    db.add(p); db.flush()
    db.add(DistressScore(property_id=p.id, lead_tier="Gold", qualified=True,
                         vertical_scores={"roofing": 80}, county_id=county,
                         score_date=datetime(2099, 6, 1, tzinfo=timezone.utc)))
    db.flush()
    return p


def _prospect(db, property_id):
    return db.execute(text(
        "INSERT INTO prospects (prospect_id, property_id, contactability_state) "
        "VALUES (gen_random_uuid(), :pid, 'contactable') RETURNING prospect_id"
    ), {"pid": property_id}).scalar()


def _verdict(db, prospect_id, grade, channel):
    db.execute(text(
        "INSERT INTO verdicts (prospect_id, grade, routed_channel) VALUES (:p, :g, :c)"
    ), {"p": prospect_id, "g": grade, "c": channel})
    db.flush()


def _account(db, *, cust, entitlement, vertical="roofing", zip_code="33601", county="hillsborough"):
    sub = Subscriber(stripe_customer_id=cust, tier="starter", vertical=vertical,
                     county_id=county, status="active")
    db.add(sub); db.flush()
    acct = CustomerAccount(stripe_customer_id=cust, subscriber_id=sub.id, status="active",
                           lead_entitlement=entitlement,
                           current_period_end=datetime(2099, 7, 1, tzinfo=timezone.utc))
    db.add(acct); db.flush()
    db.add(ZipTerritory(zip_code=zip_code, vertical=vertical, county_id=county,
                        subscriber_id=sub.id, status="locked"))
    db.flush()
    return acct


def test_verdicts_absent_falls_back_to_cds(fresh_db):
    # On this branch (no M6 yet) the verdicts table doesn't exist → CDS source.
    assert _verdicts_available(fresh_db) is False
    _, src = _select_pending(fresh_db, 10, "auto")
    assert src == "cds"


def test_sweep_consumes_verdicts_when_present(fresh_db):
    fresh_db.execute(text(_CREATE_VERDICTS))
    fresh_db.flush()
    assert _verdicts_available(fresh_db) is True

    acct = _account(fresh_db, cust="cus_v_ok", entitlement={"gold": 20})

    # deliverable: Gold routed to a contractor channel
    good = _property(fresh_db, "VERD-good")
    _verdict(fresh_db, _prospect(fresh_db, good.id), "Gold", "contractor_subscription")
    # not deliverable: a loan-lane verdict and a sub_grade verdict
    loan = _property(fresh_db, "VERD-loan")
    _verdict(fresh_db, _prospect(fresh_db, loan.id), "Platinum", "loan_lane")
    rej = _property(fresh_db, "VERD-rej")
    _verdict(fresh_db, _prospect(fresh_db, rej.id), "sub_grade", "recycle_suppress")

    leads = _pending_leads_from_verdicts(fresh_db, 100)
    ids = {l.property_id for l in leads}
    assert good.id in ids                 # contractor-channel verdict sourced
    assert loan.id not in ids             # loan lane is not M10's
    assert rej.id not in ids              # sub_grade never delivered

    lead = next(l for l in leads if l.property_id == good.id)
    assert lead.grade == "Gold" and lead.verticals == ["roofing"]

    d = claim(fresh_db, lead)
    fresh_db.flush()
    assert d is not None and d.account_id == acct.account_id
    assert d.grade == "Gold"


def test_select_pending_forced_verdict_source(fresh_db):
    fresh_db.execute(text(_CREATE_VERDICTS))
    fresh_db.flush()
    _, src = _select_pending(fresh_db, 10, "verdict")
    assert src == "verdict"
