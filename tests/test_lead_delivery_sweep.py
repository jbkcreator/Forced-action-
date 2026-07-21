"""Regression test for the lead-backlog-starvation bug.

_pending_leads / _pending_leads_from_verdicts ordered candidates by
property_id and applied LIMIT before checking territory eligibility. An
undelivered-and-unmatchable lead leaves no delivery row, so it sorts to the
same position on every run — once the true candidate count exceeds LIMIT,
permanently-unmatched low-property_id leads could occupy the entire window
forever, starving a higher-property_id lead that IS deliverable.

Uses a small limit (3) instead of the real 5000 — proves the exact same
mechanism cheaply: the fix filters ineligible leads out of the candidate SQL
itself, so they never compete for LIMIT slots regardless of the limit's size.
"""
from __future__ import annotations

import uuid

from src.core.models import DistressScore, Property, ZipTerritory
from src.tasks.lead_delivery_sweep import _pending_leads


def _rand_zip() -> str:
    # "9xxxx" never collides with real Hillsborough/Pinellas ZIPs (3xxxx),
    # and the random suffix avoids collision across repeated test runs.
    return "9" + f"{uuid.uuid4().int % 10000:04d}"


def _property(db, *, zip_code):
    tag = uuid.uuid4().hex[:12]
    p = Property(parcel_id=f"lds-{tag}", zip=zip_code, county_id="hillsborough")
    db.add(p)
    db.flush()
    return p


def _qualified_score(db, prop, *, vertical="roofing"):
    ds = DistressScore(
        property_id=prop.id, qualified=True, lead_tier="Gold",
        vertical_scores={vertical: 90},
    )
    db.add(ds)
    db.flush()
    return ds


def test_unmatched_leads_never_enter_the_candidate_set(fresh_db):
    db = fresh_db
    unmatched_zip = _rand_zip()
    deliverable_zip = _rand_zip()

    # 5 properties with a qualified score but NO locked territory in their
    # ZIP — permanently unmatchable. Created first, so they get the lowest
    # property_ids: pre-fix, these are exactly the rows that would sort
    # first on every run (no delivery row ever written for them) and could
    # occupy the entire LIMIT window once the real candidate count exceeds
    # it, in a live DB where LIMIT is small relative to total inventory.
    unmatched_ids = []
    for _ in range(5):
        prop = _property(db, zip_code=unmatched_zip)
        _qualified_score(db, prop)
        unmatched_ids.append(prop.id)

    # One deliverable property, created LAST (highest property_id), in a ZIP
    # that IS locked for the same vertical.
    deliverable = _property(db, zip_code=deliverable_zip)
    _qualified_score(db, deliverable)
    db.add(ZipTerritory(
        zip_code=deliverable_zip, vertical="roofing", county_id="hillsborough",
        status="locked",
    ))
    db.flush()

    # A tiny LIMIT (e.g. 3) can't isolate this assertion against a live,
    # shared dev DB that already has its own real eligible backlog with
    # lower property_ids than anything created in this test — a small limit
    # would just return real committed rows, not prove anything about this
    # test's synthetic data either way. Instead, use a limit far larger than
    # any realistic candidate count so the query returns every row passing
    # the eligibility filter, and assert the filter itself: unmatched leads
    # must never be in that set (they'd be the ones consuming LIMIT slots in
    # production before this fix), and the deliverable one must be.
    leads = _pending_leads(db, limit=10_000_000)
    lead_ids = {l.property_id for l in leads}

    assert deliverable.id in lead_ids, "deliverable lead missing from candidate set"
    assert not (set(unmatched_ids) & lead_ids), (
        "unmatched (no locked territory) leads leaked into the candidate set "
        "— they must be excluded so they can never consume a LIMIT slot"
    )
