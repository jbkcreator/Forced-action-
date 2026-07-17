"""
Insurance-distress lead segment (Task #7 / ADR 0032).

FEMA-safety: a property qualifies only with BOTH a genuine flip/investment
signal (wholesalers or fix_flip >= Silver floor) AND a recent (within
STACKING_WINDOW_DAYS) storm/flood damage incident. `insurance_claim` alone,
or a stale storm tag, must never qualify — that reproduces the 38k-FEMA
bulk-qualify incident the segment exists to avoid.
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from src.core.models import Property, DistressScore, Incident
from src.services.lead_pool_service import insurance_distress_segment_clause


def _mk_property(db, parcel, zip_code, county, vertical_scores):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=50.0,
        vertical_scores=vertical_scores,
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.flush()
    return p.id


def _mk_incident(db, property_id, incident_type, days_ago):
    db.add(Incident(
        property_id=property_id,
        incident_type=incident_type,
        incident_date=(datetime.now(timezone.utc) - timedelta(days=days_ago)).date(),
    ))
    db.flush()


def _qualifying_ids(db, zip_code, county, now):
    return set(db.execute(
        select(Property.id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(
            Property.zip == zip_code,
            Property.county_id == county,
            insurance_distress_segment_clause(now),
        )
    ).scalars().all())


class TestInsuranceDistressSegment:
    def test_flip_score_plus_recent_storm_qualifies(self, fresh_db):
        now = datetime.now(timezone.utc)
        zip_code, county = "95101", "hillsborough"
        pid = _mk_property(fresh_db, "ID-1", zip_code, county, {"fix_flip": 45.0})
        _mk_incident(fresh_db, pid, "storm_damage", days_ago=10)

        assert _qualifying_ids(fresh_db, zip_code, county, now) == {pid}

    def test_insurance_claim_alone_does_not_qualify(self, fresh_db):
        now = datetime.now(timezone.utc)
        zip_code, county = "95102", "hillsborough"
        pid = _mk_property(fresh_db, "ID-2", zip_code, county, {"fix_flip": 10.0})
        _mk_incident(fresh_db, pid, "insurance_claim", days_ago=10)

        assert _qualifying_ids(fresh_db, zip_code, county, now) == set()

    def test_stale_storm_tag_does_not_qualify(self, fresh_db):
        now = datetime.now(timezone.utc)
        zip_code, county = "95103", "hillsborough"
        pid = _mk_property(fresh_db, "ID-3", zip_code, county, {"fix_flip": 45.0})
        _mk_incident(fresh_db, pid, "storm_damage", days_ago=200)

        assert _qualifying_ids(fresh_db, zip_code, county, now) == set()

    def test_high_flip_score_with_only_insurance_claim_does_not_qualify(self, fresh_db):
        """A genuine flip signal alone is not enough — insurance_claim is
        excluded as a qualifier (ADR 0032 D3), even with no storm/flood tag."""
        now = datetime.now(timezone.utc)
        zip_code, county = "95105", "hillsborough"
        pid = _mk_property(fresh_db, "ID-5", zip_code, county, {"fix_flip": 90.0})
        _mk_incident(fresh_db, pid, "insurance_claim", days_ago=10)

        assert _qualifying_ids(fresh_db, zip_code, county, now) == set()

    def test_flip_score_below_silver_floor_does_not_qualify(self, fresh_db):
        now = datetime.now(timezone.utc)
        zip_code, county = "95104", "hillsborough"
        pid = _mk_property(fresh_db, "ID-4", zip_code, county, {"fix_flip": 39.0})
        _mk_incident(fresh_db, pid, "flood_damage", days_ago=10)

        assert _qualifying_ids(fresh_db, zip_code, county, now) == set()
