"""count_available_leads — the shared lead-count predicate used by the
lead-pack checkout gate and (per this ticket) the demo deal-room gate."""
from datetime import datetime, timezone

from src.core.models import DistressScore, Owner, Property
from src.services.lead_pool_service import MIN_EXCLUSIVE_LEADS, count_available_leads


def _mk_property(db, parcel, zip_code, county_id="hillsborough", contactable=True):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county_id, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=80.0,
        vertical_scores={"roofing": 60.0},
        score_date=datetime.now(timezone.utc).date(),
    ))
    if contactable:
        db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high"))
    db.flush()
    return p.id


class TestCountAvailableLeads:
    def test_counts_sellable_leads_in_zip(self, fresh_db):
        zip_code = "50001"
        for i in range(3):
            _mk_property(fresh_db, f"CNT-{i}", zip_code)
        fresh_db.commit()

        n = count_available_leads(
            fresh_db, county_id="hillsborough", zip_code=zip_code,
            segment=None, now=datetime.now(timezone.utc),
        )
        assert n == 3

    def test_caps_at_limit(self, fresh_db):
        zip_code = "50002"
        for i in range(MIN_EXCLUSIVE_LEADS + 3):
            _mk_property(fresh_db, f"CAP-{i}", zip_code)
        fresh_db.commit()

        n = count_available_leads(
            fresh_db, county_id="hillsborough", zip_code=zip_code,
            segment=None, now=datetime.now(timezone.utc),
        )
        assert n == MIN_EXCLUSIVE_LEADS

    def test_excludes_non_contactable_leads(self, fresh_db):
        zip_code = "50003"
        _mk_property(fresh_db, "NOCONTACT-1", zip_code, contactable=False)
        fresh_db.commit()

        n = count_available_leads(
            fresh_db, county_id="hillsborough", zip_code=zip_code,
            segment=None, now=datetime.now(timezone.utc),
        )
        assert n == 0

    def test_different_zip_not_counted(self, fresh_db):
        _mk_property(fresh_db, "OTHERZIP-1", "50004")
        fresh_db.commit()

        n = count_available_leads(
            fresh_db, county_id="hillsborough", zip_code="50005",
            segment=None, now=datetime.now(timezone.utc),
        )
        assert n == 0
