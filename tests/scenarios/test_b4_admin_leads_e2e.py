"""B4 — admin lead-delivery visibility service (real Postgres via fresh_db)."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.core.models import CustomerAccount, Delivery, Property
from src.services.admin_leads import list_deliveries

pytestmark = pytest.mark.scenario_platform

_T1 = datetime(2099, 6, 5, tzinfo=timezone.utc)
_T2 = datetime(2099, 6, 12, tzinfo=timezone.utc)
_T3 = datetime(2099, 6, 20, tzinfo=timezone.utc)


def _acct(db, cust, company):
    a = CustomerAccount(stripe_customer_id=cust, status="active", company_name=company)
    db.add(a); db.flush()
    return a


def _prop(db, parcel):
    p = Property(parcel_id=parcel, zip="33601", county_id="hillsborough")
    db.add(p); db.flush()
    return p


def _delivery(db, acct, prop, *, grade, when, status="delivered", reason=None):
    d = Delivery(property_id=prop.id, account_id=acct.account_id, grade=grade,
                 vertical="roofing", status=status, rejection_reason=reason,
                 delivered_at=when, billing_period_end=None)
    db.add(d); db.flush()
    return d


@pytest.fixture
def seeded(fresh_db):
    a = _acct(fresh_db, "cus_b4_a", "Acme Roofing")
    b = _acct(fresh_db, "cus_b4_b", "BuildRight")
    da = _delivery(fresh_db, a, _prop(fresh_db, "B4-1"), grade="Gold", when=_T1)
    db_ = _delivery(fresh_db, a, _prop(fresh_db, "B4-2"), grade="Bronze", when=_T2)
    dc = _delivery(fresh_db, b, _prop(fresh_db, "B4-3"), grade="Gold", when=_T3,
                   status="rejected", reason="disconnected")
    fresh_db.flush()
    return {"a": a, "b": b, "da": da, "db": db_, "dc": dc}


def _only(items, account_id):
    return [i for i in items if i["account_id"] == str(account_id)]


def test_lists_all_with_envelope(fresh_db, seeded):
    res = list_deliveries(fresh_db)
    assert {"total", "limit", "offset", "items"} <= set(res.keys())
    mine = [i for i in res["items"] if i["property_id"] in
            {seeded["da"].property_id, seeded["db"].property_id, seeded["dc"].property_id}]
    assert len(mine) == 3
    # newest first
    times = [i["delivered_at"] for i in mine]
    assert times == sorted(times, reverse=True)
    # company name joined
    assert any(i["company_name"] == "Acme Roofing" for i in mine)


def test_filter_by_account(fresh_db, seeded):
    res = list_deliveries(fresh_db, account_id=seeded["a"].account_id)
    assert res["total"] == 2
    assert all(i["account_id"] == str(seeded["a"].account_id) for i in res["items"])


def test_filter_by_grade(fresh_db, seeded):
    res = list_deliveries(fresh_db, grade="Gold", account_id=seeded["a"].account_id)
    assert res["total"] == 1
    assert res["items"][0]["grade"] == "Gold"


def test_filter_by_status_rejected(fresh_db, seeded):
    res = list_deliveries(fresh_db, status="rejected", account_id=seeded["b"].account_id)
    assert res["total"] == 1
    assert res["items"][0]["status"] == "rejected"
    assert res["items"][0]["rejection_reason"] == "disconnected"


def test_filter_by_date_window(fresh_db, seeded):
    # window that includes only T2 (2099-06-12)
    res = list_deliveries(fresh_db, account_id=seeded["a"].account_id,
                          frm=datetime(2099, 6, 10, tzinfo=timezone.utc),
                          to=datetime(2099, 6, 15, tzinfo=timezone.utc))
    assert res["total"] == 1
    assert res["items"][0]["property_id"] == seeded["db"].property_id


def test_pagination(fresh_db, seeded):
    res = list_deliveries(fresh_db, account_id=seeded["a"].account_id, limit=1, offset=0)
    assert res["total"] == 2 and len(res["items"]) == 1 and res["limit"] == 1
    res2 = list_deliveries(fresh_db, account_id=seeded["a"].account_id, limit=1, offset=1)
    assert len(res2["items"]) == 1
    assert res["items"][0]["delivery_id"] != res2["items"][0]["delivery_id"]
