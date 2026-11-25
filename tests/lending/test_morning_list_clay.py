"""R1 1.3 D9: Clay runs only on list items with a gap, never pays twice for a lead, stops at the daily cap, keeps the LLC
lookup off unless switched on, and a Clay failure never breaks the list (FakeClay + SQLite; no Clay calls)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from config.lending_morning_list import COLD_TOP, NEW_WARM_LEAD
from src.lending.morning_list.clay import ClayError, ClaySettings, FakeClay, enrich_gaps
from src.lending.morning_list.items import ListItem

NOW = datetime(2026, 11, 25, 11, 30, tzinfo=timezone.utc)  # 6:30 AM ET

_DDL = ("CREATE TABLE lending.clay_enrichments (id INTEGER PRIMARY KEY, lead_ref TEXT NOT NULL, field TEXT NOT NULL,"
        " run_id TEXT, status TEXT NOT NULL, value TEXT, requested_at TIMESTAMP NOT NULL, enriched_at TIMESTAMP,"
        " UNIQUE (lead_ref, field))",)


@pytest.fixture
def engine():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        for statement in _DDL:
            conn.execute(text(statement))
    yield eng
    eng.dispose()


def settings(**kw) -> ClaySettings:
    base = dict(email_routine="workflow:wf_email", llc_routine="workflow:wf_llc", email_enabled=True,
                llc_enabled=False, daily_cap=10, ttl_days=30)
    return ClaySettings(**{**base, **kw})


def person(ref: str, *, email: str | None = None) -> ListItem:
    return ListItem(ref=ref, reason=COLD_TOP, name="Bo Fox", entity_name="Fox Holdings LLC", phone=None,
                    line_type="mobile", email=email, brief="")


def llc_only(ref: str) -> ListItem:
    return ListItem(ref=ref, reason=COLD_TOP, name=None, entity_name="Fox Holdings LLC", phone=None,
                    line_type="mobile", brief="", property_address="2 Oak Ave, Miami FL 33101")


def rows(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT lead_ref, field, status, value FROM lending.clay_enrichments"
                                 " ORDER BY lead_ref, field")).all()


def run(engine, clay, items, now=NOW, **kw):
    with Session(engine) as db, db.begin():
        return enrich_gaps(db, items, clay, settings(**kw), now=now)


def test_only_items_missing_an_email_with_a_person_and_company_go_to_the_email_routine(engine) -> None:
    clay = FakeClay()
    run(engine, clay, [person("dlr:1"), person("dlr:2", email="has@x.com"),
                       ListItem(ref="lf:3", reason=NEW_WARM_LEAD, name="Ann Lee", phone=None, line_type="mobile")])
    assert clay.submitted == [("workflow:wf_email", [{"id": "dlr:1", "inputs": {
        "full_name": "Bo Fox", "company_name": "Fox Holdings LLC"}}])]
    assert rows(engine) == [("dlr:1", "email", "pending", None)]


def test_a_later_pass_collects_results(engine) -> None:
    clay = FakeClay(answers={"dlr:1": {"result": {"Work Email": "bo@foxholdings.com"}, "success": True}})
    run(engine, clay, [person("dlr:1")])
    run(engine, clay, [person("dlr:1")], now=NOW + timedelta(minutes=1))
    assert rows(engine) == [("dlr:1", "email", "found", "bo@foxholdings.com")]
    assert len(clay.submitted) == 1  # never paid twice


def test_no_match_is_stored_and_not_retried(engine) -> None:
    clay = FakeClay(answers={"dlr:1": {"result": {}, "success": False}})
    run(engine, clay, [person("dlr:1")])
    run(engine, clay, [person("dlr:1")], now=NOW + timedelta(minutes=1))
    run(engine, clay, [person("dlr:1")], now=NOW + timedelta(days=1))
    assert rows(engine) == [("dlr:1", "email", "not_found", None)]
    assert len(clay.submitted) == 1


def test_daily_cap_counts_every_lookup_requested_today(engine) -> None:
    clay = FakeClay()
    run(engine, clay, [person(f"dlr:{n}") for n in range(5)], daily_cap=3)
    run(engine, clay, [person(f"dlr:{n}") for n in range(5, 8)], daily_cap=3, now=NOW + timedelta(minutes=5))
    assert [r[0] for r in rows(engine)] == ["dlr:0", "dlr:1", "dlr:2"]


def test_cap_zero_means_off(engine) -> None:
    clay = FakeClay()
    run(engine, clay, [person("dlr:1")], daily_cap=0)
    assert clay.submitted == [] and rows(engine) == []


def test_llc_lookup_stays_off_unless_switched_on(engine) -> None:
    clay = FakeClay()
    run(engine, clay, [llc_only("dlr:9")])
    assert clay.submitted == []
    run(engine, clay, [llc_only("dlr:9")], llc_enabled=True)
    assert clay.submitted == [("workflow:wf_llc", [{"id": "dlr:9", "inputs": {
        "llc_name": "Fox Holdings LLC", "state": "FL", "property_address": "2 Oak Ave, Miami FL 33101"}}])]


def test_llc_owner_is_kept_only_when_ownership_is_verified(engine) -> None:
    verified = {"structuredOutputs": {"status": "matched", "ownership_verified": True,
                                      "contact_candidates": [{"name": "Bo Fox"}]}}
    clay = FakeClay(answers={"dlr:9": verified})
    run(engine, clay, [llc_only("dlr:9")], llc_enabled=True)
    run(engine, clay, [llc_only("dlr:9")], llc_enabled=True, now=NOW + timedelta(minutes=1))
    assert rows(engine) == [("dlr:9", "llc_owner", "found", "Bo Fox")]


def test_clay_failure_is_logged_and_nothing_breaks(engine) -> None:
    clay = FakeClay(fail=True)
    stats = run(engine, clay, [person("dlr:1")])
    assert stats.failed and rows(engine) == []


def test_email_switch_off_sends_nothing(engine) -> None:
    clay = FakeClay()
    run(engine, clay, [person("dlr:1")], email_enabled=False)
    assert clay.submitted == []


def test_clay_error_is_the_port_failure_type() -> None:
    with pytest.raises(ClayError):
        FakeClay(fail=True).run("workflow:wf_email", [])


def test_sweep_step_runs_only_6_to_7_40_et_every_5_minutes(engine) -> None:
    from types import SimpleNamespace

    from src.lending.morning_list.step import make_morning_enrichment

    s = SimpleNamespace(lending_morning_list_enabled=True, lending_clay_daily_cap=5, clay_api_key=None,
                        lending_clay_api_base="", lending_morning_list_size=50, lending_morning_list_lead_days=14,
                        lending_david_slack_user_id="", lending_clay_email_routine="workflow:wf_email",
                        lending_clay_llc_routine="", lending_clay_email_enabled=True, lending_clay_llc_enabled=False,
                        lending_clay_ttl_days=30, lending_ghl_app_url="", lending_ghl_location_id="")
    clock = [datetime(2026, 11, 25, 10, 59, tzinfo=timezone.utc)]  # 5:59 AM ET
    step = make_morning_enrichment(engine, s, port=FakeClay(), clock=lambda: clock[0])
    assert step() is None
    clock[0] = NOW  # 6:30
    assert step() is not None
    clock[0] = NOW + timedelta(minutes=2)
    assert step() is None  # spaced 5 minutes
    clock[0] = datetime(2026, 11, 25, 12, 41, tzinfo=timezone.utc)  # 7:41
    assert step() is None
    assert make_morning_enrichment(engine, SimpleNamespace(**{**vars(s), "lending_clay_daily_cap": 0}),
                                   port=FakeClay()) is None
