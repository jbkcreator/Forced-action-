"""P2.5: the closing scoreboard's pipeline read against Postgres (P2.4's tables in a throwaway schema, rolled back)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.scoreboard_pipeline import build_pipeline

NOW = datetime(2026, 11, 16, 0, 20, tzinfo=timezone.utc)  # 7:20 PM EST on Nov 15


@pytest.fixture
def db(closing_db):
    """P2.4's tables plus the lead tables the test-deal filter reads (phone + is_test is all it needs)."""
    schema = closing_db.info["schema"]
    for table in ("contacts", "web_leads", "lendingflow_leads"):
        closing_db.execute(text(f'CREATE TABLE "{schema}".{table} (id BIGSERIAL PRIMARY KEY, phone VARCHAR(20), '
                                "is_test BOOLEAN NOT NULL DEFAULT false)"))
    return closing_db


def stage(db, opp, name, days_ago, phone):
    db.execute(text("INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, entered_at, "
                    "phone) VALUES (:o, :n, :k, :at, :p)"),
               {"o": opp, "n": name, "k": name.lower(), "at": NOW - timedelta(days=days_ago), "p": phone})


def test_counts_active_deals_per_stage_and_lists_the_stuck_ones(db):
    stage(db, "o1", "Application", 6, "+18135550101")
    stage(db, "o2", "Application", 1, "+18135550102")
    stage(db, "o3", "Submitted to lender", 5, "+18135550103")
    stage(db, "o4", "Funded", 40, "+18135550104")
    stage(db, "o5", "Lost", 10, None)
    p = build_pipeline(db, NOW)
    assert p.available and {(s.stage_key, s.deals) for s in p.by_stage} == {
        ("application", 2), ("submitted to lender", 1), ("funded", 1), ("lost", 1)}
    assert p.by_stage[0].stage_key == "application"  # most deals first
    assert (p.active, p.total) == (3, 5)  # Funded and Lost are closed
    assert [(b.phone_last4, b.stage_name, b.days, b.owner) for b in p.blockers] == [
        ("0101", "Application", 6, "borrower"), ("0103", "Submitted to lender", 5, "lender")]  # oldest first


@pytest.mark.parametrize("table", ["contacts", "web_leads", "lendingflow_leads"])
def test_test_deals_are_left_out_of_the_counts_and_the_blockers(db, table):
    stage(db, "real", "Application", 6, "+18135550101")
    stage(db, "test", "Application", 6, "+18135550199")
    db.execute(text(f"INSERT INTO lending.{table} (phone, is_test) VALUES ('+18135550199', true), "
                    "('+18135550101', false)"))
    p = build_pipeline(db, NOW)
    assert p.total == 1 and [b.phone_last4 for b in p.blockers] == ["0101"]


def test_an_empty_pipeline_is_available_with_nothing_in_it(db):
    p = build_pipeline(db, NOW)
    assert p.available and (p.total, p.active, p.blockers) == (0, 0, [])


def test_a_missing_table_marks_the_pipeline_unavailable_and_keeps_the_session_usable(closing_db):
    # no lead tables in this schema: the test-deal filter cannot run
    p = build_pipeline(closing_db, NOW)
    assert not p.available
    assert closing_db.execute(text("SELECT 1")).scalar() == 1
