"""Reports and caps leave test rows out (Q35)."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.lending.test_data import tag_test_rows


@pytest.fixture
def rdb(lending_db):
    """lending_db with the lead, routing, partner, cohort, suppression and is_test tables (all rolled back)."""
    from migrations.apply_lending_cohorts import apply_to as apply_cohorts
    from migrations.apply_lending_is_test import apply_to as apply_is_test
    from migrations.apply_lending_lender_rule_values import apply_to as apply_rule_values
    from migrations.apply_lending_lender_routing import apply_to as apply_routing
    from migrations.apply_lending_lendingflow import apply_to as apply_lf
    from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enrichment
    from migrations.apply_lending_partner_door import apply_to as apply_partner_door
    from src.lending.models import LendingContact, LendingSuppression, LendingWebLead

    conn = lending_db.get_bind()
    for model in (LendingContact, LendingWebLead, LendingSuppression):
        model.__table__.create(bind=conn, checkfirst=True)
    for apply in (apply_lf, apply_enrichment, apply_rule_values, apply_partner_door, apply_routing, apply_cohorts,
                  apply_is_test):
        apply(conn)
    return lending_db


def _lead(db, suffix: str, email: str | None) -> int:
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, loan_type, loan_amount, property_state,"
        " ghl_status, suppressed, raw_payload, consent_status, email)"
        " VALUES (:v, :h, :p, 'FIX_AND_FLIP', 300000, 'FL', 'synced', false, '{}'::jsonb, 'present', :e) RETURNING id"),
        {"v": f"ISREP-{suffix}", "h": f"isrep-hash-{suffix}", "p": f"+1727555{suffix}", "e": email}).scalar_one()


def test_a_funded_test_file_does_not_count_against_the_seat_cap(rdb):
    from config.lending_routing import SEAT_CAP_COUNTED_ACTION
    from src.lending.routing.router import route_file
    from src.lending.routing.seat import seat_count

    lender = rdb.execute(text(
        "INSERT INTO lending.lender_routing_rules (name, active, is_primary, agreement_status, priority)"
        " VALUES ('Acme Lending', true, true, 'signed', 1) RETURNING id")).scalar_one()
    test_lead, real_lead = _lead(rdb, "9201", "qa@example.com"), _lead(rdb, "9202", "borrower@acme.io")
    route_file(test_lead, rdb)
    route_file(real_lead, rdb)
    for lead in (test_lead, real_lead):
        rdb.execute(text("INSERT INTO lending.file_routing_events (lead_id, lender_id, lender_name, action)"
                         " VALUES (:l, :lender, 'Acme Lending', :a)"),
                    {"l": lead, "lender": lender, "a": SEAT_CAP_COUNTED_ACTION})
    assert seat_count(SimpleNamespace(id=lender), rdb) == 2  # nothing is tagged yet
    tag_test_rows(rdb, apply=True)
    assert seat_count(SimpleNamespace(id=lender), rdb) == 1  # the cap counts the real file only


def test_test_leads_do_not_fill_a_cohort_batch(rdb):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from config.lending_followup import REPLACEMENT_DAY_TIMEZONE
    from src.lending.cohort_report import add_batch, cohorts

    # Bought today (Eastern): leads left in a shared throwaway database from earlier days are not counted.
    today_et = datetime.now(ZoneInfo(REPLACEMENT_DAY_TIMEZONE)).date()
    add_batch(rdb, name="Isolation batch", purchased_at=today_et, lead_count=2)
    _lead(rdb, "9301", "qa@example.com")
    _lead(rdb, "9302", "first@acme.io")
    _lead(rdb, "9303", "second@acme.io")
    before, unplaced_before = cohorts(rdb)
    assert (before[0].leads_delivered, unplaced_before) == (2, 1)  # the test lead took a slot
    tag_test_rows(rdb, apply=True)
    after, unplaced_after = cohorts(rdb)
    assert (after[0].leads_delivered, unplaced_after) == (2, 0)  # both slots are real leads now


def test_the_audience_export_never_includes_a_test_lead(rdb):
    from src.tasks.lending_audience_export import eligible_contacts

    _lead(rdb, "9401", "qa@example.com")
    _lead(rdb, "9402", "real.borrower@acme.io")
    rdb.execute(text("INSERT INTO lending.web_leads (name, phone, email) VALUES ('Web Test', '+17275559403', 'web@test.com')"))
    tag_test_rows(rdb, apply=True)
    emails = {contact.email for contact in eligible_contacts(rdb)}
    assert "real.borrower@acme.io" in emails and not emails & {"qa@example.com", "web@test.com"}


def test_funded_by_partner_leaves_out_a_test_partner_and_its_files(rdb):
    from src.lending.partners import funded_by_partner

    for token, name in (("isrep-token-1", "TEST R1 Partner"), ("isrep-token-2", "Acme Brokers")):
        partner = rdb.execute(text("INSERT INTO lending.partners (name, partner_type, token_hash) VALUES (:n, 'broker', :t) RETURNING id"),
                              {"n": name, "t": token}).scalar_one()
        rdb.execute(text(
            "INSERT INTO lending.loan_files (source, partner_id, borrower_first_name, borrower_last_name, product, loan_amount,"
            " property_state, status, funded_at) VALUES ('partner', :p, 'A', 'B', 'flip', 250000, 'FL', 'funded', now())"),
            {"p": partner})
    before = {name for name, _ in funded_by_partner(rdb, date(2000, 1, 1), date(2100, 1, 1))}
    assert {"TEST R1 Partner", "Acme Brokers"} <= before
    tag_test_rows(rdb, apply=True)
    after = {name for name, _ in funded_by_partner(rdb, date(2000, 1, 1), date(2100, 1, 1))}
    assert "Acme Brokers" in after and "TEST R1 Partner" not in after
