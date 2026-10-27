"""The is_test flag and the logged cleanup (Q35, Q42)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.lending.test_data import tag_test_rows


@pytest.fixture
def tdb(lending_db):
    """lending_db with the lead, routing, partner and is_test tables, all inside its rolled-back transaction."""
    from migrations.apply_lending_is_test import apply_to as apply_is_test
    from migrations.apply_lending_lender_rule_values import apply_to as apply_rule_values
    from migrations.apply_lending_lendingflow import apply_to as apply_lf
    from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enrichment
    from migrations.apply_lending_lender_routing import apply_to as apply_routing
    from migrations.apply_lending_partner_door import apply_to as apply_partner_door
    from src.lending.models import LendingContact, LendingWebLead

    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)
    LendingWebLead.__table__.create(bind=conn, checkfirst=True)
    apply_lf(conn)
    apply_enrichment(conn)
    apply_rule_values(conn)
    apply_partner_door(conn)
    apply_routing(conn)
    apply_is_test(conn)
    return lending_db


def _lead(db, suffix: str, email: str | None) -> int:
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, loan_type, loan_amount, property_state,"
        " ghl_status, suppressed, raw_payload, consent_status, email)"
        " VALUES (:v, :h, :p, 'FIX_AND_FLIP', 300000, 'FL', 'synced', false, '{}'::jsonb, 'present', :e) RETURNING id"),
        {"v": f"ISTEST-{suffix}", "h": f"istest-hash-{suffix}", "p": f"+1727555{suffix}", "e": email}).scalar_one()


def _flag(db, table: str, row_id: int) -> bool:
    return db.execute(text(f"SELECT is_test FROM {table} WHERE id = :i"), {"i": row_id}).scalar_one()


def test_a_dry_run_reports_what_it_would_tag_and_changes_nothing(tdb):
    test_lead = _lead(tdb, "9101", "qa@example.com")
    real_lead = _lead(tdb, "9102", "borrower@acme.io")
    report = tag_test_rows(tdb, apply=False)
    assert test_lead in report.tagged["lending.lendingflow_leads (email_domain)"]
    assert real_lead not in sum(report.tagged.values(), [])
    assert not report.applied and _flag(tdb, "lending.lendingflow_leads", test_lead) is False
    assert tdb.execute(text("SELECT count(*) FROM lending.test_data_cleanup_log WHERE run_id = :r"), {"r": report.run_id}).scalar() == 0


def test_apply_tags_the_test_rows_logs_each_one_and_leaves_real_rows_alone(tdb):
    from src.lending.routing.router import route_file

    test_lead = _lead(tdb, "9103", "qa@test.com")
    real_lead = _lead(tdb, "9104", "borrower@acme.io")
    no_email_lead = _lead(tdb, "9105", None)
    route_file(test_lead, tdb)  # no lender: held, but the file_routing row exists
    route_file(real_lead, tdb)
    report = tag_test_rows(tdb, apply=True)
    assert _flag(tdb, "lending.lendingflow_leads", test_lead) is True
    assert _flag(tdb, "lending.lendingflow_leads", real_lead) is False
    assert _flag(tdb, "lending.lendingflow_leads", no_email_lead) is False
    routing = dict(tdb.execute(text("SELECT lead_id, is_test FROM lending.file_routing WHERE lead_id = ANY(:l)"),
                               {"l": [test_lead, real_lead]}).all())
    assert routing == {test_lead: True, real_lead: False}  # the routing row follows its lead
    logged = tdb.execute(text("SELECT table_name, rule FROM lending.test_data_cleanup_log WHERE run_id = :r"),
                         {"r": report.run_id}).all()
    assert ("lending.lendingflow_leads", "email_domain") in {tuple(r) for r in logged}
    assert ("lending.file_routing", "test_lead") in {tuple(r) for r in logged}
    assert report.total == len(logged)


def test_a_second_run_tags_nothing_more(tdb):
    _lead(tdb, "9106", "qa@example.org")
    first = tag_test_rows(tdb, apply=True)
    second = tag_test_rows(tdb, apply=True)
    assert first.total >= 1 and second.total == 0


def test_tagging_never_changes_suppression_or_consent(tdb):
    lead = _lead(tdb, "9107", "qa@example.com")
    tdb.execute(text("UPDATE lending.lendingflow_leads SET suppressed = true, suppression_reason = 'suppression_list' WHERE id = :i"),
                {"i": lead})
    tag_test_rows(tdb, apply=True)
    row = tdb.execute(text("SELECT suppressed, suppression_reason, consent_status, is_test FROM lending.lendingflow_leads WHERE id = :i"),
                      {"i": lead}).one()
    assert tuple(row) == (True, "suppression_list", "present", True)


def test_a_test_partner_and_its_files_are_tagged(tdb):
    partner = tdb.execute(text(
        "INSERT INTO lending.partners (name, partner_type, token_hash) VALUES ('TEST R1 Partner', 'wholesaler', 'istest-token-1') RETURNING id")).scalar_one()
    real_partner = tdb.execute(text(
        "INSERT INTO lending.partners (name, partner_type, token_hash) VALUES ('Acme Brokers', 'broker', 'istest-token-2') RETURNING id")).scalar_one()
    file_id = tdb.execute(text(
        "INSERT INTO lending.loan_files (source, partner_id, borrower_first_name, borrower_last_name, product, loan_amount, property_state)"
        " VALUES ('partner', :p, 'A', 'B', 'flip', 250000, 'FL') RETURNING id"), {"p": partner}).scalar_one()
    tag_test_rows(tdb, apply=True)
    assert _flag(tdb, "lending.partners", partner) is True and _flag(tdb, "lending.partners", real_partner) is False
    assert _flag(tdb, "lending.loan_files", file_id) is True


def test_leads_named_by_a_person_are_tagged_with_their_routing_rows_and_nobody_else(tdb):
    from src.lending.routing.router import route_file

    named, other = _lead(tdb, "9108", "someone@acme.io"), _lead(tdb, "9109", "other@acme.io")
    route_file(named, tdb)
    route_file(other, tdb)
    report = tag_test_rows(tdb, apply=True, lead_ids=[named])
    assert _flag(tdb, "lending.lendingflow_leads", named) is True and _flag(tdb, "lending.lendingflow_leads", other) is False
    assert tdb.execute(text("SELECT is_test FROM lending.file_routing WHERE lead_id = :l"), {"l": named}).scalar() is True
    assert tdb.execute(text("SELECT is_test FROM lending.file_routing WHERE lead_id = :l"), {"l": other}).scalar() is False
    logged = tdb.execute(text("SELECT rule FROM lending.test_data_cleanup_log WHERE run_id = :r AND row_id = :i"
                              " AND table_name = 'lending.lendingflow_leads'"), {"r": report.run_id, "i": named}).scalar()
    assert logged == "named_by_operator"


def test_too_many_named_leads_are_refused(tdb):
    with pytest.raises(ValueError, match="at most"):
        tag_test_rows(tdb, apply=False, lead_ids=list(range(1, 60)))
