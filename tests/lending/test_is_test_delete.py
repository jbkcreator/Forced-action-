"""The guarded delete of tagged test rows (Q42): archive first, never an untagged row, never a blocked contact."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.lending.test_data import DELETE_ORDER, delete_test_rows, tag_test_rows


@pytest.fixture
def ddb(lending_db):
    """lending_db with the lead, routing, partner, suppression, opt-out and is_test tables (all rolled back)."""
    from migrations.apply_lending_is_test import apply_to as apply_is_test
    from migrations.apply_lending_lender_rule_values import apply_to as apply_rule_values
    from migrations.apply_lending_lender_routing import apply_to as apply_routing
    from migrations.apply_lending_lendingflow import apply_to as apply_lf
    from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enrichment
    from migrations.apply_lending_partner_door import apply_to as apply_partner_door
    from src.lending.models import LendingContact, LendingOptOutEvent, LendingSuppression, LendingWebLead

    conn = lending_db.get_bind()
    for model in (LendingContact, LendingWebLead, LendingSuppression, LendingOptOutEvent):
        model.__table__.create(bind=conn, checkfirst=True)
    for apply in (apply_lf, apply_enrichment, apply_rule_values, apply_partner_door, apply_routing, apply_is_test):
        apply(conn)
    return lending_db


def _lead(db, suffix: str, email: str | None) -> int:
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, loan_type, loan_amount, property_state,"
        " ghl_status, suppressed, raw_payload, consent_status, email)"
        " VALUES (:v, :h, :p, 'FIX_AND_FLIP', 300000, 'FL', 'synced', false, '{}'::jsonb, 'present', :e) RETURNING id"),
        {"v": f"ISDEL-{suffix}", "h": f"isdel-hash-{suffix}", "p": f"+1727555{suffix}", "e": email}).scalar_one()


def _exists(db, table: str, row_id: int) -> bool:
    return db.execute(text(f"SELECT count(*) FROM {table} WHERE id = :i"), {"i": row_id}).scalar() == 1


def _archived(db, run_id: str, table: str) -> int:
    return db.execute(text("SELECT count(*) FROM lending.test_data_deleted_rows WHERE run_id = :r AND table_name = :t"),
                      {"r": run_id, "t": table}).scalar()


def test_a_tagged_test_lead_is_deleted_after_a_copy_is_kept_and_real_rows_stay(ddb):
    test_lead, real_lead = _lead(ddb, "9501", "qa@example.com"), _lead(ddb, "9502", "borrower@acme.io")
    tag_test_rows(ddb, apply=True)
    report = delete_test_rows(ddb, apply=True)
    assert not _exists(ddb, "lending.lendingflow_leads", test_lead) and _exists(ddb, "lending.lendingflow_leads", real_lead)
    assert report.deleted["lending.lendingflow_leads"] >= 1
    archived = ddb.execute(text("SELECT row_data ->> 'vendor_lead_id' FROM lending.test_data_deleted_rows"
                                " WHERE run_id = :r AND row_id = :i"), {"r": report.run_id, "i": test_lead}).scalar()
    assert archived == "ISDEL-9501"  # the whole row is kept, so the delete can be undone


def test_a_dry_run_deletes_nothing(ddb):
    lead = _lead(ddb, "9503", "qa@example.com")
    tag_test_rows(ddb, apply=True)
    report = delete_test_rows(ddb, apply=False)
    assert report.deleted["lending.lendingflow_leads"] >= 1 and not report.applied
    assert _exists(ddb, "lending.lendingflow_leads", lead) and _archived(ddb, report.run_id, "lending.lendingflow_leads") == 0


def test_an_untagged_row_is_never_deleted_even_on_a_test_domain(ddb):
    lead = _lead(ddb, "9504", "qa@example.com")  # never tagged
    delete_test_rows(ddb, apply=True)
    assert _exists(ddb, "lending.lendingflow_leads", lead)


def test_a_lead_with_routing_history_is_kept_whole_with_its_routing_row(ddb):
    from src.lending.routing.router import route_file

    lead = _lead(ddb, "9505", "qa@example.com")
    route_file(lead, ddb)  # held: writes a file_routing row and an append-only event
    tag_test_rows(ddb, apply=True)
    report = delete_test_rows(ddb, apply=True)
    assert _exists(ddb, "lending.lendingflow_leads", lead)
    assert ddb.execute(text("SELECT count(*) FROM lending.file_routing WHERE lead_id = :l"), {"l": lead}).scalar() == 1
    assert report.kept["lending.lendingflow_leads"]["has_dependents"] >= 1


def test_a_test_lead_with_a_routing_row_but_no_events_is_deleted_with_that_row(ddb):
    lead = _lead(ddb, "9506", "qa@example.com")
    ddb.execute(text("INSERT INTO lending.file_routing (lead_id, status) VALUES (:l, 'unrouted')"), {"l": lead})
    tag_test_rows(ddb, apply=True)
    report = delete_test_rows(ddb, apply=True)
    assert not _exists(ddb, "lending.lendingflow_leads", lead)
    assert ddb.execute(text("SELECT count(*) FROM lending.file_routing WHERE lead_id = :l"), {"l": lead}).scalar() == 0
    assert _archived(ddb, report.run_id, "lending.file_routing") == 1  # the child row is copied too


def test_a_test_contact_on_the_do_not_contact_or_suppression_list_is_never_deleted(ddb):
    blocked = ddb.execute(text("INSERT INTO lending.contacts (phone, email, do_not_contact, is_test)"
                               " VALUES ('+17275559601', 'qa1@example.com', true, true) RETURNING id")).scalar_one()
    suppressed = ddb.execute(text("INSERT INTO lending.contacts (phone, email, is_test)"
                                  " VALUES ('+17275559602', 'qa2@example.com', true) RETURNING id")).scalar_one()
    ddb.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel)"
                     " VALUES ('+17275559602', 'OPT_OUT', 'sms')"))
    free = ddb.execute(text("INSERT INTO lending.contacts (phone, email, is_test)"
                            " VALUES ('+17275559603', 'qa3@example.com', true) RETURNING id")).scalar_one()
    report = delete_test_rows(ddb, apply=True)
    assert _exists(ddb, "lending.contacts", blocked) and _exists(ddb, "lending.contacts", suppressed)
    assert not _exists(ddb, "lending.contacts", free)
    assert report.kept["lending.contacts"]["protected_contact"] == 2


def test_subscribers_are_never_in_the_delete_list():
    assert "public.subscribers" not in {table for table, _ in DELETE_ORDER}
