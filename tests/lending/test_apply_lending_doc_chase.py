from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from config.lending_closing import LENDER_CHECKLISTS
from migrations.apply_lending_doc_chase import apply_to


def _insert_chase(db, phone: str, status: str = "open") -> None:
    db.execute(text("INSERT INTO lending.doc_chases (lead_ref, phone, lender_key, accepted_at, accepted_by, status) "
                    "VALUES ('web_lead:1', :phone, 'central_lending', now(), 'slack:U1', :status)"),
               {"phone": phone, "status": status})


def test_apply_is_idempotent_and_backfills_current_stage(closing_db):
    closing_db.execute(text(
        "INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, stage_name, stage_key, event_at, raw_event) VALUES "
        "('opp1', 'Application', 'application', now() - interval '5 days', '{}'),"
        "('opp1', 'Submitted to lender', 'submitted to lender', now() - interval '2 days', '{}')"))
    conn = closing_db.connection()
    apply_to(conn, closing_db.info["schema"])
    apply_to(conn, closing_db.info["schema"])
    rows = closing_db.execute(text("SELECT stage_key FROM lending.ghl_opportunity_stage")).scalars().all()
    assert rows == ["submitted to lender"]


def test_only_one_open_chase_per_phone(closing_db):
    _insert_chase(closing_db, "+18135550101", status="complete")
    _insert_chase(closing_db, "+18135550101")
    with pytest.raises(IntegrityError):
        with closing_db.begin_nested():
            _insert_chase(closing_db, "+18135550101")


def test_central_checklist_matches_josh_nov_2_list():
    assert [item.code for item in LENDER_CHECKLISTS["central_lending"]] == [
        "purchase_agreement", "valuation", "scope_of_work", "entity_docs", "government_id", "bank_statements"]
    assert "backflip" not in LENDER_CHECKLISTS


def test_a_rerun_adds_indexes_created_after_the_first_run(closing_db):
    """An earlier version of the migration ran without the one-open-chase-per-email index: a re-run adds it."""
    schema = closing_db.info["schema"]
    closing_db.execute(text(f'DROP INDEX "{schema}".uq_lending_doc_chases_open_email'))
    apply_to(closing_db.connection(), schema)
    closing_db.execute(text("INSERT INTO lending.doc_chases (lead_ref, email, lender_key, accepted_at, accepted_by) "
                            "VALUES ('web_lead:1', 'Sam@Example.com', 'central_lending', now(), 'slack:U1')"))
    with pytest.raises(IntegrityError):
        with closing_db.begin_nested():
            closing_db.execute(text("INSERT INTO lending.doc_chases (lead_ref, email, lender_key, accepted_at, "
                                    "accepted_by) VALUES ('web_lead:2', 'sam@example.com', 'central_lending', now(), "
                                    "'slack:U1')"))


def test_a_rerun_adds_the_drive_copy_columns(closing_db):
    schema = closing_db.info["schema"]
    closing_db.execute(text(f'ALTER TABLE "{schema}".doc_chase_uploads DROP COLUMN drive_file_id, '
                            "DROP COLUMN drive_attempts"))
    apply_to(closing_db.connection(), schema)
    apply_to(closing_db.connection(), schema)
    columns = set(closing_db.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = "
        "'doc_chase_uploads'"), {"s": schema}).scalars())
    assert {"drive_file_id", "drive_attempts"} <= columns
