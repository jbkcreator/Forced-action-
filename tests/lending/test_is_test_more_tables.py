"""The cleanup also covers the suppression list, email opt-outs and the other product tables with a test email (Q42)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.lending.test_data import _STEPS, DELETE_ORDER, delete_test_rows, tag_test_rows

NEW_TABLES = ("public.consent_acceptances", "public.non_buyer_nurture_sequences", "public.checkout_recovery",
              "public.email_opt_outs", "lending.suppression_list")


@pytest.fixture
def mdb(lending_db):
    from migrations.apply_lending_is_test import apply_to as apply_is_test
    from src.core.models import EmailOptOut
    from src.lending.models import LendingSuppression

    conn = lending_db.get_bind()
    for table in (LendingSuppression.__table__, EmailOptOut.__table__):
        table.create(bind=conn, checkfirst=True)
    apply_is_test(conn)
    return lending_db


def _exists(db, table: str, email: str) -> bool:
    return db.execute(text(f"SELECT count(*) FROM {table} WHERE email = :e"), {"e": email}).scalar() == 1


def test_every_new_table_is_tagged_by_email_domain_and_is_in_the_delete_list():
    tagged = {table for table, rule, _ in _STEPS if rule == "email_domain"}
    assert set(NEW_TABLES) <= tagged
    assert set(NEW_TABLES) <= {table for table, _ in DELETE_ORDER}


def test_fa_max_persons_and_subscribers_are_never_deleted_by_this_tool():
    deletable = {table for table, _ in DELETE_ORDER}
    assert "public.fa_max_persons" not in deletable and "public.subscribers" not in deletable


def test_a_test_suppression_row_is_tagged_copied_and_deleted_and_a_real_one_stays(mdb):
    for email in ("qa@example.com", "real.person@acme.io"):
        mdb.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) VALUES (:e, 'OPT_OUT', 'email')"),
                    {"e": email})
    tag_test_rows(mdb, apply=True)
    report = delete_test_rows(mdb, apply=True)
    assert not _exists(mdb, "lending.suppression_list", "qa@example.com")
    assert _exists(mdb, "lending.suppression_list", "real.person@acme.io")
    copy = mdb.execute(text("SELECT row_data ->> 'email' FROM lending.test_data_deleted_rows"
                            " WHERE run_id = :r AND table_name = 'lending.suppression_list'"), {"r": report.run_id}).scalar()
    assert copy == "qa@example.com"  # the whole row is kept, so the delete can be undone


def test_a_test_email_opt_out_is_deleted_and_a_real_one_stays(mdb):
    for email in ("unsub@test.com", "real.opt.out@acme.io"):
        mdb.execute(text("INSERT INTO public.email_opt_outs (email, source, pushed_to_instantly, opted_out_at)"
                         " VALUES (:e, 'manual', false, now())"), {"e": email})
    tag_test_rows(mdb, apply=True)
    delete_test_rows(mdb, apply=True)
    assert not _exists(mdb, "public.email_opt_outs", "unsub@test.com")
    assert _exists(mdb, "public.email_opt_outs", "real.opt.out@acme.io")


def test_an_untagged_test_domain_row_is_not_deleted(mdb):
    mdb.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) VALUES ('keep@example.org', 'OPT_OUT', 'sms')"))
    delete_test_rows(mdb, apply=True)  # never tagged
    assert _exists(mdb, "lending.suppression_list", "keep@example.org")
