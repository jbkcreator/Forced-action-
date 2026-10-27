"""The cleanup also tags the suppression list, email opt-outs and the other product tables with a test email (Q42).

Opt-outs and suppression rows are tagged for the reports but never deleted: they are the live send guards.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from src.lending.test_data import _STEPS, DELETE_ORDER, NEVER_DELETED, delete_test_rows, tag_test_rows
from src.services.email_suppression import is_email_suppressed

NEW_TABLES = ("public.consent_acceptances", "public.non_buyer_nurture_sequences", "public.checkout_recovery",
              "public.email_opt_outs", "lending.suppression_list")
GUARD_TABLES = ("public.email_opt_outs", "lending.suppression_list")


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


def test_every_new_table_is_tagged_by_email_domain_and_only_the_send_guards_are_kept_out_of_the_delete_list():
    tagged = {table for table, rule, _ in _STEPS if rule == "email_domain"}
    deletable = {table for table, _ in DELETE_ORDER}
    assert set(NEW_TABLES) <= tagged
    assert set(NEW_TABLES) - set(GUARD_TABLES) <= deletable
    assert not set(GUARD_TABLES) & deletable
    assert set(NEVER_DELETED) == set(GUARD_TABLES)


def test_fa_max_persons_and_subscribers_are_never_deleted_by_this_tool():
    deletable = {table for table, _ in DELETE_ORDER}
    assert "public.fa_max_persons" not in deletable and "public.subscribers" not in deletable


def test_a_test_suppression_row_is_tagged_but_never_deleted(mdb):
    for email in ("qa@example.com", "real.person@acme.io"):
        mdb.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) VALUES (:e, 'OPT_OUT', 'email')"),
                    {"e": email})
    tag_test_rows(mdb, apply=True)
    report = delete_test_rows(mdb, apply=True)
    assert _exists(mdb, "lending.suppression_list", "qa@example.com")
    assert _exists(mdb, "lending.suppression_list", "real.person@acme.io")
    assert mdb.execute(text("SELECT is_test FROM lending.suppression_list WHERE email = 'qa@example.com'")).scalar() is True
    assert "lending.suppression_list" not in report.deleted
    assert mdb.execute(text("SELECT count(*) FROM lending.test_data_deleted_rows WHERE run_id = :r"
                            " AND table_name = 'lending.suppression_list'"), {"r": report.run_id}).scalar() == 0


def test_a_test_email_opt_out_is_tagged_but_never_deleted(mdb):
    for email in ("unsub@test.com", "real.opt.out@acme.io"):
        mdb.execute(text("INSERT INTO public.email_opt_outs (email, source, pushed_to_instantly, opted_out_at)"
                         " VALUES (:e, 'manual', false, now())"), {"e": email})
    tag_test_rows(mdb, apply=True)
    report = delete_test_rows(mdb, apply=True)
    assert _exists(mdb, "public.email_opt_outs", "unsub@test.com")
    assert _exists(mdb, "public.email_opt_outs", "real.opt.out@acme.io")
    assert mdb.execute(text("SELECT is_test FROM public.email_opt_outs WHERE email = 'unsub@test.com'")).scalar() is True
    assert "public.email_opt_outs" not in report.deleted


def test_an_opted_out_test_domain_address_stays_blocked_by_the_send_guard_after_the_cleanup(mdb):
    """test.com is a real domain and the address may belong to a retained subscriber: the guard must still refuse it."""
    address = "retained.subscriber@test.com"
    mdb.execute(text("INSERT INTO public.email_opt_outs (email, source, pushed_to_instantly, opted_out_at)"
                     " VALUES (:e, 'unsubscribe_link', false, now())"), {"e": address})
    assert is_email_suppressed(mdb, address)
    tag_test_rows(mdb, apply=True)
    delete_test_rows(mdb, apply=True)
    assert is_email_suppressed(mdb, address)

    from src.services.email import send_email

    smtp_settings = SimpleNamespace(smtp_host="smtp.invalid", smtp_port=587, smtp_user="user", smtp_pass=SecretStr("pw"),
                                    email_from="from@invalid")
    with patch("src.services.email.get_settings", return_value=smtp_settings), \
            patch("src.services.email.smtplib.SMTP") as smtp:
        assert send_email(address, "subject", "body", db=mdb) is False
    smtp.assert_not_called()


def test_an_untagged_test_domain_row_is_not_deleted(mdb):
    mdb.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) VALUES ('keep@example.org', 'OPT_OUT', 'sms')"))
    delete_test_rows(mdb, apply=True)  # never tagged
    assert _exists(mdb, "lending.suppression_list", "keep@example.org")
