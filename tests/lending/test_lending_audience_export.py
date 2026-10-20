"""Retargeting audience export (Runbook § 4.7): who is included, how values are hashed, and what the files hold.

File tests use fixed contacts in a temporary folder; the database tests (rolled back) only check whether the rows a
test creates are included, since the shared database also holds real leads. They insert rows only, so they use their
own fixture with no DDL (lending_db re-runs migrations that ALTER live tables)."""
from __future__ import annotations

import csv
import hashlib
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.tasks import lending_audience_export as export_task
from src.tasks.lending_audience_export import Contact, eligible_contacts, export, google_email, hashed_rows

DAY = date(2031, 1, 6)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


# ── hashing and de-duplication ──────────────────────────────────────────────

def test_a_known_email_hashes_to_the_published_value():
    rows = hashed_rows([Contact("web", None, "test@example.com")], platform="meta")
    assert rows == [("973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b", "")]


def test_phones_keep_the_plus_for_google_and_drop_it_for_meta():
    contact = Contact("web", "+18135550100", None)
    assert hashed_rows([contact], platform="google") == [("", sha("+18135550100"))]
    assert hashed_rows([contact], platform="meta") == [("", sha("18135550100"))]


@pytest.mark.parametrize("email, expected", [
    ("first.last@gmail.com", "firstlast@gmail.com"),
    ("first.last@googlemail.com", "firstlast@googlemail.com"),
    ("first.last@example.com", "first.last@example.com"),
])
def test_google_drops_periods_only_for_gmail(email, expected):
    assert google_email(email) == expected


def test_each_email_and_phone_appears_once():
    contacts = [
        Contact("lendingflow", "+18135550100", "a@example.com"),
        Contact("web", "+18135550100", "a@example.com"),       # same person from the other source
        Contact("web", "+18135550101", "a@example.com"),       # new phone, known email
        Contact("web", None, "b@example.com"),
    ]
    rows = hashed_rows(contacts, platform="meta")
    assert rows == [(sha("a@example.com"), sha("18135550100")), ("", sha("18135550101")), (sha("b@example.com"), "")]


# ── files ───────────────────────────────────────────────────────────────────

@pytest.fixture
def fixed_contacts(monkeypatch):
    contacts = [Contact("lendingflow", "+18135550100", "a@example.com"), Contact("web", None, "b@gmail.com")]
    monkeypatch.setattr(export_task, "eligible_contacts", lambda db: iter(contacts))
    return contacts


def _read(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def test_both_files_hold_headers_and_hashes_only(fixed_contacts, tmp_path):
    result = export(None, tmp_path, day=DAY)
    google, meta = (tmp_path / f"google_customer_match_{DAY}.csv", tmp_path / f"meta_custom_audience_{DAY}.csv")
    assert result.files == [google, meta] and (result.google_rows, result.meta_rows) == (2, 2)
    assert _read(google) == [["Email", "Phone"], [sha("a@example.com"), sha("+18135550100")], [sha("b@gmail.com"), ""]]
    assert _read(meta)[0] == ["email", "phone"]
    raw = google.read_text() + meta.read_text()
    assert "@" not in raw and "813555" not in raw
    assert not list(tmp_path.glob("*.partial"))


def test_the_file_is_created_owner_only_before_any_hash_is_written(fixed_contacts, tmp_path, monkeypatch):
    import os

    opened: list[tuple[str, int, int]] = []
    real_open = os.open

    def spy(path, flags, mode=0o777, **kwargs):
        opened.append((os.fspath(path), flags, mode))
        return real_open(path, flags, mode, **kwargs)

    monkeypatch.setattr(export_task.os, "open", spy)
    (tmp_path / f"google_customer_match_{DAY}.partial").write_text("stale, maybe world-readable")
    export(None, tmp_path, day=DAY)
    assert [(path.endswith(".partial"), bool(flags & os.O_EXCL), mode) for path, flags, mode in opened] == [
        (True, True, 0o600), (True, True, 0o600)]
    assert not list(tmp_path.glob("*.partial"))  # the stale partial was replaced, not reused with its old mode


def test_a_failed_write_leaves_no_partial_file(fixed_contacts, tmp_path, monkeypatch):
    class Broken:
        def __init__(self, handle):
            pass

        def writerow(self, row):
            pass

        def writerows(self, rows):
            raise OSError("disk full")

    monkeypatch.setattr(export_task.csv, "writer", Broken)
    with pytest.raises(OSError):
        export(None, tmp_path, day=DAY)
    assert not list(tmp_path.glob("*.partial")) and not list(tmp_path.glob("*.csv"))


def test_a_dry_run_writes_nothing(fixed_contacts, tmp_path):
    result = export(None, tmp_path, day=DAY, dry_run=True)
    assert (result.google_rows, result.meta_rows) == (2, 2) and result.files == []
    assert not list(tmp_path.iterdir())


def test_only_the_last_nights_are_kept(fixed_contacts, tmp_path, monkeypatch):
    monkeypatch.setattr(export_task, "KEEP_NIGHTS", 2)
    for d in (1, 2, 3):
        export(None, tmp_path, day=date(2031, 1, d))
    assert sorted(p.name for p in tmp_path.glob("google_*")) == ["google_customer_match_2031-01-02.csv",
                                                                 "google_customer_match_2031-01-03.csv"]
    assert len(list(tmp_path.glob("meta_*"))) == 2


def test_the_job_is_off_without_the_switch_and_folder(monkeypatch, tmp_path):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_audience_export_enabled", False)
    monkeypatch.setattr(get_settings(), "lending_audience_export_dir", str(tmp_path))
    assert export_task.main([]) == 0
    assert not list(tmp_path.iterdir())


# ── who is included (database, rolled back) ─────────────────────────────────

@pytest.fixture
def db():
    """One rolled-back transaction on the shared database; inserts only, short lock and statement timeouts."""
    from config.settings import get_settings

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a live Postgres DATABASE_URL")
    engine = create_engine(str(url), connect_args={"connect_timeout": 20})
    try:
        conn = engine.connect()
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    tx = conn.begin()
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '30s'"))
    conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = '60s'"))
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _phone() -> str:
    return f"+1813557{uuid.uuid4().int % 10_000:04d}"


def _email() -> str:
    return f"audience-{uuid.uuid4().hex[:12]}@example.com"


def _lendingflow(db, phone, email, *, suppressed=False):
    unique = uuid.uuid4().hex
    db.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, phone, email, "
                    "suppressed) VALUES (:v, :h, '{}'::jsonb, :p, :e, :s)"),
               {"v": f"audience-{unique}", "h": unique + unique, "p": phone, "e": email, "s": suppressed})


def _web(db, phone, email, *, suppressed=False):
    db.execute(text("INSERT INTO lending.web_leads (name, phone, email, suppressed) VALUES ('Test Lead', :p, :e, :s)"),
               {"p": phone, "e": email, "s": suppressed})


def _included(db):
    contacts = list(eligible_contacts(db))
    return {c.phone for c in contacts} | {c.email for c in contacts}


def test_non_suppressed_leads_of_both_sources_are_included(db):
    lf_phone, web_email = _phone(), _email()
    _lendingflow(db, lf_phone, None)
    _web(db, _phone(), web_email.upper())
    included = _included(db)
    assert lf_phone in included and web_email in included


def test_suppressed_leads_never_appear(db):
    flagged, listed_phone, listed_email, dnc = _phone(), _phone(), _email(), _phone()
    _lendingflow(db, flagged, _email(), suppressed=True)
    _web(db, listed_phone, None)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                            "VALUES (:p, 'OPT_OUT', 'sms')"), {"p": listed_phone})
    other_phone = _phone()
    _web(db, other_phone, listed_email)  # suppressed by email: the whole contact goes, phone included
    db.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) "
                            "VALUES (:e, 'OPT_OUT', 'email')"), {"e": listed_email})
    _lendingflow(db, dnc, None)
    db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true) "
                            "ON CONFLICT (phone) DO UPDATE SET do_not_contact = true"), {"p": dnc})
    included = _included(db)
    assert not ({flagged, listed_phone, listed_email, other_phone, dnc} & included)
