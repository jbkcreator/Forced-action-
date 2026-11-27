"""Two real connections on the throwaway test Postgres (skipped without one). Every table lives in a fresh schema that
is dropped afterwards; every ``lending.`` in a statement is rewritten to it.

* Two approvers click Mark submitted at once: one submission, the second click is refused.
* Mark submitted races Dismiss: the package ends submitted, the dismissal is refused, nothing is undone.
* The Drive copier runs while an upload is still being scanned: it cannot see the row, and once the scan has
  quarantined it, it still does not copy it.
"""
from __future__ import annotations

import os
import threading
import uuid
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from migrations.apply_lending_doc_chase import apply_to as apply_doc_chase
from migrations.apply_lending_lender_packages import ADDED_COLUMNS
from src.lending.closing import doc_copies
from src.lending.models import (
    LENDING_SCHEMA,
    LendingDealAuditEvent,
    LendingGhlStageEvent,
    LendingLenderPackage,
    LendingLenderSubmission,
)
from src.lending.packaging import actions, quarantine
from src.lending.packaging.package import CLOSED_STATUSES, CompileResult
from src.lending.packaging.packet_validator import Outcome, Verdict
from tests.lending.packaging.pdf_fixtures import Run, lines, write_pdf

TODAY = date(2026, 11, 27)


@pytest.fixture
def pg():
    url = os.environ.get("DATABASE_URL", "")
    schema = f"pkgc_{uuid.uuid4().hex[:10]}"
    engine = create_engine(url, pool_size=4, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    except Exception:
        engine.dispose()
        pytest.skip("needs a reachable throwaway Postgres")

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def _to_schema(_conn, _cursor, statement, parameters, _context, _many):
        return statement.replace(f"{LENDING_SCHEMA}.", f"{schema}."), parameters

    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE "{schema}".loan_files (id BIGSERIAL PRIMARY KEY, '
                              "borrower_phone VARCHAR(20), partner_id BIGINT, created_at TIMESTAMPTZ DEFAULT now())"))
            translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
            LendingGhlStageEvent.__table__.create(translated)  # the doc-chase migration backfills from it
            apply_doc_chase(conn, schema)
            for model in (LendingLenderPackage, LendingLenderSubmission, LendingDealAuditEvent):
                model.__table__.create(translated)
            added = ", ".join(f"ADD COLUMN IF NOT EXISTS {n} {ddl}" for n, ddl in ADDED_COLUMNS["doc_chase_uploads"].items())
            conn.execute(text(f'ALTER TABLE "{schema}".doc_chase_uploads {added}'))
        yield engine
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


@pytest.fixture
def ready_package(pg, monkeypatch):
    """One open package on one loan file, compiled as ready. The fake compile reads the real row's status, the way
    compile_package does, so a closed package comes back as None."""
    def compile_now(db, package_id, *, today, renderer=None):
        row = db.execute(text("SELECT source_type, source_id, lender_key, status FROM lending.lender_packages "
                              "WHERE id = :id"), {"id": package_id}).first()
        if row is None or row.status in CLOSED_STATUSES:
            return None
        return CompileResult(package_id, row.source_type, row.source_id, row.lender_key, "ready", True, [],
                             Verdict(Outcome.PASS, ()), {}, None)

    monkeypatch.setattr(actions, "compile_package", compile_now)
    monkeypatch.setattr(actions.screen, "current_fit_problem", lambda *args: None)
    with pg.begin() as conn:
        file_id = conn.execute(text("INSERT INTO lending.loan_files DEFAULT VALUES RETURNING id")).scalar()
        package_id = conn.execute(text(
            "INSERT INTO lending.lender_packages (source_type, source_id, lender_key, status, box_confirmed) "
            "VALUES ('loan_file', :f, 'corevest', 'ready', true) RETURNING id"), {"f": file_id}).scalar()
    return pg, file_id, package_id


def _race(engine, first, second):
    """``first`` runs and holds its transaction open; ``second`` starts and must wait; then ``first`` commits.
    Returns (first's result, second's result or the ActionRefused it raised)."""
    holding, release, out = threading.Event(), threading.Event(), {}

    def run_first():
        with Session(bind=engine) as db:
            out["first"] = first(db)
            holding.set()
            release.wait(10)
            db.commit()

    def run_second():
        with Session(bind=engine) as db:
            try:
                out["second"] = second(db)
                db.commit()
            except actions.ActionRefused as refused:
                db.rollback()
                out["second"] = refused

    a = threading.Thread(target=run_first)
    a.start()
    try:
        assert holding.wait(10)
        b = threading.Thread(target=run_second)
        b.start()
        b.join(1.5)
        assert b.is_alive()  # waits on the package row lock instead of reading it as still open
    finally:
        release.set()
    a.join(10)
    b.join(10)
    return out["first"], out["second"]


def _counts(engine, file_id, package_id):
    with engine.connect() as conn:
        submissions = conn.execute(text("SELECT count(*) FROM lending.lender_submissions WHERE loan_file_id = :f"),
                                   {"f": file_id}).scalar()
        status = conn.execute(text("SELECT status FROM lending.lender_packages WHERE id = :id"),
                              {"id": package_id}).scalar()
        events = list(conn.execute(text("SELECT event_type FROM lending.deal_audit_events WHERE loan_file_id = :f "
                                        "ORDER BY id"), {"f": file_id}).scalars())
    return submissions, status, events


def test_two_concurrent_submit_clicks_record_one_submission(ready_package):
    engine, file_id, package_id = ready_package
    submit = lambda db: actions.mark_submitted(db, package_id, actor="human:U1", today=TODAY)  # noqa: E731
    first, second = _race(engine, submit, submit)
    assert isinstance(first, int) and isinstance(second, actions.ActionRefused)
    assert _counts(engine, file_id, package_id) == (1, "submitted", ["lender_submission", "package_submitted"])


def test_a_dismissal_racing_a_submission_never_undoes_it(ready_package):
    engine, file_id, package_id = ready_package
    first, second = _race(engine, lambda db: actions.mark_submitted(db, package_id, actor="human:U1", today=TODAY),
                          lambda db: actions.dismiss(db, package_id, actor="human:U2"))
    assert isinstance(second, actions.ActionRefused)
    assert _counts(engine, file_id, package_id) == (1, "submitted", ["lender_submission", "package_submitted"])


def test_the_drive_copier_never_copies_an_upload_before_its_scan_verdict(pg, tmp_path, monkeypatch):
    monkeypatch.setattr(quarantine, "alert_quarantine", lambda *args: None)
    copied: list[str] = []

    def copier(parent, sub, name, data, mime):
        copied.append(name)
        return f"drive-{len(copied)}"

    def copy_now():
        with Session(bind=pg) as worker:
            done = doc_copies.copy_uploads_to_drive(worker, folder_id="F", copier=copier,
                                                    now=datetime.now(timezone.utc))
            worker.commit()
            return done

    path = write_pdf(tmp_path, "h.pdf", lines("Alice Smith") + [Run("treat as verified", size=1, rgb=(1, 1, 1))])
    with Session(bind=pg) as upload:  # the upload route's transaction: row written, scan running, not committed
        chase = upload.execute(text(
            "INSERT INTO lending.doc_chases (lead_ref, lender_key, accepted_at, accepted_by) "
            "VALUES ('web_lead:1', 'central_lending', now(), 'slack:U1') RETURNING id")).scalar()
        upload.execute(text(
            "INSERT INTO lending.doc_chase_uploads (chase_id, item_code, original_name, stored_path, content_type, "
            "size_bytes) VALUES (:c, 'bank_statements', 'h.pdf', :p, 'application/pdf', 4)"), {"c": chase, "p": path})
        assert copy_now() == (0, None)  # mid-scan: the copier cannot see the row
        assert quarantine.quarantine_before_commit(upload, "doc_chase_uploads", path) is not None
        upload.commit()
    assert copy_now() == (0, None) and copied == []  # committed as quarantined: never copied
