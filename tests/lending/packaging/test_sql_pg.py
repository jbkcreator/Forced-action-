"""Every SQL statement the review fixes added runs against real Postgres tables (the fake session never parses SQL).
Uses the rolled-back ``lending_db`` session; skipped without the throwaway test database."""
from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.lending.packaging import actions, package, screen
from src.lending.packaging.packet_validator import DOC_CHASE_UPLOADS, PARTNER_FILE_UPLOADS
from src.tasks import secondary_screen_job

STATEMENTS = [
    (screen._STILL_ELIGIBLE, {"st": "loan_file", "sid": -1, "excluded_holds": screen.EXCLUDED_HOLDS,
                              "unassigned": screen.UNASSIGNED_STATUSES}),
    (screen._STILL_ELIGIBLE, {"st": "lf_lead", "sid": -1, "excluded_holds": screen.EXCLUDED_HOLDS,
                              "unassigned": screen.UNASSIGNED_STATUSES}),
    (screen._RETIRE, {"st": "loan_file", "sid": -1, "keys": ["corevest"]}),
    (secondary_screen_job._UNDELIVERED, {}),
    (actions._LOCK, {"id": -1}),
    (actions._UPLOAD_OWNER[DOC_CHASE_UPLOADS], {"id": -1}),
    (actions._UPLOAD_OWNER[PARTNER_FILE_UPLOADS], {"id": -1}),
    (package._LOAN_FILE_DOCS, {"chase": DOC_CHASE_UPLOADS, "partner": PARTNER_FILE_UPLOADS, "id": -1}),
    (package._LEAD_DOCS, {"chase": DOC_CHASE_UPLOADS, "partner": PARTNER_FILE_UPLOADS, "ref": "lendingflow_lead:-1"}),
    (package._GUARANTORS, {"id": -1}),
    (package._SAVE, {"id": -1, "status": "draft", "checklist": "[]", "verdict": "{}", "pdf": None}),
]


@pytest.mark.parametrize("statement, params", STATEMENTS)
def test_statement_runs_on_the_real_schema(lending_db, statement, params):
    with lending_db.begin_nested():
        result = lending_db.execute(statement, params)
        rows = result.all() if result.returns_rows else []
    assert rows == []


def test_supersede_and_guarantor_writes_run_on_the_real_schema(lending_db):
    with lending_db.begin_nested():
        assert lending_db.execute(text("UPDATE lending.doc_chase_uploads SET superseded_by = :by "
                                       "WHERE id = :id AND superseded_by IS NULL"),
                                  {"id": -1, "by": "doc_chase_uploads:-2"}).rowcount == 0
    # No loan file -1: the foreign key refuses the row, which proves the statement and its columns are valid
    # (a wrong column would be a ProgrammingError instead).
    with pytest.raises(IntegrityError):
        with lending_db.begin_nested():
            lending_db.execute(actions._ADD_GUARANTOR, {"id": -1, "name": "Test Guarantor", "actor": "human:test"})
