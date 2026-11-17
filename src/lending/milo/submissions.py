"""Duplicate guard for Milo applications: one ``lending.milo_submissions`` row per loan package.

``claim`` runs before anything reaches Milo. A ``submitted`` package is refused unless forced; a ``pending``
one is refused too (the earlier send may have created the application, so the outcome is unknown); a
``failed`` one may be sent again.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.milo.mapping import MiloApplication

STATUS_PENDING = "pending"
STATUS_SUBMITTED = "submitted"
STATUS_FAILED = "failed"

_CLAIM_SQL = text(
    """
    INSERT INTO lending.milo_submissions (package_ref, status, is_test, application, requested_by)
    VALUES (:ref, 'pending', :is_test, CAST(:application AS jsonb), :by)
    ON CONFLICT (package_ref) DO UPDATE
       SET status = 'pending', application = EXCLUDED.application, requested_by = EXCLUDED.requested_by,
           error = NULL, attempts = lending.milo_submissions.attempts + 1, updated_at = now()
     WHERE lending.milo_submissions.status = 'failed'
        OR (:force AND lending.milo_submissions.status = 'submitted')
    RETURNING id
    """
)
_STATUS_SQL = text("SELECT status, milo_ref FROM lending.milo_submissions WHERE package_ref = :ref")
_FINISH_SQL = text(
    """
    UPDATE lending.milo_submissions
       SET status = :status, milo_ref = COALESCE(:milo_ref, milo_ref), error = :error, updated_at = now()
     WHERE package_ref = :ref AND status = 'pending'
    """
)


@dataclass(frozen=True)
class Claim:
    claimed: bool
    existing_status: Optional[str] = None
    milo_ref: Optional[str] = None


def claim(session: Session, application: MiloApplication, *, is_test: bool, requested_by: Optional[str],
          force: bool = False) -> Claim:
    row = session.execute(_CLAIM_SQL, {
        "ref": application.package_ref, "is_test": is_test, "application": json.dumps(application.to_json()),
        "by": requested_by, "force": force,
    }).first()
    if row is not None:
        session.commit()
        return Claim(claimed=True)
    existing = session.execute(_STATUS_SQL, {"ref": application.package_ref}).first()
    session.rollback()
    return Claim(claimed=False, existing_status=existing[0] if existing else None,
                 milo_ref=existing[1] if existing else None)


def mark_submitted(session: Session, package_ref: str, milo_ref: Optional[str]) -> None:
    session.execute(_FINISH_SQL, {"ref": package_ref, "status": STATUS_SUBMITTED, "milo_ref": milo_ref, "error": None})
    session.commit()


def mark_failed(session: Session, package_ref: str, error: str) -> None:
    session.execute(_FINISH_SQL, {"ref": package_ref, "status": STATUS_FAILED, "milo_ref": None, "error": error[:300]})
    session.commit()
