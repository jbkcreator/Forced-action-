"""Portal completion — when a borrower has finished the application.

Spec §Abandonment (item 21): a live sequence halts once the portal is
completed. Lives in the service layer so the state engine, file-state
service and self-serve API can halt abandonment touches without importing
from the agents package.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PORTAL_COMPLETED_LIFECYCLE_STATES = frozenset({
    "application_submitted", "term_sheet_issued", "locked", "funded", "matured",
})


def is_portal_completed(
    db: Session, person_id: str, opportunity_id: Optional[str], sequence_started_at: datetime,
) -> bool:
    """True once the borrower has finished the application, by any signal we
    can observe: our self-serve session handed off to Backflip after this
    sequence started, a Backflip file recorded for the opportunity, or the
    person's lifecycle at application_submitted or later.

    The Backflip-file signal depends on Josh's manual update / mailbox
    parsing, so it can lag the borrower's real completion inside Backflip.
    """
    return bool(db.execute(
        text("""
            SELECT
                EXISTS (
                    SELECT 1 FROM selfserve_sessions s
                    WHERE s.person_id = :pid ::uuid
                      AND s.status = 'handed_off'
                      AND s.handed_off_at >= :since
                )
                OR (CAST(:oid AS uuid) IS NOT NULL AND EXISTS (
                    SELECT 1 FROM fa_max_file_state f WHERE f.opportunity_id = CAST(:oid AS uuid)
                ))
                OR EXISTS (
                    SELECT 1 FROM fa_max_persons p
                    WHERE p.person_id = :pid ::uuid AND p.lifecycle_state = ANY(:states)
                )
        """),
        {
            "pid": person_id, "oid": opportunity_id, "since": sequence_started_at,
            "states": sorted(PORTAL_COMPLETED_LIFECYCLE_STATES),
        },
    ).scalar())


def halt_for_portal_completion(db: Session, person_id: str) -> int:
    """Cancel pending abandonment touches inside the caller's transaction (no
    commit), so it can run from paths that own their own commit. Isolated in
    a savepoint: a failure here never poisons the caller's write."""
    savepoint = db.begin_nested()
    try:
        result = db.execute(
            text("""
                UPDATE abandonment_sequences
                SET cancelled_at = NOW(), cancel_reason = 'portal_completed'
                WHERE person_id = :pid ::uuid AND sent_at IS NULL AND cancelled_at IS NULL
            """),
            {"pid": person_id},
        )
        savepoint.commit()
    except Exception:
        savepoint.rollback()
        logger.exception("portal completion: halt failed for person_id=%s", person_id)
        return 0
    if result.rowcount:
        logger.info(
            "portal completion: halted %d pending abandonment touch(es) for person_id=%s",
            result.rowcount, person_id,
        )
    return result.rowcount
