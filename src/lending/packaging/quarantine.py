"""R1 1.7 — Quarantine record + ops alert helpers (D11).

Keeps the upload row quarantined and fires one ops Slack post.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def record_quarantine(db: Session, upload_id: int, upload_table: str, reason: str) -> None:
    """Mark the upload row quarantined. Idempotent — a second call does not overwrite an existing reason."""
    if upload_table not in ("doc_chase_uploads", "partner_file_uploads"):
        raise ValueError(f"unknown upload table: {upload_table!r}")
    db.execute(
        text(f"""
            UPDATE lending.{upload_table}
            SET quarantine_status = 'quarantined',
                quarantine_reason = :reason
            WHERE id = :id
              AND (quarantine_status IS NULL OR quarantine_status <> 'quarantined')
        """),  # noqa: S608 — table name validated above
        {"id": upload_id, "reason": reason[:300]},
    )


def alert_quarantine(*, chase_id: Optional[int] = None, file_id: Optional[int] = None,
                     upload_id: int, reasons: tuple[str, ...]) -> None:
    """Post one ops Slack alert for a quarantined upload. Fire-and-forget; never raises."""
    try:
        from config.settings import get_settings
        from src.lending.ops_alerts import post_ops_alert

        ref = f"chase {chase_id}" if chase_id else f"file {file_id}"
        reasons_text = "; ".join(reasons) or "unknown reason"
        post_ops_alert(
            f":radioactive_sign: *Document quarantined* (upload #{upload_id}, {ref})\n"
            f"Reason: {reasons_text}\n"
            f"Admin release required. Never send to a lender."
        )
    except Exception as exc:
        logger.warning("[quarantine] ops alert failed for upload %s: %s", upload_id, type(exc).__name__)
