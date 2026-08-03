"""Allow outbound_drafts.status = 'pending_channel_support'.

A draft whose recommended_channel has no registered Relay dispatcher (today:
sms) is parked in this status on batch approval instead of being left at
'draft'. Left at 'draft' it would be re-selected by builder.build_batch()
on every sweep and re-shown to the founder forever, while never being sent.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_draft_pending_channel_support.py
"""

from sqlalchemy import text

from src.core.database import Database

DDL = [
    """
    ALTER TABLE outbound_drafts DROP CONSTRAINT IF EXISTS ck_outbound_drafts_status
    """,
    """
    ALTER TABLE outbound_drafts ADD CONSTRAINT ck_outbound_drafts_status
        CHECK (status IN (
            'draft',
            'rejected',
            'expired',
            'superseded',
            'approved_pending_send',
            'pending_channel_support'
        ))
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("apply_draft_pending_channel_support: done.")


if __name__ == "__main__":
    main()
