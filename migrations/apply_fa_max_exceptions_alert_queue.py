"""
FA Max EXCEPTIONS alert durability (WP-T2-1 go-live review, 2026-09).

Creates fa_max_exceptions_alert_queue: a durable pending-alert queue so a
Slack outage or a process crash between deciding to page EXCEPTIONS and the
Slack call completing no longer silently drops the alert. See the
FaMaxExceptionsAlertQueue model docstring in src/core/models.py for the
full design, including the documented (accepted, not solved) duplicate-post
risk on an ambiguous Slack result.

Idempotent: safe to re-run (CREATE TABLE IF NOT EXISTS, CREATE INDEX IF NOT
EXISTS).

Run:
  PYTHONPATH=. python migrations/apply_fa_max_exceptions_alert_queue.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fa_max_exceptions_alert_queue (
        id               BIGSERIAL   PRIMARY KEY,
        venture_key      TEXT        NOT NULL,
        rule             TEXT        NOT NULL,
        message          TEXT        NOT NULL,
        status           TEXT        NOT NULL DEFAULT 'pending'
                                     CHECK (status IN ('pending', 'sent')),
        attempts         INTEGER     NOT NULL DEFAULT 0,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_attempt_at  TIMESTAMPTZ,
        sent_at          TIMESTAMPTZ,
        error            TEXT
    );
    """,
    # claimed_until (code-review finding, 2026-09): added after the initial
    # CREATE TABLE above -- ADD COLUMN IF NOT EXISTS so this migration stays
    # idempotent and safe to re-run against a DB that already has the
    # pre-fix table. See FaMaxExceptionsAlertQueue's model docstring for why
    # this exists: without it, enqueue_and_attempt()'s immediate delivery
    # attempt can race an overlapping drain_pending() tick (or two
    # overlapping drain ticks) into posting the same alert twice.
    """
    ALTER TABLE fa_max_exceptions_alert_queue
        ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_exceptions_alert_queue_status
        ON fa_max_exceptions_alert_queue (status, created_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_exceptions_alert_queue_dedup
        ON fa_max_exceptions_alert_queue (venture_key, rule, created_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_exceptions_alert_queue_claim
        ON fa_max_exceptions_alert_queue (status, claimed_until);
    """,
    # Code-review finding (third round, 2026-09): enqueue_and_attempt()'s
    # own dedup check (_recently_queued: SELECT, then INSERT if nothing
    # found) is a classic check-then-act race -- two concurrent producers
    # (e.g. two sweep ticks, or the health monitor and a sweep, firing
    # close together) can both SELECT before either commits its INSERT, and
    # both then insert a 'pending' row for the same (venture_key, rule).
    # Reproduced directly with a widened race window. A SELECT-based check
    # can never fully close this on its own -- only a DB-enforced
    # constraint can. This partial unique index makes a second 'pending'
    # row for the same (venture_key, rule) impossible at the database
    # level, regardless of how the application races; exceptions_alert_
    # queue.py catches the resulting IntegrityError and treats it exactly
    # like the existing dedup skip (nothing new needed, another caller has
    # it covered). Partial (WHERE status = 'pending') rather than a plain
    # unique index because 'sent' rows must remain free to accumulate --
    # only ever one 'pending' row per condition needs enforcing.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_fa_max_exceptions_alert_queue_pending_dedup
        ON fa_max_exceptions_alert_queue (venture_key, rule)
        WHERE status = 'pending';
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_fa_max_exceptions_alert_queue: done")


if __name__ == "__main__":
    main()
