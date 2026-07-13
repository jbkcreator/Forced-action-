"""
Create email_opt_outs — the email side of cross-channel Do-Not-Contact
suppression (sibling of sms_opt_outs). Backfills from DBPRContact rows
already flagged is_opted_out/is_hard_bounced under the old, campaign-scoped
mechanism, so they're suppressed under the new universal one from day one.

Also backfills from existing sms_opt_outs rows (phone opt-outs recorded
before this cross-channel cascade shipped) — otherwise a contact who opted
out by SMS/IVR pre-deploy keeps receiving email indefinitely, since
record_opt_out() only cascades on the write path, not for history. Matches
by last-10-digits, not exact string equality, because DBPR-imported phones
aren't guaranteed to be stored in normalized E.164 form.

Also adds the functional indexes the eligibility filters need (they compare
lower(dbpr_contacts.email) / lower(work_email) in a correlated NOT EXISTS on
a ~522k-row table) and the pushed_to_instantly watermark column.

Idempotent: CREATE ... IF NOT EXISTS, ADD COLUMN IF NOT EXISTS, backfill
INSERT ON CONFLICT DO NOTHING.

    PYTHONPATH=. python migrations/apply_email_opt_outs.py
    PYTHONPATH=. python migrations/apply_email_opt_outs.py --dry-run
"""
import argparse
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS email_opt_outs (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        source VARCHAR(30) NOT NULL DEFAULT 'manual',
        pushed_to_instantly BOOLEAN NOT NULL DEFAULT false,
        opted_out_at TIMESTAMP NOT NULL DEFAULT now()
    )
    """,
    "ALTER TABLE email_opt_outs ADD COLUMN IF NOT EXISTS pushed_to_instantly BOOLEAN NOT NULL DEFAULT false",
    "CREATE INDEX IF NOT EXISTS ix_email_opt_outs_email ON email_opt_outs (email)",
    "CREATE INDEX IF NOT EXISTS ix_email_opt_outs_unpushed ON email_opt_outs (id) WHERE pushed_to_instantly = false",
    # Functional indexes for the correlated NOT EXISTS suppression filters.
    "CREATE INDEX IF NOT EXISTS ix_dbpr_contacts_email_lower ON dbpr_contacts (lower(email))",
    "CREATE INDEX IF NOT EXISTS ix_dbpr_contacts_work_email_lower ON dbpr_contacts (lower(work_email))",
]

# Both email and work_email are possible send targets (Instantly sends to
# `email or work_email`), so both must be suppressed for a flagged contact.
BACKFILL_SQL = """
INSERT INTO email_opt_outs (email, source, opted_out_at)
SELECT DISTINCT lower(addr), 'hard_bounce', now()
FROM (
    SELECT email AS addr FROM dbpr_contacts
     WHERE email IS NOT NULL AND email <> '' AND (is_opted_out OR is_hard_bounced)
    UNION
    SELECT work_email AS addr FROM dbpr_contacts
     WHERE work_email IS NOT NULL AND work_email <> '' AND (is_opted_out OR is_hard_bounced)
) s
ON CONFLICT (email) DO NOTHING;
"""

COUNT_SQL = """
SELECT count(DISTINCT lower(addr)) FROM (
    SELECT email AS addr FROM dbpr_contacts
     WHERE email IS NOT NULL AND email <> '' AND (is_opted_out OR is_hard_bounced)
    UNION
    SELECT work_email AS addr FROM dbpr_contacts
     WHERE work_email IS NOT NULL AND work_email <> '' AND (is_opted_out OR is_hard_bounced)
) s
"""

# Resolve each existing sms_opt_outs.phone to a sibling email across both
# populations, matching on last-10-digits (not exact string equality) since
# dbpr_contacts.phone is not guaranteed to be normalized E.164.
BACKFILL_SMS_SQL = """
INSERT INTO email_opt_outs (email, source, opted_out_at)
SELECT DISTINCT lower(addr), 'cascaded_from_sms_backfill', now()
FROM (
    SELECT s.email AS addr
    FROM sms_opt_outs so
    JOIN subscribers s
      ON right(regexp_replace(s.phone, '[^0-9]', '', 'g'), 10)
       = right(regexp_replace(so.phone, '[^0-9]', '', 'g'), 10)
    WHERE s.email IS NOT NULL AND s.email <> ''
    UNION
    SELECT COALESCE(NULLIF(dc.email, ''), dc.work_email) AS addr
    FROM sms_opt_outs so
    JOIN dbpr_contacts dc
      ON right(regexp_replace(dc.phone, '[^0-9]', '', 'g'), 10)
       = right(regexp_replace(so.phone, '[^0-9]', '', 'g'), 10)
    WHERE COALESCE(NULLIF(dc.email, ''), dc.work_email) IS NOT NULL
) resolved
WHERE addr IS NOT NULL AND addr <> ''
ON CONFLICT (email) DO NOTHING;
"""

SMS_COUNT_SQL = """
SELECT count(DISTINCT lower(addr)) FROM (
    SELECT s.email AS addr
    FROM sms_opt_outs so
    JOIN subscribers s
      ON right(regexp_replace(s.phone, '[^0-9]', '', 'g'), 10)
       = right(regexp_replace(so.phone, '[^0-9]', '', 'g'), 10)
    WHERE s.email IS NOT NULL AND s.email <> ''
    UNION
    SELECT COALESCE(NULLIF(dc.email, ''), dc.work_email) AS addr
    FROM sms_opt_outs so
    JOIN dbpr_contacts dc
      ON right(regexp_replace(dc.phone, '[^0-9]', '', 'g'), 10)
       = right(regexp_replace(so.phone, '[^0-9]', '', 'g'), 10)
    WHERE COALESCE(NULLIF(dc.email, ''), dc.work_email) IS NOT NULL
) resolved
WHERE addr IS NOT NULL AND addr <> ''
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_db_context() as db:
        if args.dry_run:
            count = db.execute(text(COUNT_SQL)).scalar()
            sms_count = db.execute(text(SMS_COUNT_SQL)).scalar()
            print(f"dry-run: would create/patch email_opt_outs + indexes; "
                  f"backfill ~{count} DBPR-flagged rows + ~{sms_count} sms_opt_outs-resolved rows")
            return 0

        for stmt in DDL_STATEMENTS:
            db.execute(text(stmt))
        result = db.execute(text(BACKFILL_SQL))
        sms_result = db.execute(text(BACKFILL_SMS_SQL))
        db.commit()
        total = db.execute(text("SELECT count(*) FROM email_opt_outs")).scalar()
        print(f"backfilled {result.rowcount} dbpr rows + {sms_result.rowcount} sms-resolved rows; "
              f"email_opt_outs total={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
