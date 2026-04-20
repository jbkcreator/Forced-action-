"""subscriber_email_dedup_index

Normalise subscriber emails to lowercase, resolve existing active/grace duplicates,
and add a partial unique index on (lower(email), vertical, county_id) scoped to
active/grace rows only — so churned/cancelled rows remain as history and
re-subscriptions are permitted.

Steps (all in one transaction):
  1. Normalise — SET email = lower(trim(email))
  2. Dedup    — for each (lower(email), vertical, county_id) group with >1 active/grace
               row, keep the oldest (lowest id), cancel the rest, and reassign
               zip_territories + sent_leads to the surviving row.
  3. Index    — CREATE UNIQUE INDEX (partial) enforces the invariant going forward.

Run the pre-migration check manually before applying:
    SELECT lower(email), vertical, county_id, count(*)
    FROM subscribers
    WHERE status IN ('active','grace')
    GROUP BY 1,2,3
    HAVING count(*) > 1;

Revision ID: l2m3n4o5p6q7
Revises: k1l2m3n4o5p6
Create Date: 2026-04-20 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'l2m3n4o5p6q7'
down_revision: Union[str, None] = '4fd02fc1f9a4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Step 1: Normalise all existing emails to lowercase ────────────────
    op.execute("UPDATE subscribers SET email = lower(trim(email)) WHERE email IS NOT NULL")

    # ── Step 2: Resolve duplicates ────────────────────────────────────────
    # For each group of active/grace rows sharing the same (lower(email), vertical,
    # county_id), keep the row with the lowest id (earliest subscriber), cancel the
    # rest, and reassign their FK references so no data is orphaned.

    # 2a. Find all duplicate groups and their survivors
    op.execute("""
        CREATE TEMP TABLE _sub_survivors AS
        SELECT
            lower(email)  AS norm_email,
            vertical,
            county_id,
            min(id)       AS keep_id
        FROM subscribers
        WHERE status IN ('active', 'grace')
          AND email IS NOT NULL
        GROUP BY lower(email), vertical, county_id
        HAVING count(*) > 1
    """)

    # 2b. Collect the ids that will be cancelled (all except keep_id per group)
    op.execute("""
        CREATE TEMP TABLE _sub_cancelled AS
        SELECT s.id AS cancelled_id, sv.keep_id AS survivor_id
        FROM subscribers s
        JOIN _sub_survivors sv
          ON lower(s.email) = sv.norm_email
         AND s.vertical     = sv.vertical
         AND s.county_id    = sv.county_id
        WHERE s.status IN ('active', 'grace')
          AND s.id <> sv.keep_id
    """)

    # 2c. Reassign zip_territories to the surviving subscriber
    op.execute("""
        UPDATE zip_territories zt
        SET subscriber_id = sc.survivor_id
        FROM _sub_cancelled sc
        WHERE zt.subscriber_id = sc.cancelled_id
    """)

    # 2d. Reassign sent_leads to the surviving subscriber
    #     Use ON CONFLICT DO NOTHING so we don't violate uq_sent_lead if both
    #     the cancelled and surviving subscriber already have the same property.
    op.execute("""
        INSERT INTO sent_leads (subscriber_id, property_id, sent_at)
        SELECT sc.survivor_id, sl.property_id, sl.sent_at
        FROM sent_leads sl
        JOIN _sub_cancelled sc ON sl.subscriber_id = sc.cancelled_id
        ON CONFLICT DO NOTHING
    """)
    op.execute("""
        DELETE FROM sent_leads sl
        USING _sub_cancelled sc
        WHERE sl.subscriber_id = sc.cancelled_id
    """)

    # 2e. Cancel the duplicate rows
    op.execute("""
        UPDATE subscribers
        SET status = 'cancelled'
        WHERE id IN (SELECT cancelled_id FROM _sub_cancelled)
    """)

    # 2f. Clean up temp tables
    op.execute("DROP TABLE _sub_survivors")
    op.execute("DROP TABLE _sub_cancelled")

    # ── Step 3: Create partial unique index ───────────────────────────────
    # Scoped to active/grace only — cancelled/churned rows are excluded so
    # the same email can re-subscribe after cancellation.
    op.execute("""
        CREATE UNIQUE INDEX uq_subscriber_email_vertical_active
        ON subscribers (lower(email), vertical, county_id)
        WHERE status IN ('active', 'grace')
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_subscriber_email_vertical_active")
    # Email normalisation and dedup cancellations are not reversed —
    # reverting them would risk re-introducing duplicates.
