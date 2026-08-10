"""Idempotent: add is_test column to subscribers.

Marks rows created during QA/testing so revenue dashboards can exclude them.
Admin-only — the normal checkout path never sets this; it defaults false.
"""
import os
import sys

import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
    ALTER TABLE subscribers
    ADD COLUMN IF NOT EXISTS is_test BOOLEAN NOT NULL DEFAULT FALSE;
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS ix_subscribers_is_test
    ON subscribers (is_test)
    WHERE is_test = TRUE;
""")

print("apply_add_subscriber_is_test: done")
cur.close()
conn.close()
