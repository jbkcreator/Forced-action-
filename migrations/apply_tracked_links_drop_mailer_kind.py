"""FA Max WP-7 follow-up — drop the 'property_mailer' tracked-link kind.

Physical mail campaigns are not part of this client's workflow and
'property_mailer' was never in the spec (forced-action-max-amendment-1-
detail.md item 20 names only partner/source/campaign) — it was an
engineering addition made without client confirmation. Removed 2026-09-18.

Idempotent, safe to re-run. Any existing 'property_mailer' rows are
reclassified to 'source' before the constraint is tightened, so this never
fails on a DB that already has such rows (none are expected in practice).

Usage: PYTHONPATH=. python migrations/apply_tracked_links_drop_mailer_kind.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "reclassify any property_mailer rows to source",
        "UPDATE tracked_links SET kind = 'source' WHERE kind = 'property_mailer';",
    ),
    (
        "drop ck_tracked_links_kind",
        "ALTER TABLE tracked_links DROP CONSTRAINT IF EXISTS ck_tracked_links_kind;",
    ),
    (
        "recreate ck_tracked_links_kind without property_mailer",
        "ALTER TABLE tracked_links ADD CONSTRAINT ck_tracked_links_kind "
        "CHECK (kind IN ('partner', 'campaign', 'source'));",
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"Applying: {label}")
            session.execute(text(sql))
        session.commit()

        remaining = session.execute(
            text("SELECT count(*) FROM tracked_links WHERE kind = 'property_mailer'")
        ).scalar()

    print("\nVerification:")
    print(f"  remaining property_mailer rows: {remaining}")


if __name__ == "__main__":
    main()
