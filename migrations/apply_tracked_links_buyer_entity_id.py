"""FA Max WP-7 follow-up — bind a tracked link to a known repeat borrower.

Adds tracked_links.buyer_entity_id: a nullable FK to buyer_entities, set by
the /tracked-link Slack command when Josh supplies a name that resolves to
exactly one BuyerEntity (see src/services/tracked_links.py:
find_buyer_entity_by_name). Deliberately independent of property_id — a
property's current owner-of-record is not the same as the borrower buying it
next, so this is never derived from property_id.

Idempotent, safe to re-run.

Usage: PYTHONPATH=. python migrations/apply_tracked_links_buyer_entity_id.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "add tracked_links.buyer_entity_id",
        "ALTER TABLE tracked_links ADD COLUMN IF NOT EXISTS buyer_entity_id "
        "INTEGER REFERENCES buyer_entities(id);",
    ),
    (
        "index tracked_links.buyer_entity_id",
        "CREATE INDEX IF NOT EXISTS idx_tracked_links_buyer_entity "
        "ON tracked_links (buyer_entity_id) WHERE buyer_entity_id IS NOT NULL;",
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"Applying: {label}")
            session.execute(text(sql))
        session.commit()

        col_exists = session.execute(
            text(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_name = 'tracked_links' AND column_name = 'buyer_entity_id'"
            )
        ).scalar()

    print("\nVerification:")
    print(f"  tracked_links.buyer_entity_id present: {bool(col_exists)}")


if __name__ == "__main__":
    main()
