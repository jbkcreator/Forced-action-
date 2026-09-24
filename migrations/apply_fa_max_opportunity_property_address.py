"""migrations/apply_fa_max_opportunity_property_address.py

WP-T2-6 review fix: the log-submission Slack modal's new-borrower view
collects a property address (slack_post.py's new_property_address_block)
that _handle_log_submission_view_submit never persisted anywhere -- it was
accepted by Slack and silently discarded. Adds a free-text column to hold
it (not a FK into properties(id); see FaMaxOpportunityProperty for the
matched-property link table -- this is just what the human typed).

Idempotent: safe to re-run (ADD COLUMN IF NOT EXISTS).
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "ADD property_address to fa_max_opportunities",
        "ALTER TABLE fa_max_opportunities ADD COLUMN IF NOT EXISTS property_address VARCHAR(500);",
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"Applying: {label}")
            session.execute(text(sql))
        session.commit()
    print("fa_max_opportunities.property_address applied")


if __name__ == "__main__":
    main()
