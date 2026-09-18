"""FA Max WP-7 — tracked links + click log for the self-serve pre-fill path.

Idempotent, safe to re-run. See tasks/FA_Max_build/dev2_wp7_selfserve_prefill_plan.md
WI-1. Run after WP-1 (fa_max_persons must exist first — not referenced here,
but the identity resolver in WI-3 depends on it).

Usage: PYTHONPATH=. python migrations/apply_selfserve_tracked_links.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE tracked_links",
        """
        CREATE TABLE IF NOT EXISTS tracked_links (
            id            BIGSERIAL PRIMARY KEY,
            slug          TEXT NOT NULL UNIQUE,
            kind          TEXT NOT NULL,
            label         TEXT NOT NULL,
            partner_ref   TEXT,
            campaign_ref  TEXT,
            property_id   INTEGER REFERENCES properties(id),
            buyer_entity_id INTEGER REFERENCES buyer_entities(id),
            destination   TEXT,
            is_active     BOOLEAN NOT NULL DEFAULT true,
            created_by    TEXT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_tracked_links_kind
                CHECK (kind IN ('partner', 'campaign', 'source'))
        );
        """,
    ),
    (
        "CREATE tracked_link_clicks",
        """
        CREATE TABLE IF NOT EXISTS tracked_link_clicks (
            id              BIGSERIAL PRIMARY KEY,
            tracked_link_id BIGINT NOT NULL REFERENCES tracked_links(id),
            clicked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            ip_hash         TEXT,
            user_agent      TEXT,
            referer         TEXT,
            session_token   TEXT NOT NULL
        );
        """,
    ),
    (
        "INDEX tracked_link_clicks link+time",
        """
        CREATE INDEX IF NOT EXISTS idx_link_clicks_link
            ON tracked_link_clicks (tracked_link_id, clicked_at DESC);
        """,
    ),
    (
        "INDEX tracked_link_clicks session",
        """
        CREATE INDEX IF NOT EXISTS idx_link_clicks_session
            ON tracked_link_clicks (session_token);
        """,
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"Applying: {label}")
            session.execute(text(sql))
        session.commit()

        links_exists = session.execute(
            text("SELECT to_regclass('tracked_links') IS NOT NULL")
        ).scalar()
        clicks_exists = session.execute(
            text("SELECT to_regclass('tracked_link_clicks') IS NOT NULL")
        ).scalar()

    print("\nVerification:")
    print(f"  tracked_links present:        {bool(links_exists)}")
    print(f"  tracked_link_clicks present:  {bool(clicks_exists)}")


if __name__ == "__main__":
    main()
