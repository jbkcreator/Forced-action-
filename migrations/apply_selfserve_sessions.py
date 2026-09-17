"""FA Max WP-7 WI-3 — selfserve_sessions.

Idempotent, safe to re-run. Depends on tracked_links (apply_selfserve_tracked_links.py),
buyer_entities (WP-3/WP-4) and fa_max_persons (WP-1) already existing.

Usage: PYTHONPATH=. python migrations/apply_selfserve_sessions.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE selfserve_sessions",
        """
        CREATE TABLE IF NOT EXISTS selfserve_sessions (
            id                BIGSERIAL PRIMARY KEY,
            token             UUID NOT NULL UNIQUE,
            tracked_link_id   BIGINT REFERENCES tracked_links(id),
            property_id       INTEGER REFERENCES properties(id),
            buyer_entity_id   INTEGER REFERENCES buyer_entities(id),
            person_id         UUID REFERENCES fa_max_persons(person_id),
            prefill_snapshot  JSONB NOT NULL,
            corrections       JSONB,
            confirmations     JSONB,
            contact           JSONB,
            status            TEXT NOT NULL DEFAULT 'started',
            handoff_ref       TEXT,
            handed_off_at     TIMESTAMPTZ,
            started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_activity_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_selfserve_sessions_status
                CHECK (status IN ('started', 'prefilled', 'confirmed', 'handed_off', 'abandoned'))
        );
        """,
    ),
    (
        "INDEX selfserve_sessions status",
        """
        CREATE INDEX IF NOT EXISTS idx_selfserve_status
            ON selfserve_sessions (status, started_at DESC);
        """,
    ),
    (
        "INDEX selfserve_sessions person",
        """
        CREATE INDEX IF NOT EXISTS idx_selfserve_person
            ON selfserve_sessions (person_id);
        """,
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"Applying: {label}")
            session.execute(text(sql))
        session.commit()

        exists = session.execute(
            text("SELECT to_regclass('selfserve_sessions') IS NOT NULL")
        ).scalar()

    print("\nVerification:")
    print(f"  selfserve_sessions present: {bool(exists)}")


if __name__ == "__main__":
    main()
