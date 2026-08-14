"""Create demo_users — login for the /demo/deal-room generator.

Idempotent. Stores email + bcrypt password_hash (never plaintext).

    PYTHONPATH=. python migrations/apply_demo_users.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context


def main() -> None:
    with get_db_context() as db:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS demo_users (
                id            SERIAL PRIMARY KEY,
                email         VARCHAR(255) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                is_active     BOOLEAN NOT NULL DEFAULT TRUE,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_demo_users_email ON demo_users (email)"
        ))
        db.commit()
        print("demo_users ready.")


if __name__ == "__main__":
    main()
