"""tests/migrations/test_apply_fa_max_persons_contact_fields.py"""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="requires a live Postgres DATABASE_URL",
)


def test_migration_is_idempotent_and_adds_contact_columns():
    from migrations.apply_fa_max_persons_contact_fields import apply

    engine = create_engine(os.environ["DATABASE_URL"])
    apply(engine=engine)
    apply(engine=engine)  # must not raise on second run

    with engine.connect() as conn:
        columns = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'fa_max_persons' "
                    "AND column_name IN ('full_name', 'email', 'phone')"
                )
            )
        }
    assert columns == {"full_name", "email", "phone"}
