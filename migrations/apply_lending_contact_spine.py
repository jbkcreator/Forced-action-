"""R1 1.1 Unified Contact Spine tables (docs/lending/contact-spine.md).

``lending.contact_spine`` (one row per person), ``contact_identifiers`` (phones, emails, LLCs, GHL and BatchDialer
ids; a merge-key value belongs to one contact), ``contact_sources`` (which contact each source row resolved to) and
the append-only ``contact_spine_merges`` audit. ``lending.contacts`` (the per-phone compliance record) is not touched.

Idempotent; creates empty tables. The sync (src.tasks.lending_contact_spine_sync) fills them.

Usage:
    PYTHONPATH=. python migrations/apply_lending_contact_spine.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    s = f'"{schema}"'
    statements = [
        f"""CREATE TABLE IF NOT EXISTS {s}.contact_spine (
            id           bigserial    PRIMARY KEY,
            seed_key     varchar(120) NOT NULL UNIQUE,
            display_name varchar(200),
            merged_into  bigint       REFERENCES {s}.contact_spine (id),
            created_at   timestamptz  NOT NULL DEFAULT now(),
            updated_at   timestamptz  NOT NULL DEFAULT now())""",
        f"""CREATE TABLE IF NOT EXISTS {s}.contact_identifiers (
            id            bigserial    PRIMARY KEY,
            contact_id    bigint       NOT NULL REFERENCES {s}.contact_spine (id),
            kind          varchar(30)  NOT NULL CONSTRAINT ck_lending_contact_identifiers_kind
                          CHECK (kind IN ('phone', 'email', 'entity', 'ghl_contact', 'batchdialer_contact')),
            value         varchar(320) NOT NULL,
            merge_key     boolean      NOT NULL,
            first_seen_at timestamptz  NOT NULL DEFAULT now(),
            last_seen_at  timestamptz  NOT NULL DEFAULT now(),
            CONSTRAINT uq_lending_contact_identifiers_kind_value_contact UNIQUE (kind, value, contact_id))""",
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_contact_identifiers_merge_key "
        f"ON {s}.contact_identifiers (kind, value) WHERE merge_key",
        f"CREATE INDEX IF NOT EXISTS ix_lending_contact_identifiers_contact ON {s}.contact_identifiers (contact_id)",
        f"""CREATE TABLE IF NOT EXISTS {s}.contact_sources (
            source     varchar(40) NOT NULL,
            source_ref varchar(100) NOT NULL,
            contact_id bigint      NOT NULL REFERENCES {s}.contact_spine (id),
            seen_at    timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (source, source_ref))""",
        f"CREATE INDEX IF NOT EXISTS ix_lending_contact_sources_contact ON {s}.contact_sources (contact_id)",
        f"""CREATE TABLE IF NOT EXISTS {s}.contact_spine_merges (
            id          bigserial   PRIMARY KEY,
            survivor_id bigint      NOT NULL,
            merged_id   bigint      NOT NULL,
            merged_at   timestamptz NOT NULL DEFAULT now())""",
    ]
    for statement in statements:
        conn.execute(text(statement))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_contact_spine complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
