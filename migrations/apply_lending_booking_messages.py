"""WP-GL-10: booking confirmation / reminder schedule and confirmation-call tasks.

``lending.booking_messages`` holds one row per (booking, message kind): the confirmation, the
night-before reminder and the 90-minute reminder. ``UNIQUE (booking_ref, kind)`` makes scheduling
idempotent. A row moves pending -> sending -> sent | send_unknown | failed, or pending -> skipped |
cancelled; a text is never re-sent from sending / send_unknown.

``lending.confirmation_tasks`` holds the human confirmation call (one per booking).
``lending.reply_handoffs`` records each reply-agent Slack post once per GHL message id.

Idempotent; safe to re-run. Usage:
    PYTHONPATH=. python migrations/apply_lending_booking_messages.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_reminders import ALL_KINDS, ALL_STATUSES, CHANNEL_EMAIL, CHANNEL_TEXT
from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    messages = f'"{schema}".booking_messages'
    tasks = f'"{schema}".confirmation_tasks'
    handoffs = f'"{schema}".reply_handoffs'
    for ddl in (
        f"""CREATE TABLE IF NOT EXISTS {messages} (
            id                  bigserial PRIMARY KEY,
            booking_ref         text         NOT NULL,
            provider_event_id   text,
            person_id           text,
            kind                varchar(20)  NOT NULL CHECK (kind IN ({_in(ALL_KINDS)})),
            send_at             timestamptz  NOT NULL,
            status              varchar(24)  NOT NULL DEFAULT 'pending'
                                CHECK (status IN ({_in(ALL_STATUSES)})),
            skip_reason         varchar(60),
            cancel_reason       varchar(60),
            channel             varchar(10)  CHECK (channel IN ('{CHANNEL_TEXT}', '{CHANNEL_EMAIL}')),
            attempts            integer      NOT NULL DEFAULT 0,
            first_name          varchar(60),
            contact_phone       varchar(20),
            contact_email       varchar(255),
            property_address    text,
            slot_start_utc      timestamptz  NOT NULL,
            booked_by           varchar(120),
            provider_message_id varchar(100),
            created_at          timestamptz  NOT NULL DEFAULT now(),
            decided_at          timestamptz,
            sent_at             timestamptz,
            CONSTRAINT uq_lending_booking_messages_ref_kind UNIQUE (booking_ref, kind)
        )""",
        # an already-migrated table has the old kind CHECK: widen it to include gate_fail
        f"ALTER TABLE {messages} DROP CONSTRAINT IF EXISTS booking_messages_kind_check",
        f"ALTER TABLE {messages} ADD CONSTRAINT booking_messages_kind_check CHECK (kind IN ({_in(ALL_KINDS)}))",
        f"CREATE INDEX IF NOT EXISTS ix_lending_booking_messages_due ON {messages} (send_at) WHERE status = 'pending'",
        f"CREATE INDEX IF NOT EXISTS ix_lending_booking_messages_stale ON {messages} (decided_at) WHERE status = 'sending'",
        f"CREATE INDEX IF NOT EXISTS ix_lending_booking_messages_event ON {messages} (provider_event_id) "
        f"WHERE provider_event_id IS NOT NULL",
        f"""CREATE TABLE IF NOT EXISTS {tasks} (
            id           bigserial PRIMARY KEY,
            booking_ref  text         NOT NULL UNIQUE,
            person_id    text,
            assignee     varchar(255) NOT NULL,
            due_date     date         NOT NULL,
            created_at   timestamptz  NOT NULL DEFAULT now(),
            completed_at timestamptz,
            cancelled_at timestamptz
        )""",
        f"ALTER TABLE {tasks} ADD COLUMN IF NOT EXISTS cancelled_at timestamptz",
        f"CREATE INDEX IF NOT EXISTS ix_lending_confirmation_tasks_open ON {tasks} (due_date, assignee) "
        f"WHERE completed_at IS NULL",
        f"""CREATE TABLE IF NOT EXISTS {handoffs} (
            id          bigserial PRIMARY KEY,
            message_id  text        NOT NULL UNIQUE,
            kind        varchar(30) NOT NULL,
            contact_id  text,
            phone_hash  varchar(12),
            created_at  timestamptz NOT NULL DEFAULT now(),
            posted_at   timestamptz,
            responded_at timestamptz,
            overdue_alerted_at timestamptz
        )""",
        f"ALTER TABLE {handoffs} ADD COLUMN IF NOT EXISTS responded_at timestamptz",
        f"ALTER TABLE {handoffs} ADD COLUMN IF NOT EXISTS overdue_alerted_at timestamptz",
        f"CREATE INDEX IF NOT EXISTS ix_lending_reply_handoffs_open ON {handoffs} (posted_at) "
        f"WHERE posted_at IS NOT NULL AND responded_at IS NULL AND overdue_alerted_at IS NULL",
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_booking_messages complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
