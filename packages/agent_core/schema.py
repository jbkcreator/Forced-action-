"""PostgreSQL DDL for the agent_core tables. Idempotent; the host's migration scripts run it.

``base_statements`` creates the tables; ``safeguard_statements`` adds the send safeguards
(revision history, recipient and lead references, idempotency key, expiry, separate reviser,
widened status set). Each list is safe to re-run, and :func:`apply_to` runs both in order.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

ACTION_STATUSES = ("pending", "revising", "approved", "rejected", "sending", "sent", "failed",
                   "blocked", "expired")


def _status_check() -> str:
    statuses = ", ".join(f"'{status}'" for status in ACTION_STATUSES)
    return f"CHECK (status IN ({statuses}))"


def base_statements(schema: str) -> list[str]:
    return [
        f'CREATE SCHEMA IF NOT EXISTS "{schema}"',
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}".agent_halt_state (
            id smallint PRIMARY KEY,
            halted boolean NOT NULL DEFAULT false,
            reason text NOT NULL DEFAULT '',
            set_by varchar(32),
            set_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_agent_halt_state_single_row CHECK (id = 1)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}".pending_actions (
            action_id bigserial PRIMARY KEY,
            tool_name varchar(100) NOT NULL,
            channel varchar(64) NOT NULL,
            payload jsonb NOT NULL,
            summary text NOT NULL DEFAULT '',
            status varchar(16) NOT NULL DEFAULT 'pending',
            requested_by varchar(32),
            source_channel varchar(32),
            source_thread_ts varchar(64),
            card_channel varchar(32),
            card_ts varchar(64),
            decided_by varchar(32),
            decided_at timestamptz,
            revision_note text,
            executed_at timestamptz,
            provider_ref varchar(200),
            error text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_pending_actions_status {_status_check()}
        )
        """,
        f'CREATE INDEX IF NOT EXISTS ix_pending_actions_status ON "{schema}".pending_actions (status, decided_at)',
    ]


def safeguard_statements(schema: str) -> list[str]:
    table = f'"{schema}".pending_actions'
    added_columns = (
        "revisions jsonb NOT NULL DEFAULT '[]'::jsonb",
        "recipient_phone varchar(20)",
        "recipient_email varchar(255)",
        "contact_ref varchar(64)",
        "deal_ref varchar(64)",
        "idempotency_key varchar(128)",
        "expires_at timestamptz",
        "revised_by varchar(32)",
    )
    return [
        *(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column}" for column in added_columns),
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS ck_pending_actions_status",
        f"ALTER TABLE {table} ADD CONSTRAINT ck_pending_actions_status {_status_check()}",
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_actions_idempotency_key ON {table} (idempotency_key)",
        # The open-revision lookup moved from decided_by to revised_by.
        f'DROP INDEX IF EXISTS "{schema}".ix_pending_actions_revising_user',
        f"CREATE INDEX IF NOT EXISTS ix_pending_actions_revised_by ON {table} (revised_by) WHERE status = 'revising'",
        f"CREATE INDEX IF NOT EXISTS ix_pending_actions_expires_at ON {table} (expires_at) "
        "WHERE status IN ('pending', 'revising')",
    ]


def ddl_statements(schema: str) -> list[str]:
    return [*base_statements(schema), *safeguard_statements(schema)]


def apply_base(conn: Connection, schema: str) -> None:
    for statement in base_statements(schema):
        conn.execute(text(statement))


def apply_safeguards(conn: Connection, schema: str) -> None:
    for statement in safeguard_statements(schema):
        conn.execute(text(statement))


def apply_to(conn: Connection, schema: str) -> None:
    apply_base(conn, schema)
    apply_safeguards(conn, schema)
