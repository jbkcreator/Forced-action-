"""Create the deal_rooms table for the 3M deal-room hold-deposit flow.

Each row represents one deal-room session initiated by a prospect depositing
a refundable hold. The table is audit-only for the snapshot; no service logic
lives here.

Columns:
  id                   serial PK
  token                UUID, unique, not null — public-facing opaque handle
  prospect_name        varchar
  prospect_email       varchar
  zip_code             varchar(10)
  tier                 varchar — subscriber tier the prospect is evaluating
  job_value            numeric — estimated job value shown in the room
  close_rate           numeric — estimated close rate shown in the room
  properties_snapshot  JSONB not null — point-in-time lead sample (audit only)
  held_at              timestamptz nullable — when the hold deposit was taken
  expires_at           timestamptz nullable — when the hold lapses if unconverted
  converted_at         timestamptz nullable — when the prospect became a subscriber
  refund_status        varchar nullable — null | 'refunded' | 'refund_failed'
  created_at           timestamptz default now

Idempotent — IF NOT EXISTS / ADD COLUMN IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_deal_rooms.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS deal_rooms (
        id               SERIAL PRIMARY KEY,
        token            UUID NOT NULL,
        prospect_name    VARCHAR(255),
        prospect_email   VARCHAR(255),
        zip_code         VARCHAR(10),
        tier             VARCHAR(50),
        job_value        NUMERIC,
        close_rate       NUMERIC,
        properties_snapshot JSONB NOT NULL,
        held_at          TIMESTAMPTZ,
        expires_at       TIMESTAMPTZ,
        converted_at     TIMESTAMPTZ,
        refund_status    VARCHAR(20) DEFAULT NULL,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_rooms_token ON deal_rooms (token);",
    "CREATE INDEX IF NOT EXISTS idx_deal_rooms_zip_code ON deal_rooms (zip_code);",
    "CREATE INDEX IF NOT EXISTS idx_deal_rooms_expires_at ON deal_rooms (expires_at);",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'check_deal_room_refund_status'
              AND conrelid = 'deal_rooms'::regclass
        ) THEN
            ALTER TABLE deal_rooms
                ADD CONSTRAINT check_deal_room_refund_status
                CHECK (refund_status IS NULL
                    OR refund_status IN ('refunded', 'refund_failed'));
        END IF;
    END $$;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("apply_deal_rooms complete.")


if __name__ == "__main__":
    main()
