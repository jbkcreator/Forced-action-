"""Backfill sms_opt_outs and sms_opt_ins phone columns to canonical E.164.

Processes rows in 1000-row batches:
  - Phones already matching +1XXXXXXXXXX are skipped.
  - Phones that normalize() can convert are UPDATEd.
  - Phones that normalize() returns None for (non-US, junk) are DELETEd.
  - Collision (normalized form already exists): DELETE the non-canonical row.

After this migration runs the V2 mitigation OR-clause in can_send() is
safe to remove.

Revision ID: fa018_phone_normalize_backfill
Revises: fa017_sms_send_logs
Create Date: 2026-05-14
"""

from __future__ import annotations

import re
import sys
import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "fa018_phone_normalize_backfill"
down_revision = "fa017_sms_send_logs"
branch_labels = None
depends_on = None

_E164_RE = re.compile(r"^\+1\d{10}$")
_BATCH = 1000


def _normalize_phone(raw: str) -> str | None:
    """Inline normalize — mirrors phone_utils.normalize() without importing app code."""
    try:
        import phonenumbers
        from phonenumbers import NumberParseException
        parsed = phonenumbers.parse(raw, "US")
        if not phonenumbers.is_valid_number(parsed):
            return None
        if parsed.country_code != 1:
            return None
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None


def _backfill_table(conn, table: str) -> dict:
    stats = {"updated": 0, "deleted_junk": 0, "deleted_collision": 0, "skipped": 0}
    offset = 0
    while True:
        rows = conn.execute(
            text(f"SELECT id, phone FROM {table} ORDER BY id LIMIT :lim OFFSET :off"),
            {"lim": _BATCH, "off": offset},
        ).fetchall()
        if not rows:
            break
        offset += _BATCH

        for row in rows:
            stored = row.phone or ""
            if _E164_RE.match(stored):
                stats["skipped"] += 1
                continue

            normed = _normalize_phone(stored)
            if normed is None:
                conn.execute(text(f"DELETE FROM {table} WHERE id = :id"), {"id": row.id})
                stats["deleted_junk"] += 1
                continue

            # Check for collision before updating
            existing = conn.execute(
                text(f"SELECT id FROM {table} WHERE phone = :p AND id != :id"),
                {"p": normed, "id": row.id},
            ).first()
            if existing:
                conn.execute(text(f"DELETE FROM {table} WHERE id = :id"), {"id": row.id})
                stats["deleted_collision"] += 1
            else:
                conn.execute(
                    text(f"UPDATE {table} SET phone = :p WHERE id = :id"),
                    {"p": normed, "id": row.id},
                )
                stats["updated"] += 1

    return stats


def upgrade() -> None:
    conn = op.get_bind()
    for table in ("sms_opt_outs", "sms_opt_ins"):
        stats = _backfill_table(conn, table)
        print(
            f"[fa018] {table}: "
            f"skipped={stats['skipped']} "
            f"updated={stats['updated']} "
            f"deleted_junk={stats['deleted_junk']} "
            f"deleted_collision={stats['deleted_collision']}"
        )


def downgrade() -> None:
    # Data loss is intentional (deleted rows were non-canonical / junk).
    # Downgrade is a no-op.
    pass
