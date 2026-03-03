"""add_trgm_indexes_for_matching

Revision ID: b1c2d3e4f5a6
Revises: a1b2c3d4e5f6
Create Date: 2026-03-03

Adds GIN trigram indexes on:
  - owners.owner_name              → fast fuzzy name search across all 522k owners
  - properties.legal_description   → fast legal-description substring matching
  - properties.address             → fast fuzzy address search (violations/permits/evictions)

PREREQUISITE (run once as postgres superuser before applying this migration):
    psql -U postgres -d <your_db> -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

The app user (distress_user) cannot create extensions. Once the superuser
installs pg_trgm, this migration creates the GIN indexes under the app user.
If pg_trgm is not yet installed, all index creations are skipped and a
warning is logged — the indexes can be created later by re-running this
migration after the extension is installed.
"""
import logging
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

logger = logging.getLogger(__name__)

revision: str = 'b1c2d3e4f5a6'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _trgm_installed(conn) -> bool:
    """Return True if pg_trgm extension is available in this database."""
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
    ).fetchone()
    return result is not None


def upgrade() -> None:
    conn = op.get_bind()

    if not _trgm_installed(conn):
        logger.warning(
            "\n"
            "  pg_trgm extension is not installed — GIN trigram indexes skipped.\n"
            "  To enable full-table fuzzy owner name matching, ask a superuser to run:\n"
            "      psql -U postgres -d <your_db> -c \"CREATE EXTENSION IF NOT EXISTS pg_trgm;\"\n"
            "  Then re-run:  alembic upgrade b1c2d3e4f5a6\n"
            "  (stamp it back first: alembic stamp a1b2c3d4e5f6)\n"
        )
        return

    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_owner_name_trgm "
        "ON owners USING gin(owner_name gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_property_legal_desc_trgm "
        "ON properties USING gin(legal_description gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_property_address_trgm "
        "ON properties USING gin(address gin_trgm_ops)"
    )
    logger.info("pg_trgm GIN indexes created on owners.owner_name, properties.legal_description, properties.address")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_property_address_trgm")
    op.execute("DROP INDEX IF EXISTS idx_property_legal_desc_trgm")
    op.execute("DROP INDEX IF EXISTS idx_owner_name_trgm")
