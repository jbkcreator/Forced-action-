"""strip_zip_plus4_from_properties

1,255 rows in `properties` have a ZIP+4 suffix (e.g. "33601-1234") instead of the
plain 5-digit ZIP.  The master loader already normalises incoming data, but existing
rows loaded before that fix retain the long form.

This migration strips the 4-digit extension so all ZIP values are exactly 5 digits,
matching the format expected by ZIP-territory queries and the ZipChecker endpoint.

Revision ID: n4o5p6q7r8s9
Revises:     l2m3n4o5p6q7
Create Date: 2026-04-20
"""

from alembic import op

revision = 'n4o5p6q7r8s9'
down_revision = 'm3n4o5p6q7r8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        UPDATE properties
        SET zip = LEFT(zip, 5)
        WHERE zip ~ '^\d{5}-\d{4}$'
        """
    )


def downgrade() -> None:
    # The stripped 4-digit suffixes cannot be recovered — downgrade is a no-op.
    pass
