"""
Apply fa062_email_campaigns DDL via direct DB connection.
Usage: python scripts/apply_fa062.py [--dry-run]
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.core.database import get_db_context
from alembic.versions.fa062_email_campaigns import upgrade, downgrade


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("[apply_fa062] DRY RUN — no changes applied")
        return

    print("[apply_fa062] Applying fa062_email_campaigns schema...")
    with get_db_context() as db:
        upgrade(db.connection())
        db.commit()
        print("[apply_fa062] Done.")


if __name__ == "__main__":
    main()
