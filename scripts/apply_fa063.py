"""
Apply fa063_campaign_instantly_settings DDL via direct DB connection.
Usage: python scripts/apply_fa063.py [--dry-run]
"""
import sys
import os
import importlib.util
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.core.database import get_db_context


def _load_upgrade():
    path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "alembic", "versions", "fa063_campaign_instantly_settings.py",
    )
    spec = importlib.util.spec_from_file_location("fa063_campaign_instantly_settings", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.upgrade


def main():
    upgrade = _load_upgrade()
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("[apply_fa063] DRY RUN — no changes applied")
        return

    print("[apply_fa063] Adding email_campaigns.instantly_settings ...")
    with get_db_context() as db:
        upgrade(db.connection())
        db.commit()
        print("[apply_fa063] Done.")


if __name__ == "__main__":
    main()
