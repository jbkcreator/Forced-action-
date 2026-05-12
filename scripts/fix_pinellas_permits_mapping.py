"""
One-off: clear the bogus cached playwright_code for Pinellas permits and
reject the two pending column-mappings that were synthesized from its
garbage column names.

After running this, the next permit scrape will regenerate code using the
updated system prompt that handles Accela's tr.ACA_GridHeader header row,
producing properly-named columns and a clean ColumnMapper lookup against
the existing approved mapping (id=5).
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

if "DATABASE_URL" not in os.environ:
    env_path = PROJECT_ROOT / ".env"
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DATABASE_URL="):
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip()
            break

import sqlalchemy as sa
from src.core.database import Database


FEEDBACK = (
    "Headers extracted as integers (0,1,2,...,10) instead of canonical "
    "column names. Root cause: generated run_scrape queried th elements "
    "which do not exist in Accela's grid (header row is td inside "
    "tr.ACA_GridHeader). Cleared the cached playwright_code; next "
    "regeneration uses the updated system prompt that handles "
    "ACA_GridHeader extraction."
)


def main() -> int:
    db = Database()
    with db.session_scope() as session:
        src_row = session.execute(
            sa.text(
                "SELECT id FROM county_sources "
                "WHERE county_id='pinellas' AND signal_type='permits'"
            )
        ).first()
        if src_row:
            session.execute(
                sa.text(
                    "UPDATE county_sources "
                    "SET playwright_code = NULL, "
                    "    playwright_code_version = NULL, "
                    "    playwright_code_approved = false "
                    "WHERE id = :id"
                ),
                {"id": src_row.id},
            )
            print(f"Cleared playwright_code on source_id={src_row.id}")

        bad = session.execute(
            sa.text(
                "SELECT m.id FROM county_column_mappings m "
                "JOIN county_sources s ON s.id = m.source_id "
                "WHERE s.county_id='pinellas' AND s.signal_type='permits' "
                "  AND m.is_approved = false "
                "  AND m.reject_feedback IS NULL"
            )
        ).all()
        for r in bad:
            session.execute(
                sa.text(
                    "UPDATE county_column_mappings "
                    "SET reject_feedback = :fb "
                    "WHERE id = :id"
                ),
                {"fb": FEEDBACK, "id": r.id},
            )
            print(f"Rejected pending mapping id={r.id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
