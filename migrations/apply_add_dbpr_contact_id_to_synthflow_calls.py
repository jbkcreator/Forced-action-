"""
Add dbpr_contact_id FK column to synthflow_calls (idempotent).

Links a Synthflow call row to the DBPR contractor record when the called
phone matches a dbpr_contacts entry, so DBPR contractor calls are traceable.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database import get_db_context
from sqlalchemy import text


def apply():
    with get_db_context() as db:
        db.execute(text("""
            ALTER TABLE synthflow_calls
            ADD COLUMN IF NOT EXISTS dbpr_contact_id INTEGER
                REFERENCES dbpr_contacts(id);
        """))
        db.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_synthflow_calls_dbpr_contact_id
            ON synthflow_calls (dbpr_contact_id);
        """))
        print("apply_add_dbpr_contact_id_to_synthflow_calls: done")


if __name__ == "__main__":
    apply()
