"""
Read-only diagnostic: show what column name each county's approved lien
CountyColumnMapping uses for the document/instrument type field, and what
row_routing.column points to.

Tells us why Pinellas rows land in legal_and_liens with document_type='LIEN'.

Usage:
    PYTHONPATH=. python scripts/diagnose_lien_doc_type_mapping.py
"""
import json
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

from sqlalchemy import text
from src.core.database import get_db_context

DOC_TYPE_CANDIDATES = {"DocType", "document_type", "Instrument", "Type", "Doc Type", "Instrument Type"}

def main() -> None:
    with get_db_context() as db:
        rows = db.execute(text(
            """
            SELECT
                s.county_id,
                s.signal_type,
                s.id          AS source_id,
                m.id          AS mapping_id,
                m.is_approved,
                m.source_columns,
                m.mapping,
                m.row_routing
            FROM county_sources s
            JOIN county_column_mappings m ON m.source_id = s.id
            WHERE s.signal_type = 'liens'
            ORDER BY s.county_id, m.is_approved DESC, m.created_at DESC
            """
        )).fetchall()

    if not rows:
        print("No county_column_mappings rows found for signal_type='liens'.")
        return

    for r in rows:
        mapping   = r.mapping   or {}
        routing   = r.row_routing or {}
        src_cols  = r.source_columns or []

        # Which raw column(s) map to a doc-type-like canonical name?
        doc_type_mappings = {
            raw: canonical
            for raw, canonical in mapping.items()
            if canonical in DOC_TYPE_CANDIDATES or raw in DOC_TYPE_CANDIDATES
        }

        routing_col = routing.get("column") if isinstance(routing, dict) else None

        print(f"\n{'='*60}")
        print(f"county={r.county_id}  source_id={r.source_id}  mapping_id={r.mapping_id}  approved={r.is_approved}")
        print(f"  raw source columns : {src_cols}")
        print(f"  doc-type mappings  : {doc_type_mappings or '(none found)'}")
        print(f"  row_routing.column : {routing_col!r}")
        if isinstance(routing, dict) and routing.get("rules"):
            print(f"  routing rules      : {json.dumps(routing['rules'], indent=4)}")
        print(f"  *** _sub_categorise_liens reads 'DocType' — above column must equal 'DocType' to sub-label correctly ***")


if __name__ == "__main__":
    main()
