"""
Backfill: re-classify 36,009 Pinellas legal_and_liens rows that landed with
document_type='LIEN' (generic fallback) because the Pinellas column mapping
used the canonical name 'document_type' for the raw doc-type column while
_sub_categorise_liens expected 'DocType' — causing the classifier to always
see an empty string and fall back to "LIEN".

The forward-going fix (apply the fix to _sub_categorise_liens) is already
deployed. This script updates the existing misclassified rows to match what
the fixed pipeline would have produced.

Classification mirrors label_row() in lien_engine.py exactly, same priority:
  1. medical filer keywords in creditor      → MEDICAL LIEN
  2. insurance grantee keywords in debtor    → MEDICAL LIEN
  3. HOA keywords in creditor OR debtor      → HOA LIENS (HL)
  4. IRS/tax keywords in creditor OR debtor  → TAX LIEN
  5. Pinellas code filer in creditor         → CODE LIEN
  6. everything else                         → MECHANICS LIENS (ML)

Limitation: the original raw portal doc-type string (which might have been
"TAX LIEN" or "LIS PENDENS" for some rows) is gone from the DB — those rows
will be classified via keyword matching only (IRS keywords still catch most
tax liens; LIS PENDENS rows without a distinctive keyword will land in ML).

Usage:
    # Preview projected label breakdown (no writes):
    PYTHONPATH=. python migrations/apply_pinellas_lien_doc_type_backfill.py

    # Apply:
    PYTHONPATH=. python migrations/apply_pinellas_lien_doc_type_backfill.py --apply
"""
import argparse
import logging
import os
import sys
from collections import Counter
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

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("pinellas_lien_backfill")

# ---------------------------------------------------------------------------
# Keyword sets — must stay in sync with lien_engine.py constants.
# ---------------------------------------------------------------------------
_MEDICAL_FILER = [
    "HEALTH ADVENT", "ADVENT HEALTH", "ADVENTHEALTH",
    "HCA FLORIDA", "BAYCARE", "TAMPA GENERAL",
    "FLORIDA HOSPITAL", "JOHNS HOPKINS ALL CHILDRENS",
]
_INSURANCE_GRANTEE = [
    "GEICO", "PROGRESSIVE", "ALLSTATE", "STATE FARM", "FARM STATE",
    "DIRECT GENERAL", "USAA", "NATIONWIDE", "TRAVELERS", "LIBERTY MUTUAL",
]
_HOA = [
    "ASSOCIATION", "HOA", "CONDO", "COMMUNITY",
    "VILLAGE", "TOWNHOME", "PROPERTY OWNERS",
]
_IRS = [
    "UNITED STATES", "INTERNAL REVENUE",
    "STATE OF FLORIDA", "DEPARTMENT OF REVENUE",
    "FLORIDA DEPARTMENT OF REVENUE", "FLORIDA STATE REVENUE", "FL DEPT OF REVENUE",
]
# Pinellas city/county code-lien filers (county_cfg["city_filer_keywords"])
_CODE_FILER = [
    "PINELLAS COUNTY", "CITY OF ST. PETERSBURG", "CITY OF CLEARWATER",
    "CITY OF LARGO", "CITY OF PINELLAS PARK",
]


def _like_any(column: str, keywords: list[str]) -> str:
    """Render: UPPER(column) LIKE ANY(ARRAY[...])"""
    literals = ", ".join(f"'%{kw}%'" for kw in keywords)
    return f"UPPER({column}) LIKE ANY(ARRAY[{literals}])"


def _build_case_expr() -> str:
    """
    Build the CASE WHEN expression that mirrors label_row() priority order.
    All matching is case-insensitive via UPPER().
    """
    return f"""
        CASE
            WHEN {_like_any("creditor", _MEDICAL_FILER)}
                THEN 'MEDICAL LIEN'
            WHEN {_like_any("debtor", _INSURANCE_GRANTEE)}
                THEN 'MEDICAL LIEN'
            WHEN {_like_any("creditor", _HOA)}
              OR  {_like_any("debtor",  _HOA)}
                THEN 'HOA LIENS (HL)'
            WHEN {_like_any("creditor", _IRS)}
              OR  {_like_any("debtor",  _IRS)}
                THEN 'TAX LIEN'
            WHEN {_like_any("creditor", _CODE_FILER)}
                THEN 'CODE LIEN'
            ELSE 'MECHANICS LIENS (ML)'
        END
    """.strip()


_WHERE = "county_id = 'pinellas' AND document_type = 'LIEN'"

_DRY_RUN_SQL = text(f"""
    SELECT
        {_build_case_expr()} AS new_label,
        COUNT(*) AS cnt
    FROM legal_and_liens
    WHERE {_WHERE}
    GROUP BY new_label
    ORDER BY cnt DESC
""")

# UPDATE uses a CTE to compute new labels in one scan, then joins back.
# RETURNING lets us aggregate the actual outcome without a second query.
_APPLY_SQL = text(f"""
    WITH labelled AS (
        SELECT
            id,
            {_build_case_expr()} AS new_label
        FROM legal_and_liens
        WHERE {_WHERE}
    )
    UPDATE legal_and_liens l
    SET
        document_type = labelled.new_label,
        meta_data = COALESCE(l.meta_data, '{{}}' ::jsonb)
                    || jsonb_build_object(
                        'backfill_source', 'apply_pinellas_lien_doc_type_backfill',
                        'backfill_at',     NOW()
                    )
    FROM labelled
    WHERE l.id = labelled.id
    RETURNING l.document_type AS new_label
""")


def _print_breakdown(label: str, counts: Counter, total: int) -> None:
    print(f"\n{label} — {total:,} rows")
    print(f"  {'Label':<30}  {'Count':>7}  {'%':>6}")
    print("  " + "-" * 45)
    for lbl, n in counts.most_common():
        print(f"  {lbl:<30}  {n:>7,}  {100*n/total:>5.1f}%")


def main(apply: bool) -> None:
    with get_db_context() as db:
        if not apply:
            rows = db.execute(_DRY_RUN_SQL).fetchall()
            if not rows:
                logger.info("No rows matching county_id='pinellas' AND document_type='LIEN' — nothing to do.")
                return
            counts = Counter({r.new_label: r.cnt for r in rows})
            _print_breakdown("DRY RUN — projected label breakdown", counts, sum(counts.values()))
            print("\nRun with --apply to write changes.")
            return

        # Apply
        result = db.execute(_APPLY_SQL)
        updated_rows = result.fetchall()
        db.commit()

        counts = Counter(r.new_label for r in updated_rows)
        total  = sum(counts.values())
        _print_breakdown("APPLIED — rows updated", counts, total)
        logger.info("Backfill complete: %d rows updated.", total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Pinellas lien document_type classification.")
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run).")
    args = parser.parse_args()
    main(apply=args.apply)
