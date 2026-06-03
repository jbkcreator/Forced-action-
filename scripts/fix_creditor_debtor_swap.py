"""One-time data fix: swap creditor/debtor on Judgment + Mechanics Lien rows.

The liens loader's write-time assignment rule (c) had creditor/debtor swapped
for Judgments and Mechanics Liens (creditor=Grantee, debtor=Grantor), while the
correct party-role model — verified against recorded judgment PDFs 2026-06-03
(e.g. Pinellas instrument 2026148246: State of Florida = creditor, defendant =
debtor) — is Grantor=creditor, Grantee=debtor. ~92% of judgment rows had an
institutional name (state/bank/LLC) in the debtor column as a result.

The loader was fixed in src/loaders/liens.py (feature/ocr-v2-pinellas-fixes);
this script repairs the rows loaded before that fix.

Scope (mirrors the loader's rule-(c) branch):
  - record_type = 'Judgment'                       (all doc types are JUD/CCJ/JUDGMENTS)
  - record_type = 'Lien' AND document_type ILIKE mechanics  (MECHANICS LIENS (ML))
Excluded: tax liens (creditor hardcoded to IRS), code liens (filer-keyword
rule), HOA/other liens (Grantor=debtor rule was already correct).

Usage:
    python scripts/fix_creditor_debtor_swap.py            # dry run (default)
    python scripts/fix_creditor_debtor_swap.py --apply    # perform the swap
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fix_creditor_debtor_swap")

# Mirrors loader scope: judgments + mechanics liens loaded with the old rule (c).
_SCOPE = """
    record_type = 'Judgment'
    OR (record_type = 'Lien' AND (
        document_type ILIKE '%MECHANIC%' OR document_type ILIKE '%(ML)%'
    ))
"""

_MARKER = "creditor_debtor_swap_2026_06_03"


def main(apply: bool) -> None:
    db = Database()
    with db.session_scope() as session:
        # Idempotency guard: skip rows already swapped by a previous run.
        scoped = f"({_SCOPE}) AND (meta_data IS NULL OR NOT meta_data ? '{_MARKER}')"

        total = session.execute(
            text(f"SELECT count(*) FROM legal_and_liens WHERE {scoped}")
        ).scalar()
        inverted = session.execute(text(f"""
            SELECT count(*) FROM legal_and_liens
            WHERE {scoped}
              AND debtor ~* '(BANK|FLORIDA|LLC|CREDIT UNION|FUNDING|CAPITAL|FINANC|PORTFOLIO)'
        """)).scalar()
        logger.info("Rows in scope (not yet swapped): %d", total)
        logger.info("Of those, institutional-looking debtor (inversion signal): %d", inverted)

        sample = session.execute(text(f"""
            SELECT id, record_type, document_type, county_id, creditor, debtor
            FROM legal_and_liens WHERE {scoped} ORDER BY id DESC LIMIT 5
        """)).fetchall()
        logger.info("Sample BEFORE swap:")
        for r in sample:
            logger.info("  id=%s %s/%s %s | creditor=%r debtor=%r", *r)

        if not apply:
            logger.info("DRY RUN — no changes made. Re-run with --apply to swap %d rows.", total)
            return

        if not total:
            logger.info("Nothing to do.")
            return

        # Atomic swap + audit marker in meta_data.
        result = session.execute(text(f"""
            UPDATE legal_and_liens
            SET creditor = debtor,
                debtor   = creditor,
                meta_data = COALESCE(meta_data, '{{}}'::jsonb)
                            || jsonb_build_object('{_MARKER}', true)
            WHERE {scoped}
        """))
        logger.info("Swapped %d rows.", result.rowcount)

        after = session.execute(text(f"""
            SELECT id, creditor, debtor FROM legal_and_liens
            WHERE id IN :ids
        """), {"ids": tuple(r[0] for r in sample)}).fetchall()
        logger.info("Sample AFTER swap:")
        for r in after:
            logger.info("  id=%s | creditor=%r debtor=%r", *r)
        # session_scope commits on clean exit


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="Perform the swap (default: dry run)")
    args = parser.parse_args()
    main(apply=args.apply)
