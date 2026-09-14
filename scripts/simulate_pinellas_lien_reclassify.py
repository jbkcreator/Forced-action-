"""
Simulation: apply the updated _sub_categorise_liens logic to 100 random
Pinellas rows that are currently misclassified as document_type='LIEN'.

Read-only. Shows what the forward-going fix produces on existing data.
The classifier's keyword branches (HOA/IRS/medical/code/mechanics) work
entirely from creditor/debtor — the original raw doc-type string from the
portal is gone (the DB only holds the processed output 'LIEN'), but these
branches are the ones that actually matter for sub-labelling.

Usage:
    PYTHONPATH=. python scripts/simulate_pinellas_lien_reclassify.py
"""
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

import pandas as pd
from sqlalchemy import text

from src.core.database import get_db_context
from src.scrappers.liens.lien_engine import _sub_categorise_liens
from src.utils.county_config import get_county_config


def main() -> None:
    pinellas_cfg = get_county_config("pinellas")

    with get_db_context() as db:
        rows = db.execute(text(
            """
            SELECT id, document_type, creditor, debtor
            FROM legal_and_liens
            WHERE county_id = 'pinellas'
              AND document_type = 'LIEN'
            ORDER BY RANDOM()
            LIMIT 100
            """
        )).fetchall()

    if not rows:
        print("No misclassified Pinellas rows found.")
        return

    df = pd.DataFrame(rows, columns=["id", "document_type", "creditor", "debtor"])

    # Map DB column names to what _sub_categorise_liens expects.
    # creditor = who filed (Grantor), debtor = property owner (Grantee).
    # document_type stays as-is — the updated code falls back to it when
    # DocType is absent, so it sees 'LIEN' and enters the keyword branch.
    df = df.rename(columns={"creditor": "Grantor", "debtor": "Grantee"})

    result = _sub_categorise_liens(df, pinellas_cfg)

    counts = Counter(result["document_type"].tolist())
    total  = len(result)

    print(f"\nSimulation: {total} random Pinellas rows currently labelled 'LIEN'")
    print(f"{'Label':<30}  {'Count':>6}  {'%':>6}")
    print("-" * 46)
    for label, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"{label:<30}  {n:>6}  {100*n/total:>5.1f}%")

    unchanged = counts.get("LIEN", 0)
    reclassified = total - unchanged
    print(f"\nReclassified:  {reclassified}/{total}  ({100*reclassified/total:.1f}%)")
    print(f"Still 'LIEN':  {unchanged}/{total}  ({100*unchanged/total:.1f}%)")
    print(
        "\nNote: rows still 'LIEN' are genuine mechanics liens with no HOA/IRS/"
        "medical/code-filer keywords in creditor/debtor. Rows that should have"
        " been 'TAX LIEN' or 'LIS PENDENS' at the portal level cannot be"
        " recovered from DB alone — the forward-going fix will capture them"
        " correctly on the next scrape run."
    )

    print("\nSample reclassified rows:")
    changed = result[result["document_type"] != "LIEN"][["id", "Grantor", "Grantee", "document_type"]].head(15)
    print(changed.to_string(index=False))


if __name__ == "__main__":
    main()
