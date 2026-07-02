"""
EXPLAIN / EXPLAIN ANALYZE runner — edit QUERY and OPTIONS below, then run:

    PYTHONPATH=. .venv/Scripts/python.exe scripts/explain_query.py

No DB shell required. Uses the same DATABASE_URL as the app.
"""
from __future__ import annotations

from sqlalchemy import text as sa_text
from src.core.database import get_db_context

# ── Edit your query here ─────────────────────────────────────────────────────
QUERY = """
WITH ranked_scores AS (
    SELECT
        property_id,
        final_cds_score,
        county_id,
        PERCENT_RANK() OVER (PARTITION BY county_id ORDER BY final_cds_score DESC) AS pct_rank
    FROM distress_scores
    WHERE county_id IN ('hillsborough', 'pinellas')
)
SELECT p.id, p.address, rs.final_cds_score, rs.county_id, rs.pct_rank
FROM ranked_scores rs
JOIN properties p ON p.id = rs.property_id
WHERE rs.pct_rank <= 0.10
ORDER BY rs.county_id, rs.final_cds_score DESC
"""

# ── Options ──────────────────────────────────────────────────────────────────
# MODE: "explain" (plan only, no execution) or "analyze" (runs the query)
MODE = "analyze"

# Extra EXPLAIN options — set to True to enable
BUFFERS = True    # shows cache hits vs disk reads (only meaningful with analyze)
VERBOSE = False   # adds output column list and per-node CPU costs
FORMAT  = "text"  # "text" | "json" | "yaml"
# ─────────────────────────────────────────────────────────────────────────────


def build_explain(query: str) -> str:
    opts: list[str] = []
    if MODE == "analyze":
        opts.append("ANALYZE")
    if BUFFERS:
        opts.append("BUFFERS")
    if VERBOSE:
        opts.append("VERBOSE")
    opts.append(f"FORMAT {FORMAT.upper()}")
    options = ", ".join(opts)
    return f"EXPLAIN ({options})\n{query.strip()}"


def main() -> None:
    explain_sql = build_explain(QUERY)

    print("=" * 70)
    print("Query being explained:")
    print("-" * 70)
    print(QUERY.strip())
    print("=" * 70)
    print(f"Mode: EXPLAIN ({', '.join(o for o in build_explain('').splitlines()[0].replace('EXPLAIN (','').replace(')','').split(','))})")
    print("=" * 70)

    with get_db_context() as session:
        rows = session.execute(sa_text(explain_sql)).fetchall()

    print()
    for row in rows:
        print(row[0])


if __name__ == "__main__":
    main()
