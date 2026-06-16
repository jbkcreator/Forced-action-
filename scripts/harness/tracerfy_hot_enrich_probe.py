"""
Tracerfy Hot-Enrichment probe (ADR 0018) — isolated, write-safe.

Checks the Tracerfy credit balance, and (optionally) runs the REAL
hot_enrich_properties() over a handful of property IDs inside a transaction it
ROLLS BACK — so it exercises the live API + parser without persisting anything.

This is the one piece the mocked unit tests never cover.

Usage:
    python scripts/harness/tracerfy_hot_enrich_probe.py                 # balance only
    python scripts/harness/tracerfy_hot_enrich_probe.py --pids 101,102,103,104,105
    python scripts/harness/tracerfy_hot_enrich_probe.py --auto 5        # pick 5 qualified leads itself
"""
import argparse
import json
import sys

from sqlalchemy import text

sys.path.insert(0, ".")

from config.settings import get_settings          # noqa: E402
from src.core.database import get_db_context       # noqa: E402


def _auto_pick(db, n: int) -> list[int]:
    rows = db.execute(text("""
        SELECT p.id
        FROM properties p
        JOIN distress_scores ds ON ds.property_id = p.id
        JOIN owners o ON o.property_id = p.id
        WHERE ds.qualified = true
          AND (o.phone_1 IS NOT NULL OR o.email_1 IS NOT NULL)
        ORDER BY ds.final_cds_score DESC
        LIMIT :n
    """), {"n": n}).scalars().all()
    return list(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pids", help="comma-separated property IDs to trace")
    ap.add_argument("--auto", type=int, default=0, help="auto-pick N qualified leads")
    args = ap.parse_args()

    s = get_settings()
    if not s.tracerfy_api_key:
        print("TRACERFY_API_KEY not set — cannot check balance or run a real trace.")
        print("Set it in .env to test the live Hot-Enrichment path.")
        return 1

    from src.services.tracerfy_fallback import get_tracerfy_balance, hot_enrich_properties

    try:
        bal = get_tracerfy_balance()
        print("Tracerfy balance:", json.dumps(bal, default=str)[:500])
    except Exception as exc:
        print("Balance check failed:", exc)
        return 1

    pids: list[int] = []
    if args.pids:
        pids = [int(x) for x in args.pids.split(",") if x.strip()]

    with get_db_context() as db:
        if args.auto and not pids:
            pids = _auto_pick(db, args.auto)
            print("auto-picked property_ids:", pids)

        if not pids:
            print("No --pids/--auto — balance only. Done.")
            return 0

        print(f"Tracing {len(pids)} properties (transaction will roll back)...")
        res = hot_enrich_properties(db, pids)
        db.rollback()   # NEVER persist probe writes

    hits = 0
    for pid in pids:
        r = res.get(pid)
        ok = bool(r and r.get("match_success"))
        hits += ok
        print(f"  {pid}: {'HIT ' if ok else 'MISS'} "
              f"phone={r.get('mobile_phone') or r.get('landline') if r else None} "
              f"email={r.get('email') if r else None}")

    print(f"Quality Floor: {hits}/{len(pids)} -> {'PASS' if hits == len(pids) else 'FAIL'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
