"""CLI wrapper for the set-based voter bulk loader.

Core logic lives in src/loaders/voter_registry.py:bulk_load_voters_csv —
shared with the monthly auto-refresh cron (src/tasks/voter_registry_refresh.py).

Usage:
    PYTHONPATH=. python scripts/bulk_load_voters.py [--zip PATH] [--limit N]
"""
import argparse
import logging
import sys
import zipfile

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from src.loaders.voter_registry import bulk_load_voters_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", default="scratch/all_eligible.zip")
    ap.add_argument("--limit", type=int, default=None, help="max rows (for testing)")
    ap.add_argument("--county", default="hillsborough")
    args = ap.parse_args()

    zf = zipfile.ZipFile(args.zip)
    txt = next(n for n in zf.namelist() if n.lower().endswith(".txt"))
    with zf.open(txt) as fh:
        rows, upserted, unmatched = bulk_load_voters_csv(
            fh, county_id=args.county, limit=args.limit,
        )
    print(f"rows_read={rows} matched_upserted={upserted} unmatched={unmatched}")


if __name__ == "__main__":
    main()
