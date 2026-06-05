"""
Verification dump: scrape Pinellas eviction/probate/divorce from the court portal
and write the extracted data to CSVs for manual inspection.

For each signal it writes TWO files to data/verify/:
  <signal>_raw_<window>.csv         — exact portal export columns (1 row per case)
  <signal>_normalized_<window>.csv  — after the name parser (party rows the loader sees)

No DB, no dedup — shows everything extracted. Costs 1 captcha solve per signal.

Usage:
    python -m scripts.experiments.verify_pinellas_extract --days 7
    python -m scripts.experiments.verify_pinellas_extract --days 7 --no-proxy
"""

import argparse
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config.constants import PINELLAS_CASE_TYPE_KEYWORDS
from src.scrappers.court_docket.pinellas.civil_filing import (
    scrape_pinellas_civil, normalize_style_col,
)
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

OUT = Path("data/verify")
FALLBACK_URL = "https://courtrecords.mypinellasclerk.gov"
SIGNALS = ["eviction", "probate", "divorce"]


def _url(county_id: str) -> str:
    try:
        from src.utils.county_config import get_county_config
        s = (get_county_config(county_id).get("sources", {}).get("evictions")
             or get_county_config(county_id).get("sources", {}).get("court_records") or {})
        if s.get("url"):
            return s["url"]
    except Exception as e:
        logger.warning("[verify] county_config failed (%s) — fallback", e)
    return FALLBACK_URL


async def run(days: int, no_proxy: bool) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    url = _url("pinellas")
    end = datetime.now()
    start = end - timedelta(days=days)
    s_iso, e_iso = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    win = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
    summary = []

    for signal in SIGNALS:
        kws = PINELLAS_CASE_TYPE_KEYWORDS[signal]
        logger.info("[verify] === %s (%s..%s) ===", signal, s_iso, e_iso)
        try:
            xlsx = await scrape_pinellas_civil(
                signal, kws, url, OUT,
                start_date=s_iso, end_date=e_iso, no_proxy=no_proxy,
            )
            raw = pd.read_excel(xlsx)
            raw_csv = OUT / f"{signal}_raw_{win}.csv"
            raw.to_csv(raw_csv, index=False)

            norm = normalize_style_col(raw.copy(), signal)
            norm_csv = OUT / f"{signal}_normalized_{win}.csv"
            norm.to_csv(norm_csv, index=False)

            logger.info("[verify] %s: %d cases -> %d party rows", signal, len(raw), len(norm))
            summary.append((signal, len(raw), len(norm), raw_csv.name, norm_csv.name))
        except Exception as e:
            logger.error("[verify] %s FAILED: %s", signal, e)
            summary.append((signal, "ERR", "ERR", str(e)[:80], ""))

    print("\n" + "=" * 72)
    print(f"VERIFY DUMP  window={s_iso}..{e_iso}  ->  {OUT.resolve()}")
    print("=" * 72)
    for sig, ncase, nrow, rawf, normf in summary:
        print(f"  {sig:9} cases={ncase:>4}  party_rows={nrow:>4}  | {rawf}  +  {normf}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--no-proxy", action="store_true")
    args = ap.parse_args()
    asyncio.run(run(args.days, args.no_proxy))


if __name__ == "__main__":
    main()
