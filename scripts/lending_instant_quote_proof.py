"""Cora P2.2 proof for Josh: render the done-when cases as a markdown table.

Pure: runs the quote engine only (no database, no network).

Usage:
    PYTHONPATH=. python scripts/lending_instant_quote_proof.py [--out PATH]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.lending.instant_quote.proof import render_markdown

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, help="write the table here instead of stdout")
    args = parser.parse_args()
    table = render_markdown()
    if args.out:
        args.out.write_text(table + "\n", encoding="utf-8")
        logger.info("proof table written to %s", args.out)
    else:
        logger.info("\n%s", table)


if __name__ == "__main__":
    main()
