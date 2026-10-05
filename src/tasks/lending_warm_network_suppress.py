"""Load Josh's warm-network list into the permanent lending suppression gate.

Oct 4 answers §2: "Not in my warm network (permanently suppressed from the cold
queue)." This is not an opt-out — Josh still works these relationships himself —
so it is loaded once he sends the list, via its own SuppressionReason, and is
picked up by every gate that already checks lending.suppression_list
(``filter_loadable`` at load time, the re-load path in dialer_load.py).

Dry run by default: parses the CSV and reports how many phones would be
suppressed, writes nothing. ``--apply`` commits.

Usage:
    python -m src.tasks.lending_warm_network_suppress --input warm_network.csv
    python -m src.tasks.lending_warm_network_suppress --input warm_network.csv --apply

The input is a CSV with a ``phone`` column (any other columns are ignored).
"""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

from src.core.database import get_db_context
from src.lending.compliance import suppress_warm_network_phones

logger = logging.getLogger(__name__)


def _read_phones(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [row["phone"] for row in csv.DictReader(handle) if (row.get("phone") or "").strip()]


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Load Josh's warm network into the permanent suppression gate.")
    parser.add_argument("--input", type=Path, required=True, help="CSV with a 'phone' column")
    parser.add_argument("--apply", action="store_true", help="commit (default: dry run)")
    args = parser.parse_args(argv)

    phones = _read_phones(args.input)
    with get_db_context() as session:
        count = suppress_warm_network_phones(session, phones, source_ref=args.input.name)
        if args.apply:
            session.commit()
        else:
            session.rollback()
    logger.info("[warm-network] %s %d phone(s) from %s (%d rows read)",
                "suppressed" if args.apply else "would suppress", count, args.input, len(phones))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
