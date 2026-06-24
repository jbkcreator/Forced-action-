"""Seed scoring_weight_overrides from config/heuristics.json.

Run once after migration, or any time the closing desk updates heuristics.json:
    PYTHONPATH=. python scripts/seed_heuristics.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.database import get_db_context
from src.services.heuristic_loader import seed_from_json


def main() -> None:
    with get_db_context() as db:
        n = seed_from_json(db)
    print(f"seed_heuristics: seeded {n} override rows from config/heuristics.json")


if __name__ == "__main__":
    main()
