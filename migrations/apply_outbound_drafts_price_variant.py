"""
Add price_cents / experiment_assignment_id to outbound_drafts (LEARN-v2.2
Layer 1 — wiring src/services/agent_lane_experiment_engine.py's price-band
arm selection into Cora's outreach subgraph, src/agents/cora/subgraphs/
outreach.py's new price_variant node).

Both nullable — an offer with no configured price band (RESPA-excluded or
unconfigured, see price_assignment.is_respa_excluded()/PRICE_BANDS) leaves
both NULL, same as every draft written before this column existed.
experiment_assignment_id is what Layer 2's attribution join will use to
connect a later reply/booking/payment back to the arm that produced this
draft.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_outbound_drafts_price_variant.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

DDL = [
    "ALTER TABLE outbound_drafts ADD COLUMN IF NOT EXISTS price_cents INTEGER",
    "ALTER TABLE outbound_drafts ADD COLUMN IF NOT EXISTS experiment_assignment_id INTEGER "
    "REFERENCES agent_lane_experiment_assignments(id)",
]


def main() -> None:
    with get_db_context() as db:
        for stmt in DDL:
            db.execute(text(stmt))
        db.commit()
    print("apply_outbound_drafts_price_variant: done.")


if __name__ == "__main__":
    main()
