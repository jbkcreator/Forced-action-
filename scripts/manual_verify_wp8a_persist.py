"""
Manual WP-8A persistence verification.

Writes ONE real row to fa_max_quote_ready_results using compute_quote_ready()
+ build_result_row() (the real production functions) against a real,
existing fa_max_opportunities row. There is currently no production caller
that does this INSERT — this script does it directly via raw SQL because no
write function exists in src/services/quote_ready/persistence.py (confirmed:
that module only has build_result_row(), which returns a dict, never an
INSERT). This script proves the compute+row-building logic is correct and
storable; it does NOT prove any real workflow persists this today.

Run: PYTHONPATH=. .venv/bin/python scripts/manual_verify_wp8a_persist.py
Then check the raw row yourself:
  psql -h localhost -U distress_user -d distress_db -x -c \
    "SELECT * FROM fa_max_quote_ready_results ORDER BY computed_at DESC LIMIT 1;"
"""
import json
from decimal import Decimal

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.quote_ready.compute import compute_quote_ready
from src.services.quote_ready.models import QuoteReadyInput
from src.services.quote_ready.persistence import build_result_row

with get_db_context() as db:
    real_opp = db.execute(text("SELECT opportunity_id::text FROM fa_max_opportunities LIMIT 1")).scalar()
if real_opp is None:
    raise SystemExit("No real fa_max_opportunities row exists on this server to attach to.")

print(f"Using real existing opportunity_id={real_opp}")

inp = QuoteReadyInput(
    opportunity_id=real_opp,
    max_ltc=Decimal("0.85"),
    max_ltv=Decimal("0.75"),
    purchase_price=Decimal("300000"),
    rehab_estimate=Decimal("50000"),
    arv=Decimal("450000"),
)
result = compute_quote_ready(inp)
row = build_result_row(inp, result, computed_by="manual_verification_run")

with get_db_context() as db:
    db.execute(
        text(
            """
            INSERT INTO fa_max_quote_ready_results
                (opportunity_id, property_id, calculation_version, input_hash, status,
                 inputs, outputs, provenance, confidence, missing_inputs, computed_by)
            VALUES
                (:opportunity_id, :property_id, :calculation_version, :input_hash, :status,
                 :inputs ::jsonb, :outputs ::jsonb, :provenance ::jsonb, :confidence ::jsonb,
                 :missing_inputs ::jsonb, :computed_by)
            """
        ),
        {
            **row,
            "inputs": json.dumps(row["inputs"]),
            "outputs": json.dumps(row["outputs"]),
            "provenance": json.dumps(row["provenance"]),
            "confidence": json.dumps(row["confidence"]),
            "missing_inputs": json.dumps(row["missing_inputs"]),
        },
    )
    db.commit()

print("Inserted 1 row into fa_max_quote_ready_results.")
print("Now run:")
print(
    '  psql -h localhost -U distress_user -d distress_db -x -c '
    '"SELECT * FROM fa_max_quote_ready_results ORDER BY computed_at DESC LIMIT 1;"'
)
