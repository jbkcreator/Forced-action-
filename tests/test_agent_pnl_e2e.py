"""
QUALITY-v2.2 Q2 — end-to-end acceptance test.

Seeds compute cost (api_usage_logs) and a manual cost entry
(agent_manual_cost_entries) for a far-future period, runs the full
agent_pnl_monthly.run(), and asserts the agent_pnl row is correct.
"""
from datetime import date
from unittest.mock import patch

from sqlalchemy import text

from src.tasks.agent_pnl_monthly import run

PERIOD = date(2099, 11, 1)


def _seed(db) -> None:
    # Compute: cora_outreach $1.00
    db.execute(text("""
        INSERT INTO api_usage_logs (service, model, cost_usd, graph_name, created_at)
        VALUES ('claude', 'sonnet', 1.00, 'cora_outreach',
                CAST('2099-11-10T10:00:00+00:00' AS timestamptz))
    """))
    # Manual: dev_shop $5.00
    db.execute(text("""
        INSERT INTO agent_manual_cost_entries (seat, period_month, vendor, amount_cents, description)
        VALUES ('dev_shop', '2099-11-01', 'test_vendor', 500, 'contractor invoice test')
    """))
    db.commit()


def test_e2e_pnl_rows_written(fresh_db):
    _seed(fresh_db)
    with patch("src.services.email.send_email"):
        run(session=fresh_db, period_month=PERIOD)

    rows = fresh_db.execute(text(
        "SELECT seat, compute_cost_cents, data_cost_cents, net_contribution_cents "
        "FROM agent_pnl WHERE period_month = :m ORDER BY seat"
    ), {"m": PERIOD}).fetchall()

    by_seat = {r.seat: r for r in rows}
    assert set(by_seat) == {"vera", "cora", "hunter", "relay", "dev_shop", "lifecycle"}

    cora = by_seat["cora"]
    assert cora.compute_cost_cents == 100   # $1.00 × 100
    assert cora.net_contribution_cents == (
        0 - cora.compute_cost_cents - cora.data_cost_cents - 0
    )

    dev = by_seat["dev_shop"]
    assert dev.data_cost_cents == 500   # $5.00 manual
