from datetime import date

from sqlalchemy import text

from src.services.agent_pnl_service import SEAT_GRAPH_MAP, rollup_month, build_monthly_email


def _seed_api_usage(db, graph_name: str, cost_usd: float, created_at: str) -> None:
    db.execute(text("""
        INSERT INTO api_usage_logs (service, model, cost_usd, graph_name, created_at)
        VALUES ('claude', 'sonnet', :cost, :graph, CAST(:ts AS timestamptz))
    """), {"cost": cost_usd, "graph": graph_name, "ts": created_at})


def test_seat_graph_map_covers_all_seats():
    assert set(SEAT_GRAPH_MAP.keys()) == {"vera", "cora", "hunter", "relay", "dev_shop", "lifecycle"}


def test_lifecycle_graphs_in_seat_map():
    lifecycle_graphs = SEAT_GRAPH_MAP["lifecycle"]
    assert "fomo" in lifecycle_graphs
    assert "retention" in lifecycle_graphs
    assert len(lifecycle_graphs) == 14


def test_cora_graphs_in_seat_map():
    assert set(SEAT_GRAPH_MAP["cora"]) == {"cora_outreach", "cora_pre_call", "cora_reply"}


def test_rollup_month_aggregates_cora_compute(fresh_db):
    _seed_api_usage(fresh_db, "cora_outreach", 0.50, "2099-07-15T10:00:00+00:00")
    _seed_api_usage(fresh_db, "cora_reply",    0.25, "2099-07-20T10:00:00+00:00")
    fresh_db.commit()

    result = rollup_month(fresh_db, date(2099, 7, 1))

    cora_row = next(r for r in result if r["seat"] == "cora")
    assert cora_row["compute_cost_cents"] == 75   # ($0.50 + $0.25) × 100


def test_rollup_month_lifecycle_compute(fresh_db):
    _seed_api_usage(fresh_db, "fomo",      0.10, "2099-07-10T10:00:00+00:00")
    _seed_api_usage(fresh_db, "retention", 0.20, "2099-07-11T10:00:00+00:00")
    fresh_db.commit()

    result = rollup_month(fresh_db, date(2099, 7, 1))

    lc_row = next(r for r in result if r["seat"] == "lifecycle")
    assert lc_row["compute_cost_cents"] == 30


def test_rollup_month_net_contribution_formula(fresh_db):
    """net_contribution = attributed_gp - compute - data - founder_minutes."""
    _seed_api_usage(fresh_db, "cora_outreach", 1.00, "2099-07-05T10:00:00+00:00")
    fresh_db.commit()

    result = rollup_month(fresh_db, date(2099, 7, 1))
    cora_row = next(r for r in result if r["seat"] == "cora")

    expected_net = (
        cora_row["attributed_gp_cents"]
        - cora_row["compute_cost_cents"]
        - cora_row["data_cost_cents"]
        - cora_row["founder_minutes_cost_cents"]
    )
    assert cora_row["net_contribution_cents"] == expected_net


def test_build_monthly_email_contains_all_seats(fresh_db):
    result = rollup_month(fresh_db, date(2099, 7, 1))
    email = build_monthly_email(result, date(2099, 7, 1))
    for seat in ("vera", "cora", "hunter", "relay", "dev_shop", "lifecycle"):
        assert seat in email.lower(), f"Email must contain seat name: {seat}"
