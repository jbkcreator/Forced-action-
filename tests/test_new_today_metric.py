from datetime import date
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.tasks.daily_report import _build_vertical_tier_crosstab


def test_build_vertical_tier_crosstab_counts_gold_plus_scored_today(monkeypatch):
    run_date = date(2026, 4, 13)

    def fake_current_gold_plus_inventory(session, run_date_, county_id):
        return [
            (1, "Gold", {"roofing": 10}, run_date),
            (2, "Gold", {"roofing": 8}, run_date),
            (3, "Gold", {"roofing": 6}, date(2026, 4, 12)),
        ]

    monkeypatch.setattr("src.tasks.daily_report._current_gold_plus_inventory", fake_current_gold_plus_inventory)

    crosstab = _build_vertical_tier_crosstab(None, run_date, "hillsborough")

    assert crosstab["Roofing"]["Gold"]["count"] == 3
    assert crosstab["Roofing"]["Gold"]["new_today"] == 2


def test_feed_stats_new_today_counts_unique_properties(mock_db):
    subscriber = MagicMock(status="active", county_id="hillsborough")

    subscriber_query = MagicMock()
    subscriber_query.scalar_one_or_none.return_value = subscriber

    locked_zips_query = MagicMock()
    locked_zips_query.scalars.return_value.all.return_value = ["12345"]

    total_query = MagicMock()
    total_query.scalar.return_value = 3

    new_today_query = MagicMock()
    new_today_query.scalar.return_value = 2

    tier_query = MagicMock()
    tier_query.all.return_value = [MagicMock(lead_tier="Gold", cnt=2)]

    last_updated_query = MagicMock()
    last_updated_query.scalar.return_value = date.today()

    mock_db.execute.side_effect = [
        subscriber_query,
        locked_zips_query,
        total_query,
        new_today_query,
        tier_query,
        last_updated_query,
    ]

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        client = TestClient(app)
        response = client.get("/api/feed/feed-123/stats")

        assert response.status_code == 200
        assert response.json()["new_today"] == 2
    finally:
        app.dependency_overrides.pop(get_db, None)
