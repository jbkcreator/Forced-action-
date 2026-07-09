"""
Smoke test for the non-buyer nurture Instantly sync task — thin wiring only.
"""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import src.core.database  # noqa: F401 — force real-settings import before tests patch get_settings
from src.tasks import non_buyer_nurture_sync


@contextmanager
def _fake_db_context(db):
    yield db


def test_sync_applies_status_per_lead_then_stops_pagination():
    settings = MagicMock()
    settings.non_buyer_nurture_campaign_id = "camp_shared"
    db = MagicMock()

    page = {
        "leads": [
            {"id": "lead_1", "email": "a@example.com", "status": "unsubscribed"},
            {"id": "lead_2", "email": "b@example.com", "status": "active"},
        ],
        "next_starting_after": None,
    }

    with patch("config.settings.get_settings", return_value=settings), \
         patch("src.core.database.get_db_context", return_value=_fake_db_context(db)), \
         patch("src.services.instantly_service.list_leads", return_value=page), \
         patch("src.services.non_buyer_nurture.apply_instantly_status") as mock_apply:
        result = non_buyer_nurture_sync.run()

    assert result["synced"] == 2
    assert mock_apply.call_count == 2
    mock_apply.assert_any_call(db, "a@example.com", "unsubscribed", instantly_lead_id="lead_1")
    mock_apply.assert_any_call(db, "b@example.com", "active", instantly_lead_id="lead_2")
