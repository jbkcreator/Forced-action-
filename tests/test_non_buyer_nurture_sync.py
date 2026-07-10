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


def test_sync_follows_pagination_across_pages():
    settings = MagicMock()
    settings.non_buyer_nurture_campaign_id = "camp_shared"
    db = MagicMock()

    page1 = {"leads": [{"id": "l1", "email": "p1@example.com", "status": "bounced"}],
             "next_starting_after": "cursor_2"}
    page2 = {"leads": [{"id": "l2", "email": "p2@example.com", "status": "active"}],
             "next_starting_after": None}

    with patch("config.settings.get_settings", return_value=settings), \
         patch("src.core.database.get_db_context", side_effect=lambda: _fake_db_context(db)), \
         patch("src.services.instantly_service.list_leads", side_effect=[page1, page2]) as mock_list, \
         patch("src.services.non_buyer_nurture.apply_instantly_status") as mock_apply:
        result = non_buyer_nurture_sync.run()

    assert result["synced"] == 2
    assert mock_apply.call_count == 2
    # second page fetched with the cursor from the first
    assert mock_list.call_count == 2
    assert mock_list.call_args_list[1].kwargs.get("cursor") == "cursor_2" \
        or mock_list.call_args_list[1].args[1:] == ("cursor_2",)


def test_sync_skips_leads_without_email():
    settings = MagicMock()
    settings.non_buyer_nurture_campaign_id = "camp_shared"
    db = MagicMock()

    page = {"leads": [{"id": "l1", "email": "", "status": "bounced"},
                      {"id": "l2", "email": "ok@example.com", "status": "unsubscribed"}],
            "next_starting_after": None}

    with patch("config.settings.get_settings", return_value=settings), \
         patch("src.core.database.get_db_context", return_value=_fake_db_context(db)), \
         patch("src.services.instantly_service.list_leads", return_value=page), \
         patch("src.services.non_buyer_nurture.apply_instantly_status") as mock_apply:
        result = non_buyer_nurture_sync.run()

    assert result["synced"] == 1
    mock_apply.assert_called_once_with(db, "ok@example.com", "unsubscribed", instantly_lead_id="l2")
