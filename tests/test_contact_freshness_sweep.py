from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def test_refresh_owner_uses_stale_refresh_path():
    from src.tasks.contact_freshness_sweep import _refresh_owner

    mock_run = MagicMock(return_value={"success": 1, "retraced": 0})
    assert _refresh_owner(123, "hillsborough", run_skip_trace_fn=mock_run) is True

    mock_run.assert_called_once_with(
        owner_ids=[123],
        county_id="hillsborough",
        today_only=False,
        refresh_stale=True,
        refresh_stale_after_days=0,
        limit=1,
    )


def test_run_sweep_dry_scan_marks_stale_and_queues():
    from src.tasks import contact_freshness_sweep as sweep

    owner = SimpleNamespace(
        id=1,
        property_id=10,
        phone_1="+18135550000",
        phone_2=None,
        phone_3=None,
        phone_metadata={"phone_1": {"type": "mobile", "score": 90, "reachable": True}},
        skip_trace_success=True,
        contact_refresh_status=None,
    )
    contact = SimpleNamespace(
        enriched_at=datetime(2025, 9, 1, tzinfo=timezone.utc),
        confidence=0.85,
        verification_status="valid",
    )
    session = MagicMock()
    session.query.return_value.join.return_value.join.return_value.filter.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [owner]

    with patch.object(sweep, "get_db_context") as mock_ctx, \
         patch.object(sweep, "_latest_contact", return_value=contact), \
         patch.object(sweep, "_last_sms_failed_at", return_value=None):
        mock_ctx.return_value.__enter__.return_value = session
        stats = sweep.run_sweep(
            county_id="hillsborough",
            limit=1,
            refresh_limit=1,
            refresh=False,
            now=datetime(2026, 6, 8, tzinfo=timezone.utc),
        )

    assert stats["scanned"] == 1
    assert stats["stale"] == 1
    assert stats["queued"] == 1
    assert owner.contact_info_confidence == "stale"
    assert owner.contact_refresh_status == "queued"
