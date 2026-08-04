from unittest.mock import patch, MagicMock

from sqlalchemy import text

from src.services.sms_compliance import send_sms


def test_send_sms_logs_telnyx_cost_to_api_usage(fresh_db):
    """A successful Telnyx send must write one api_usage_logs row with service='telnyx'."""
    fake_result = {
        "message_id": "msg_cost_test_001",
        "status": "queued",
        "vendor": "telnyx",
        "cost_cents": 75,
        "sent_at": "2026-07-29T08:00:00+00:00",
    }

    mock_settings = MagicMock()
    mock_settings.telnyx_sms_enabled = True
    mock_settings.telnyx_sms_api_key = "key"
    mock_settings.telnyx_from_number = "+18881234567"
    mock_settings.telnyx_messaging_profile_id = "profile_id"

    with (
        patch("src.services.sms_compliance.telnyx_send_message", return_value=fake_result),
        patch("src.services.sms_compliance.has_opted_in", return_value=True),
        patch("src.services.sms_compliance.settings", mock_settings),
        patch("src.services.compliance_gator.validate_outbound", return_value=MagicMock(allowed=True, reason=None)),
        patch("src.services.sms_compliance.allotment_consume", return_value=True),
    ):
        send_sms(
            to="+18135551234",
            body="test message",
            db=fresh_db,
            message_type="transactional",
        )
    fresh_db.commit()

    row = fresh_db.execute(text("""
        SELECT service, task_type, cost_usd
        FROM api_usage_logs
        WHERE service = 'telnyx' AND task_type = 'telnyx_sms'
        ORDER BY id DESC LIMIT 1
    """)).fetchone()

    assert row is not None, "api_usage_logs must have a telnyx row after a successful send"
    assert row.service == "telnyx"
    assert row.task_type == "telnyx_sms"
    assert abs(float(row.cost_usd) - 0.75) < 0.001
