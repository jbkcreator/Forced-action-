from datetime import datetime, timezone
from unittest.mock import MagicMock


def test_create_suppression_cancels_pending_messages():
    from src.services.cora_suppression import create_suppression

    db = MagicMock()
    pending = MagicMock()
    pending.send_status = "pending_review"
    db.execute.return_value.scalars.return_value.all.return_value = [pending]

    now = datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc)
    suppression = create_suppression(
        db,
        subscriber_id=7,
        reason="human_replied",
        source="inbound_sms",
        source_id="msg_123",
        notes="Auto-pause triggered by human reply",
        cancel_reason="human_replied_auto_pause",
        now=now,
    )

    assert suppression.subscriber_id == 7
    assert suppression.reason == "human_replied"
    assert suppression.source_id == "msg_123"
    assert pending.send_status == "cancelled"
    assert pending.cancelled_at == now
    assert pending.cancel_reason == "human_replied_auto_pause"
    assert db.add.called
    assert db.flush.called


def test_record_generic_sms_reply_sets_replied_at_and_suppresses():
    from src.services.cora_suppression import record_generic_sms_reply

    db = MagicMock()
    opt_in = MagicMock(subscriber_id=9)
    outcome = MagicMock()
    db.execute.side_effect = [
        MagicMock(scalar_one_or_none=MagicMock(return_value=opt_in)),
        MagicMock(scalar_one_or_none=MagicMock(return_value=outcome)),
        MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))),
    ]

    now = datetime(2026, 5, 28, 12, 5, tzinfo=timezone.utc)
    suppression = record_generic_sms_reply(
        db,
        phone="+18135550123",
        source_id="msg_456",
        now=now,
    )

    assert outcome.replied_at == now
    assert suppression.subscriber_id == 9
    assert suppression.reason == "human_replied"
    assert suppression.source == "inbound_sms"
