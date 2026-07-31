import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from src.agents.contracts.base import HandoffRejected
from src.agents.contracts.cora_to_relay import validate_handoff
from src.services.relay import queue as relay_queue


def test_valid_email_handoff_passes():
    handoff = validate_handoff(
        idempotency_key="cora_draft:abc-123",
        channel="noop",  # registered by default (channels.py's own built-in)
        recipient="buyer@example.com",
        payload={"subject": "Quick question", "body": "Hi there"},
        thread_id="OPP-2026-00042",
    )
    assert handoff.thread_id == "OPP-2026-00042"


def test_missing_thread_id_rejected():
    with pytest.raises(ValidationError):
        validate_handoff(
            idempotency_key="cora_draft:abc-124",
            channel="noop",
            recipient="buyer@example.com",
            payload={"subject": "Hi", "body": "Hi there"},
            thread_id=None,
        )


def test_bad_thread_id_format_rejected():
    with pytest.raises(ValidationError):
        validate_handoff(
            idempotency_key="cora_draft:abc-125",
            channel="noop",
            recipient="buyer@example.com",
            payload={"subject": "Hi", "body": "Hi there"},
            thread_id="not-a-real-thread",
        )


def test_unregistered_channel_rejected():
    """Directly proves the R4 audit's finding #1 is closed: an unknown
    channel must be rejected HERE, at intake -- not silently land 'pending'
    and fail later at dispatch."""
    with pytest.raises(ValidationError):
        validate_handoff(
            idempotency_key="cora_draft:abc-126",
            channel="carrier_pigeon",
            recipient="buyer@example.com",
            payload={"subject": "Hi", "body": "Hi there"},
            thread_id="OPP-2026-00042",
        )


def test_empty_payload_rejected():
    """Directly proves the R4 audit's other finding: payload={} must be
    rejected here, not silently default subject/body to ''."""
    with pytest.raises(ValidationError):
        validate_handoff(
            idempotency_key="cora_draft:abc-127",
            channel="noop",
            recipient="buyer@example.com",
            payload={},
            thread_id="OPP-2026-00042",
        )


def test_email_channel_requires_valid_email_recipient_and_subject():
    with pytest.raises(ValidationError):
        validate_handoff(
            idempotency_key="cora_draft:abc-128",
            channel="email",
            recipient="not-an-email",
            payload={"subject": "Hi", "body": "Hi there"},
            thread_id="OPP-2026-00042",
        )
    with pytest.raises(ValidationError):
        validate_handoff(
            idempotency_key="cora_draft:abc-129",
            channel="email",
            recipient="buyer@example.com",
            payload={"body": "Hi there"},  # no subject
            thread_id="OPP-2026-00042",
        )


def test_missing_booking_and_payment_link_is_allowed():
    """SOFT field -- confirmed multiple real offers (bankruptcy_alert,
    founder_tier without Stripe configured) legitimately carry neither."""
    handoff = validate_handoff(
        idempotency_key="cora_draft:abc-130",
        channel="noop",
        recipient="buyer@example.com",
        payload={"subject": "Hi", "body": "Hi there"},
        thread_id="OPP-2026-00042",
    )
    assert handoff.body == "Hi there"


def test_missing_cell_tag_logs_warning_but_does_not_reject(caplog):
    """SOFT field -- to_relay_handoff_payload() never carries cell_id/offer/
    avenue/angle today (verified directly), so hard-requiring it would
    reject 100% of real traffic."""
    import logging
    with caplog.at_level(logging.WARNING):
        validate_handoff(
            idempotency_key="cora_draft:abc-131",
            channel="noop",
            recipient="buyer@example.com",
            payload={"subject": "Hi", "body": "Hi there"},  # no cell_id/offer/avenue/angle
            thread_id="OPP-2026-00042",
        )
    assert any("cell_tag_missing" in m for m in caplog.messages)


def test_enqueue_rejects_incomplete_payload_and_writes_audit_row(fresh_db):
    idem = f"bad-{uuid.uuid4().hex[:12]}"
    with pytest.raises(HandoffRejected):
        relay_queue.enqueue(
            idempotency_key=idem,
            channel="noop",
            recipient="buyer@example.com",
            payload={},  # missing body -- the exact R4-audit gap
            thread_id="OPP-2026-00042",
        )

    row = fresh_db.execute(
        text("SELECT boundary FROM handoff_rejections WHERE reference_id = :r ORDER BY id DESC LIMIT 1"),
        {"r": idem},
    ).mappings().first()
    assert row["boundary"] == "cora_to_relay"

    # And no relay_approval_queue row was ever created for the rejected item.
    queue_row = fresh_db.execute(
        text("SELECT id FROM relay_approval_queue WHERE idempotency_key = :k"), {"k": idem},
    ).first()
    assert queue_row is None


def test_enqueue_accepts_a_complete_handoff(fresh_db):
    idem = f"good-{uuid.uuid4().hex[:12]}"
    item = relay_queue.enqueue(
        idempotency_key=idem,
        channel="noop",
        recipient="buyer@example.com",
        payload={"subject": "Hi", "body": "Hello there"},
        thread_id="OPP-2026-00043",
    )
    assert item.status == "pending"
    assert item.thread_id == "OPP-2026-00043"


def test_seed_escape_hatch_bypasses_validation(fresh_db):
    """--seed's skip_contract_validation=True must still work with no
    thread_id -- confirms the escape hatch doesn't regress R1's own
    scaffolding tool."""
    idem = f"seed-{uuid.uuid4().hex[:12]}"
    item = relay_queue.enqueue(
        idempotency_key=idem,
        channel="noop",
        recipient="buyer@example.com",
        payload={"subject": "Hi", "body": "Hello there"},
        thread_id=None,
        skip_contract_validation=True,
    )
    assert item.status == "pending"
    assert item.thread_id is None
