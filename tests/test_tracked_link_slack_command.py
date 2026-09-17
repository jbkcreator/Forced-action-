"""WP-7 — /tracked-link Slack slash command (Phase A of the Socket Mode
integration, see tasks/FA_Max_build). Two layers tested separately:

- build_tracked_link_reply(): the actual parsing + minting logic, tested
  directly against a real DB session (fresh_db) — no Slack objects needed.
- handle_tracked_link_socket_request(): the thin Socket Mode envelope
  adapter, tested with the same MagicMock/SimpleNamespace fake-request
  pattern PR #276 (wp2-socket-mode-approvals) uses for socket_listener.py's
  handle_socket_request, so both listeners are tested the same way once
  they share one connection.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import uuid

from config.settings import settings as global_settings
from src.core.database import get_db_context
from src.services.tracked_links import (
    build_tracked_link_reply,
    handle_tracked_link_socket_request,
)

_TAG = "ztest-slack-slash"
_ALLOWED_CHANNEL = "C_TEST_RELATIONSHIPS"


def test_usage_message_on_missing_args(fresh_db):
    reply = build_tracked_link_reply(fresh_db, "partner", "josh")
    assert "Usage" in reply["text"]


def test_unknown_kind_rejected(fresh_db):
    reply = build_tracked_link_reply(fresh_db, "bogus_kind some label", "josh")
    assert "Unknown kind" in reply["text"]


def test_partner_link_created(fresh_db):
    reply = build_tracked_link_reply(fresh_db, f"partner {_TAG} Acme Title Co", "josh")
    assert reply["text"].startswith("Created:")
    assert "/go/" in reply["text"]


def test_property_mailer_address_not_found(fresh_db):
    reply = build_tracked_link_reply(fresh_db, f"property_mailer {uuid.uuid4().hex} Nonexistent Rd", "josh")
    assert "Couldn't confidently match" in reply["text"]


def test_property_mailer_resolves_address_and_creates_link(fresh_db):
    from sqlalchemy import text as sa_text
    from src.loaders.base import BaseLoader

    address = "77 Slash Phase A Ln"
    normalized = BaseLoader.normalize_address(address, "hillsborough")
    row = fresh_db.execute(
        sa_text(
            "INSERT INTO properties (parcel_id, address, normalized_address, city, state, zip, "
            "county_id, created_at, updated_at) "
            "VALUES (:parcel, :address, :normalized, 'Tampa', 'FL', '33604', 'hillsborough', now(), now()) "
            "RETURNING id"
        ),
        {"parcel": f"{_TAG}-{uuid.uuid4().hex[:8]}", "address": address, "normalized": normalized},
    ).first()
    assert row is not None
    fresh_db.flush()

    reply = build_tracked_link_reply(fresh_db, f"property_mailer {address}", "josh")
    assert reply["text"].startswith("Created:")

    slug = reply["text"].split("/go/")[1].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT kind, property_id, created_by FROM tracked_links WHERE slug = :slug"),
        {"slug": slug},
    ).mappings().first()
    assert link_row["kind"] == "property_mailer"
    assert link_row["property_id"] == row.id
    assert link_row["created_by"] == "slack:josh"


# ---------------------------------------------------------------------------
# Socket Mode envelope adapter — same fake-request pattern as PR #276
# ---------------------------------------------------------------------------


def test_socket_handler_ignores_non_slash_command_requests():
    client = MagicMock()
    request = SimpleNamespace(envelope_id="env-1", type="interactive", payload={})
    assert handle_tracked_link_socket_request(client, request) is False
    client.send_socket_mode_response.assert_not_called()


def test_socket_handler_ignores_other_slash_commands():
    client = MagicMock()
    request = SimpleNamespace(
        envelope_id="env-2", type="slash_commands",
        payload={"command": "/some-other-command", "text": "", "user_name": "josh", "channel_id": _ALLOWED_CHANNEL},
    )
    assert handle_tracked_link_socket_request(client, request) is False
    client.send_socket_mode_response.assert_not_called()


def test_socket_handler_rejects_wrong_channel(monkeypatch):
    monkeypatch.setattr(global_settings, "fa_max_slack_channel_relationships", _ALLOWED_CHANNEL)

    client = MagicMock()
    request = SimpleNamespace(
        envelope_id="env-4", type="slash_commands",
        payload={"command": "/tracked-link", "text": f"partner {_TAG} wrong channel", "user_name": "josh",
                  "channel_id": "C_SOME_OTHER_CHANNEL"},
    )

    assert handle_tracked_link_socket_request(client, request) is True  # handled — we responded, just refused
    client.send_socket_mode_response.assert_called_once()
    sent = client.send_socket_mode_response.call_args[0][0]
    assert "fa-max-relationships" in sent.payload["text"]


def test_socket_handler_handles_tracked_link_command(monkeypatch):
    monkeypatch.setattr(global_settings, "fa_max_slack_channel_relationships", _ALLOWED_CHANNEL)

    client = MagicMock()
    request = SimpleNamespace(
        envelope_id="env-3", type="slash_commands",
        payload={"command": "/tracked-link", "text": f"partner {_TAG} socket test", "user_name": "josh",
                  "channel_id": _ALLOWED_CHANNEL},
    )

    assert handle_tracked_link_socket_request(client, request) is True
    client.send_socket_mode_response.assert_called_once()

    sent = client.send_socket_mode_response.call_args[0][0]
    assert sent.envelope_id == "env-3"
    assert sent.payload["text"].startswith("Created:")

    with get_db_context() as db:
        from sqlalchemy import text as sa_text
        db.execute(sa_text("DELETE FROM tracked_links WHERE created_by = 'slack:josh' AND label LIKE :pat"),
                   {"pat": f"%{_TAG}%"})
        db.commit()
