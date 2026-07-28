from __future__ import annotations

import base64
from unittest.mock import MagicMock

from src.agents.cora import queue
from src.agents.cora.ingestion import reply_mailbox_poller as poller


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")


def _fake_message(message_id: str, from_addr: str, subject: str, body_text: str) -> dict:
    return {
        "id": message_id,
        "payload": {
            "mimeType": "text/plain",
            "headers": [
                {"name": "From", "value": f"Prospect Name <{from_addr}>"},
                {"name": "Subject", "value": subject},
            ],
            "body": {"data": _b64(body_text)},
        },
    }


def test_not_configured_returns_zero_without_error(monkeypatch):
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: None)
    assert poller.poll_once() == 0


def test_poll_once_publishes_unread_messages_and_marks_seen(monkeypatch):
    service = MagicMock()
    service.users.return_value.messages.return_value.list.return_value.execute.return_value = {
        "messages": [{"id": "msg-1"}]
    }
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-1", "prospect@example.com", "Re: Founding seat", "Sounds good, tell me more."
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once()
    assert published == 1

    seen = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(seen) == 1
    assert seen[0].event_type == "reply.received"
    assert seen[0].payload["from_address"] == "prospect@example.com"
    assert seen[0].payload["subject"] == "Re: Founding seat"
    assert seen[0].payload["body_text"] == "Sounds good, tell me more."
    assert seen[0].payload["opportunity_thread_id"] is None
    queue.ack(seen[0].message_id)

    # Never calls .modify() — read-only scope only.
    service.users.return_value.messages.return_value.modify.assert_not_called()


def test_already_seen_message_is_not_republished(monkeypatch):
    service = MagicMock()
    service.users.return_value.messages.return_value.list.return_value.execute.return_value = {
        "messages": [{"id": "msg-2"}]
    }
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-2", "prospect@example.com", "Re:", "hi"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    first = poller.poll_once()
    assert first == 1
    queue.read_batch("drain-1", count=10, block_ms=200)  # drain so pending doesn't confuse the next assertion

    second = poller.poll_once()
    assert second == 0


def test_decode_body_walks_multipart_preferring_plain_text():
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {"mimeType": "text/html", "body": {"data": _b64("<p>hi <b>there</b></p>")}},
            {"mimeType": "text/plain", "body": {"data": _b64("hi there")}},
        ],
    }
    assert poller._decode_body(payload) == "hi there"


def test_decode_body_falls_back_to_html_stripped_of_tags():
    payload = {"mimeType": "text/html", "body": {"data": _b64("<p>hello <b>world</b></p>")}}
    assert poller._decode_body(payload) == "hello world"


def test_a_processing_failure_on_one_message_does_not_block_others(monkeypatch):
    service = MagicMock()
    service.users.return_value.messages.return_value.list.return_value.execute.return_value = {
        "messages": [{"id": "bad-1"}, {"id": "good-1"}]
    }

    def _get(userId, id, format):
        if id == "bad-1":
            raise RuntimeError("simulated Gmail API error")
        result = MagicMock()
        result.execute.return_value = _fake_message("good-1", "prospect2@example.com", "Re:", "ok")
        return result

    service.users.return_value.messages.return_value.get.side_effect = _get
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once()
    assert published == 1  # the good one still made it through

    seen = queue.read_batch("test-consumer-3", count=10, block_ms=200)
    assert len(seen) == 1
    assert seen[0].payload["from_address"] == "prospect2@example.com"
