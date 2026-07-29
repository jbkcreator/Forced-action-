from __future__ import annotations

import base64
from unittest.mock import MagicMock

from src.agents.cora import queue, store
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


def _seed_draft_for(db, thread_id: str, contact_email: str) -> None:
    store.append_draft(db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=thread_id, buyer_entity_id=1,
        cell_id="founder_tier_blitz", offer="founder_tier", avenue="flippers", angle="scarcity_seat_number",
        subject="s", body="b", facts_used=[], source_refs=[], recommended_channel="email",
        confidence_score=90, contact_email=contact_email,
    ))
    store.index_contact_email(contact_email, thread_id)


def _mock_service_with_search(list_response: dict, profile_history_id: str = "999") -> MagicMock:
    service = MagicMock()
    service.users.return_value.messages.return_value.list.return_value.execute.return_value = list_response
    service.users.return_value.getProfile.return_value.execute.return_value = {"historyId": profile_history_id}
    return service


def test_not_configured_returns_zero_without_error(fresh_db, monkeypatch):
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: None)
    assert poller.poll_once(fresh_db) == 0


# ── Layer 1: watermark / fetch path selection ────────────────────────────────

def test_first_run_with_no_saved_history_id_uses_bootstrap(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-BOOT-1", "prospect@example.com")
    service = _mock_service_with_search(
        {"messages": [{"id": "msg-1"}]}, profile_history_id="hid-fresh",
    )
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-1", "prospect@example.com", "Re:", "hello"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    assert poller._get_saved_history_id() is None
    published = poller.poll_once(fresh_db)

    assert published == 1
    service.users.return_value.history.assert_not_called()
    assert poller._get_saved_history_id() == "hid-fresh"


def test_second_run_with_saved_history_id_uses_history_path(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-HIST-1", "prospect@example.com")
    poller._save_history_id("hid-existing")

    service = MagicMock()
    service.users.return_value.history.return_value.list.return_value.execute.return_value = {
        "historyId": "hid-next",
        "history": [{"messagesAdded": [{"message": {"id": "msg-2"}}]}],
    }
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-2", "prospect@example.com", "Re:", "hello again"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once(fresh_db)

    assert published == 1
    service.users.return_value.messages.return_value.list.assert_not_called()  # never used the search path
    assert poller._get_saved_history_id() == "hid-next"


def test_stale_history_cursor_falls_back_to_bootstrap(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-STALE-1", "prospect@example.com")
    poller._save_history_id("hid-stale")

    class _FakeHttpError(Exception):
        def __init__(self):
            self.resp = MagicMock(status=404)

    service = MagicMock()
    service.users.return_value.history.return_value.list.return_value.execute.side_effect = _FakeHttpError()
    service.users.return_value.messages.return_value.list.return_value.execute.return_value = {
        "messages": [{"id": "msg-3"}]
    }
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-3", "prospect@example.com", "Re:", "after the gap"
    )
    service.users.return_value.getProfile.return_value.execute.return_value = {"historyId": "hid-recovered"}
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once(fresh_db)

    assert published == 1
    assert poller._get_saved_history_id() == "hid-recovered"


def test_history_poll_paginates_across_multiple_pages(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-PAGE-1", "a@example.com")
    _seed_draft_for(fresh_db, "OPP-PAGE-2", "b@example.com")
    poller._save_history_id("hid-existing")

    service = MagicMock()
    service.users.return_value.history.return_value.list.return_value.execute.side_effect = [
        {"historyId": "hid-mid", "history": [{"messagesAdded": [{"message": {"id": "msg-a"}}]}], "nextPageToken": "p2"},
        {"historyId": "hid-final", "history": [{"messagesAdded": [{"message": {"id": "msg-b"}}]}]},
    ]

    def _get(userId, id, format):
        result = MagicMock()
        result.execute.return_value = (
            _fake_message("msg-a", "a@example.com", "Re:", "one")
            if id == "msg-a" else _fake_message("msg-b", "b@example.com", "Re:", "two")
        )
        return result

    service.users.return_value.messages.return_value.get.side_effect = _get
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once(fresh_db)

    assert published == 2
    assert poller._get_saved_history_id() == "hid-final"


# ── Layer 3: relevance filter ─────────────────────────────────────────────────

def test_unmatched_sender_is_not_queued(fresh_db, monkeypatch):
    service = _mock_service_with_search({"messages": [{"id": "msg-spam"}]})
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-spam", "spammer@eventfeeds.com", "Josh - specifics", "buy my list"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once(fresh_db)

    assert published == 0
    seen = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert seen == []


def test_matched_sender_publishes_with_resolved_thread_id(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-MATCH-1", "prospect@example.com")
    service = _mock_service_with_search({"messages": [{"id": "msg-real"}]})
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-real", "prospect@example.com", "Re: Founding seat", "Yes let's talk"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once(fresh_db)
    assert published == 1

    seen = queue.read_batch("test-consumer-2", count=10, block_ms=200)
    assert len(seen) == 1
    assert seen[0].payload["opportunity_thread_id"] == "OPP-MATCH-1"
    assert seen[0].payload["from_address"] == "prospect@example.com"


# ── Layer 4: seen-cache dedup ──────────────────────────────────────────────────

def test_already_seen_message_is_not_reprocessed(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-SEEN-1", "prospect@example.com")
    service = _mock_service_with_search({"messages": [{"id": "msg-dup"}]})
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-dup", "prospect@example.com", "Re:", "hi"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    first = poller.poll_once(fresh_db)
    assert first == 1
    queue.read_batch("drain", count=10, block_ms=200)

    # A genuine re-delivery of the same Gmail message_id — the seen-cache
    # (not the watermark) is what must catch this one.
    assert poller._process_candidate_message(service, "msg-dup", fresh_db) is False


# ── Layer 5/6: stable idempotency key ─────────────────────────────────────────

def test_idempotency_key_derived_from_stable_message_id_not_processing_time(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-IDEMP-1", "prospect@example.com")
    service = _mock_service_with_search({"messages": [{"id": "msg-stable"}]})
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-stable", "prospect@example.com", "Re:", "hello"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    poller.poll_once(fresh_db)
    published = queue.read_batch("test-consumer-3", count=10, block_ms=200)
    assert len(published) == 1
    expected_key = queue.make_idempotency_key("reply.received", "OPP-IDEMP-1", "gmail:msg-stable")
    assert published[0].idempotency_key == expected_key


# ── Resilience: one bad message never blocks the rest of the batch ──────────

def test_a_processing_failure_on_one_message_does_not_block_others(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-RESIL-1", "good@example.com")
    service = _mock_service_with_search({"messages": [{"id": "bad-1"}, {"id": "good-1"}]})

    def _get(userId, id, format):
        if id == "bad-1":
            raise RuntimeError("simulated Gmail API error")
        result = MagicMock()
        result.execute.return_value = _fake_message("good-1", "good@example.com", "Re:", "ok")
        return result

    service.users.return_value.messages.return_value.get.side_effect = _get
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)

    published = poller.poll_once(fresh_db)
    assert published == 1

    seen = queue.read_batch("test-consumer-4", count=10, block_ms=200)
    assert len(seen) == 1
    assert seen[0].payload["from_address"] == "good@example.com"


# ── PR #180 finding: a publish failure (e.g. Redis unavailable) must never be
# treated the same as "handled" — the message must not be marked seen, and
# the watermark must not advance past it, or it can never be reconsidered. ──

def test_process_candidate_message_returns_none_on_publish_failure(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-REDISDOWN-UNIT", "prospect@example.com")
    service = MagicMock()
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-unit-1", "prospect@example.com", "Re:", "hello"
    )
    monkeypatch.setattr(poller, "produce_stub_reply", lambda payload, idempotency_key=None: None)

    result = poller._process_candidate_message(service, "msg-unit-1", fresh_db)

    assert result is None
    assert poller._already_seen("msg-unit-1") is False


def test_publish_failure_holds_watermark_in_bootstrap_mode(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-REDISDOWN-BOOT", "prospect@example.com")
    service = _mock_service_with_search(
        {"messages": [{"id": "msg-boot-down"}]}, profile_history_id="hid-must-not-be-saved",
    )
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-boot-down", "prospect@example.com", "Re:", "hello"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)
    monkeypatch.setattr(poller, "produce_stub_reply", lambda payload, idempotency_key=None: None)

    assert poller._get_saved_history_id() is None
    published = poller.poll_once(fresh_db)

    assert published == 0
    # A fresh cursor must NOT be saved — doing so would skip past this
    # message on the incremental history path next poll.
    assert poller._get_saved_history_id() is None
    assert poller._already_seen("msg-boot-down") is False
    seen = queue.read_batch("test-consumer-redisdown-boot", count=10, block_ms=200)
    assert seen == []


def test_publish_failure_holds_watermark_in_history_mode(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-REDISDOWN-HIST", "prospect@example.com")
    poller._save_history_id("hid-before-failure")

    service = MagicMock()
    service.users.return_value.history.return_value.list.return_value.execute.return_value = {
        "historyId": "hid-after-failure",
        "history": [{"messagesAdded": [{"message": {"id": "msg-hist-down"}}]}],
    }
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = _fake_message(
        "msg-hist-down", "prospect@example.com", "Re:", "hello"
    )
    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)
    monkeypatch.setattr(poller, "produce_stub_reply", lambda payload, idempotency_key=None: None)

    published = poller.poll_once(fresh_db)

    assert published == 0
    # The OLD cursor must be preserved, not advanced to hid-after-failure —
    # otherwise history.list would never return this message again.
    assert poller._get_saved_history_id() == "hid-before-failure"
    assert poller._already_seen("msg-hist-down") is False


def test_one_publish_failure_does_not_block_other_messages_in_same_poll(fresh_db, monkeypatch):
    _seed_draft_for(fresh_db, "OPP-REDISDOWN-MIXED", "good@example.com")
    _seed_draft_for(fresh_db, "OPP-REDISDOWN-MIXED-2", "also-good@example.com")
    service = _mock_service_with_search(
        {"messages": [{"id": "msg-fail"}, {"id": "msg-ok"}]}, profile_history_id="hid-must-not-be-saved",
    )

    def _get(userId, id, format):
        result = MagicMock()
        result.execute.return_value = (
            _fake_message("msg-fail", "good@example.com", "Re:", "one")
            if id == "msg-fail" else _fake_message("msg-ok", "also-good@example.com", "Re:", "two")
        )
        return result

    service.users.return_value.messages.return_value.get.side_effect = _get

    def _publish_side_effect(payload, idempotency_key=None):
        if payload["from_address"] == "good@example.com":
            return None  # simulated publish failure for this one message only
        return queue.publish("reply.received", payload, idempotency_key=idempotency_key)

    monkeypatch.setattr(poller, "_build_gmail_service", lambda: service)
    monkeypatch.setattr(poller, "produce_stub_reply", _publish_side_effect)

    published = poller.poll_once(fresh_db)

    # Only the message that actually published counts, and the watermark is
    # held back for the WHOLE cycle so the failed one gets retried — the
    # already-seen guard (not the watermark) is what prevents the successful
    # one from being reprocessed on the next poll.
    assert published == 1
    assert poller._get_saved_history_id() is None
    assert poller._already_seen("msg-fail") is False
    assert poller._already_seen("msg-ok") is True


# ── MIME decoding (unchanged by the redesign) ────────────────────────────────

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
