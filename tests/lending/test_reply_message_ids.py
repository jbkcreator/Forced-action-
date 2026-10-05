"""GHL's default workflow body has no message id and its top-level "id" is the contact's id: never use it as one."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.lending.reply_guard import parse_event

NOW = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)


def default_body(text="What's your rate?", contact="c1"):
    return {"id": contact, "contact_id": contact, "first_name": "Marcus", "phone": "(813) 555-0147",
            "message": {"type": 2, "body": text, "direction": "inbound"}}


def test_a_top_level_id_is_never_the_message_id():
    event = parse_event(default_body(), now=NOW)
    assert event.message_id != "c1" and event.message_id.startswith("auto-")
    assert (event.contact_id, event.first_name, event.body) == ("c1", "Marcus", "What's your rate?")


def test_two_different_messages_from_one_contact_get_different_ids():
    first = parse_event(default_body("What's your rate?"), now=NOW)
    second = parse_event(default_body("How many points?"), now=NOW)
    assert first.message_id != second.message_id


def test_a_redelivery_maps_to_the_same_id_but_the_same_words_later_do_not():
    first = parse_event(default_body(), now=NOW)
    assert parse_event(default_body(), now=NOW + timedelta(minutes=2)).message_id == first.message_id
    assert parse_event(default_body(), now=NOW + timedelta(hours=1)).message_id != first.message_id


def test_a_real_message_id_wins():
    body = {**default_body(), "messageId": "m-42"}
    assert parse_event(body, now=NOW).message_id == "m-42"
    nested = {"contact_id": "c1", "message": {"id": "m-43", "body": "rate?"}}
    assert parse_event(nested, now=NOW).message_id == "m-43"


def test_a_body_with_no_text_is_still_ignored():
    assert parse_event({"messageId": "m", "contact_id": "c1"}, now=NOW) is None
    assert parse_event({"messageId": "m", "message": {"body": "  "}}, now=NOW) is None
