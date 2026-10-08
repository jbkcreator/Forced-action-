"""WP-GL-10 reply agent safety net: a rate question is handed to Josh, a quoted number is flagged."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.reply_guard import (
    ReplyEvent,
    asks_rate_or_terms,
    classify,
    format_slack,
    handle_reply_event,
    parse_event,
    quotes_numbers,
)

SECRET = "test-ghl-secret"


@pytest.mark.parametrize("message", [
    "What's your rate?", "what are the points and fees", "How much will it cost me?", "is it 10% interest",
    "Can you give me a quote", "what are your terms", "need to know the APR", "how much do you charge",
    "What LTV do you go to?", "$500k loan, what would the payment be",
])
def test_rate_and_terms_questions_are_recognised(message):
    assert asks_rate_or_terms(message) is True


@pytest.mark.parametrize("message", [
    "Yes that works", "Can we do Thursday at 3?", "wrong number", "I own 412 Oak Ave", "Who is this?",
    "reschedule please", "call me tomorrow morning", "STOP",
])
def test_ordinary_replies_are_not_rate_questions(message):
    assert asks_rate_or_terms(message) is False


@pytest.mark.parametrize("reply", [
    "Rates start at 9.5%", "We charge 2 points", "That would be about $5,000", "our rate of 10 is competitive",
    "around 8 percent", "fees are $1,200",
])
def test_an_ai_reply_with_a_number_is_flagged(reply):
    assert quotes_numbers(reply) is True


@pytest.mark.parametrize("reply", [
    "Josh will go through the specifics with you. I have Thursday at 3:00 pm ET or Friday at 10:00 am ET.",
    "Your call is in about 90 minutes at 10:00 am ET. Call us at (813) 555-0100.",
    "I can get you 15 minutes with Josh about 412 Oak Ave.",
    "Rates depend on the deal, so Josh will walk you through the real numbers on a call.",
])
def test_an_ai_reply_that_hands_off_without_numbers_is_clean(reply):
    assert quotes_numbers(reply) is False


def event(**kw):
    base = dict(message_id="m1", direction="inbound", body="What's your rate?", contact_id="c1",
                first_name="Marcus", phone="+18135550147", handoff=False)
    return ReplyEvent(**{**base, **kw})


def test_classification():
    assert classify(event()) == "rate_terms_handoff"
    assert classify(event(body="Thursday works", handoff=True)) == "ai_handoff"
    assert classify(event(body="Thursday works")) is None
    assert classify(event(direction="outbound", body="Rates start at 9%")) == "ai_quoted_numbers"
    assert classify(event(direction="outbound", body="Josh will call you")) is None
    # an inbound rate question the AI also answered with a number: the outbound message is what is flagged
    assert classify(event(direction="outbound", body="Josh can walk you through the terms")) is None


def test_the_slack_post_names_the_contact_without_the_full_number():
    message = format_slack("rate_terms_handoff", event())
    assert "Marcus" in message and "…0147" in message and "+18135550147" not in message
    assert "one business hour" in message and "What's your rate?" in message


def test_a_long_message_is_truncated_in_the_post():
    assert "…" in format_slack("rate_terms_handoff", event(body="rate " + "x" * 600))


def test_parse_event_reads_the_common_ghl_shapes_and_drops_empty_ones():
    parsed = parse_event({"messageId": "m9", "body": " What are the fees? ", "direction": "inbound",
                          "contact": {"id": "c9", "firstName": "Dana", "phone": "(813) 555-0147"}})
    assert (parsed.message_id, parsed.body, parsed.contact_id, parsed.first_name, parsed.phone) == (
        "m9", "What are the fees?", "c9", "Dana", "+18135550147")
    assert parse_event({"messageId": "m9", "body": "  "}) is None
    assert parse_event({"body": "hi"}).message_id.startswith("auto-")   # no id in the body: a stable fallback, never the contact id
    assert parse_event({"messageId": "m", "body": "ok", "direction": "outbound"}).direction == "outbound"


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


class Poster:
    def __init__(self, error=None):
        self.messages, self.error = [], error

    def __call__(self, message):
        if self.error:
            raise self.error
        self.messages.append(message)


def test_a_rate_question_is_posted_once(db):
    poster = Poster()
    assert handle_reply_event(db, event(), poster=poster) == "posted"
    assert handle_reply_event(db, event(), poster=poster) == "duplicate"
    assert len(poster.messages) == 1
    row = db.execute(text("SELECT kind, contact_id, phone_hash, posted_at IS NOT NULL FROM lending.reply_handoffs")).one()
    assert row[0] == "rate_terms_handoff" and row[1] == "c1" and len(row[2]) == 12 and row[3] is True


def test_nothing_is_posted_or_stored_for_an_ordinary_reply(db):
    poster = Poster()
    assert handle_reply_event(db, event(body="Thursday works"), poster=poster) == "ignored"
    assert poster.messages == []
    assert db.execute(text("SELECT count(*) FROM lending.reply_handoffs")).scalar() == 0


def test_a_failed_post_is_retried_when_the_webhook_is_redelivered(db):
    with pytest.raises(RuntimeError):
        handle_reply_event(db, event(), poster=Poster(RuntimeError("slack down")))
    assert db.execute(text("SELECT posted_at FROM lending.reply_handoffs")).scalar() is None
    poster = Poster()
    assert handle_reply_event(db, event(), poster=poster) == "posted"
    assert len(poster.messages) == 1


def test_not_configured_slack_keeps_the_claim_for_a_later_retry(db):
    assert handle_reply_event(db, event(), poster=None) == "not_configured"
    assert handle_reply_event(db, event(), poster=Poster()) == "posted"


def test_the_message_text_is_never_stored(db):
    handle_reply_event(db, event(body="my secret rate question 123"), poster=Poster())
    stored = db.execute(text("SELECT row_to_json(r)::text FROM lending.reply_handoffs r")).scalar()
    assert "secret" not in stored and "+1813" not in stored


# ── the webhook ──────────────────────────────────────────────────────────────

@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.lending import reply_webhook
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(reply_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    client = TestClient(app)
    client.posted = []
    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: client.posted.append)
    return client


def post(client, body, secret=SECRET):
    return client.post("/webhooks/lending/ghl-reply", headers={"X-Webhook-Secret": secret} if secret else {}, json=body)


def test_the_webhook_posts_a_rate_question_and_ignores_the_rest(client):
    body = {"messageId": "m1", "body": "What's your rate?", "direction": "inbound", "contact": {"id": "c1", "firstName": "Marcus"}}
    assert post(client, body).json() == {"status": "posted"}
    assert post(client, body).json() == {"status": "duplicate"}
    assert post(client, {"messageId": "m2", "body": "Thursday works"}).json() == {"status": "ignored"}
    assert post(client, {"messageId": "m3"}).json()["status"] == "ignored"
    assert len(client.posted) == 1


def test_the_webhook_alerts_when_the_ai_quotes_a_number(client):
    post(client, {"messageId": "m4", "body": "Rates start at 9.5%", "direction": "outbound"})
    assert "must not quote" in client.posted[0]


def test_the_webhook_needs_the_secret(client):
    assert post(client, {"messageId": "m1", "body": "rate?"}, secret="nope").status_code == 401
    assert post(client, {"messageId": "m1", "body": "rate?"}, secret=None).status_code == 401
    assert client.posted == []


def test_a_failed_slack_post_returns_an_error_so_ghl_retries(client, monkeypatch):
    from src.lending import reply_webhook

    def boom(message):
        raise RuntimeError("slack down")

    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: boom)
    response = post(client, {"messageId": "m1", "body": "What's your rate?"})
    assert response.status_code == 502 and "slack" not in response.text.lower()
