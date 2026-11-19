"""Dev 1 cold-email and auto-send rules: only NDL campaigns reach Cora, every opt-out goes onto Instantly's block list,
the unsubscribe link lives on NDL's own host, and a graduated message sends only wording Josh approved."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from config.lending_compliance import UNSUBSCRIBE_PATH
from src.api import lending_email_unsubscribe_router as unsub_router
from src.lending import compliance, standing_sends
from src.services.email_unsubscribe import mint_unsubscribe_token
from tests.lending.test_instantly_replies import client, post, reply_body  # noqa: F401  (client is a fixture)

LEAD = "mark.lee@example.com"


# ── only NDL campaigns reach Cora ───────────────────────────────────────────

def _campaigns(monkeypatch, main="camp-ndl", extra=""):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_instantly_campaign_id", main, raising=False)
    monkeypatch.setattr(get_settings(), "lending_instantly_extra_campaign_ids", extra, raising=False)


def _stored(client) -> bool:
    return any("INSERT INTO lending.instantly_replies" in sql for sql, _ in client.db.executed)


def test_a_reply_from_another_campaign_is_ignored_and_not_stored(client, monkeypatch):
    _campaigns(monkeypatch)
    assert post(client, reply_body(campaign_id="camp-forced-action")).json() == {"status": "ignored"}
    assert not _stored(client)


def test_an_unsubscribe_from_another_campaign_is_ignored_too(client, monkeypatch):
    _campaigns(monkeypatch)
    body = {"event_type": "lead_unsubscribed", "lead_email": LEAD, "campaign_id": "camp-forced-action"}
    assert post(client, body).json() == {"status": "ignored"}
    assert not _stored(client)


def test_a_reply_from_the_ndl_campaign_or_an_extra_one_is_handled(client, monkeypatch):
    _campaigns(monkeypatch, extra="camp-ndl-2, camp-ndl-3")
    for campaign in ("camp-ndl", "camp-ndl-3"):
        client.db.executed.clear()
        post(client, reply_body(campaign_id=campaign, email_id=f"em-{campaign}"))
        assert _stored(client), campaign


def test_an_event_that_names_no_campaign_is_accepted_so_a_stop_is_never_dropped(client, monkeypatch):
    _campaigns(monkeypatch)
    body = reply_body()
    body.pop("campaign_id", None)
    post(client, body)
    assert _stored(client)


def test_the_campaign_id_is_compared_case_insensitively(client, monkeypatch):
    _campaigns(monkeypatch, main="Camp-NDL-ABC")
    post(client, reply_body(campaign_id=" camp-ndl-abc "))
    assert _stored(client)


def test_with_no_ndl_campaign_configured_nothing_is_filtered(client, monkeypatch):
    _campaigns(monkeypatch, main="")
    post(client, reply_body(campaign_id="anything"))
    assert _stored(client)


# ── every opt-out goes onto Instantly's block list ──────────────────────────

def test_every_opted_out_address_is_added_to_the_block_list(monkeypatch):
    blocked = []
    monkeypatch.setattr("src.lending.instantly_client.lending_instantly_account", lambda: object())
    monkeypatch.setattr("src.lending.instantly_client.block_address", lambda account, email: blocked.append(email) or True)
    compliance._block_in_instantly(["a@x.com", "b@x.com"])
    assert blocked == ["a@x.com", "b@x.com"]


def test_without_an_instantly_account_the_block_list_step_is_a_no_op(monkeypatch):
    called = []
    monkeypatch.setattr("src.lending.instantly_client.lending_instantly_account", lambda: None)
    monkeypatch.setattr("src.lending.instantly_client.block_address", lambda *a: called.append(a))
    compliance._block_in_instantly(["a@x.com"])
    assert called == []


def test_a_block_list_failure_never_raises_and_the_rest_are_still_tried(monkeypatch):
    seen = []

    def block(account, email):
        seen.append(email)
        if email == "a@x.com":
            raise RuntimeError("boom for a@x.com")
        return True

    monkeypatch.setattr("src.lending.instantly_client.lending_instantly_account", lambda: object())
    monkeypatch.setattr("src.lending.instantly_client.block_address", block)
    compliance._block_in_instantly(["a@x.com", "b@x.com"])
    assert seen == ["a@x.com", "b@x.com"]


def test_the_block_list_step_is_bounded_in_count_and_time(monkeypatch):
    seen = []
    monkeypatch.setattr("src.lending.instantly_client.lending_instantly_account", lambda: object())
    monkeypatch.setattr("src.lending.instantly_client.block_address", lambda account, email: seen.append(email) or True)
    compliance._block_in_instantly([f"u{i}@x.com" for i in range(50)])
    assert len(seen) == compliance.BLOCKLIST_MAX_PER_BATCH
    seen.clear()
    monkeypatch.setattr(compliance, "BLOCKLIST_BUDGET_SECONDS", -1.0)
    compliance._block_in_instantly(["a@x.com"])
    assert seen == []


def test_propagate_adds_every_opted_out_email_to_the_block_list_on_postgres(lending_db, monkeypatch):
    from sqlalchemy import text

    from config.lending_compliance import OptOutChannel
    try:
        lending_db.execute(text("SELECT 1 FROM lending.opt_out_events LIMIT 1"))
        lending_db.execute(text("SELECT 1 FROM subscribers LIMIT 1"))
    except Exception:
        lending_db.rollback()
        pytest.skip("the test database lacks the opt-out or subscriber tables")
    blocked = []
    monkeypatch.setattr(compliance, "_block_in_instantly", lambda emails: blocked.extend(emails))
    compliance.propagate_opt_out(lending_db, email="Block.Me@Example.com", channel=OptOutChannel.EMAIL,
                                 actor="test", source_ref="dev1-test:1")
    assert blocked == ["block.me@example.com"]


# ── the unsubscribe link on NDL's own host ──────────────────────────────────

@pytest.fixture
def unsub_client(monkeypatch):
    from src.api.deps import get_db
    app = FastAPI()
    app.include_router(unsub_router.router)
    calls = []
    app.dependency_overrides[get_db] = lambda: SimpleNamespace(commit=lambda: None, rollback=lambda: None)
    monkeypatch.setattr(unsub_router, "propagate_opt_out", lambda db, **kw: calls.append(kw) or 1)
    monkeypatch.setattr(unsub_router, "verify_unsubscribe_token", lambda token: LEAD if token == "good" else None)
    monkeypatch.setattr(unsub_router, "enforce_or_429", lambda *a, **k: None)
    test_client = TestClient(app)
    test_client.calls = calls
    return test_client


def test_opening_the_link_only_shows_a_button_and_opts_nobody_out(unsub_client):
    response = unsub_client.get(f"{UNSUBSCRIBE_PATH}?token=good")
    assert response.status_code == 200 and 'method="post"' in response.text and "Unsubscribe" in response.text
    assert unsub_client.calls == []  # a scanner or prefetcher opening the link changes nothing


def test_pressing_the_button_or_a_one_click_post_opts_the_address_out_on_every_channel(unsub_client):
    response = unsub_client.post(f"{UNSUBSCRIBE_PATH}?token=good")
    assert response.status_code == 200 and "Next Deal Lending" in response.text
    call = unsub_client.calls[0]
    assert call["email"] == LEAD and call["channel"].value == "email"
    assert call["source_ref"].startswith("unsubscribe-link:") and LEAD not in call["source_ref"]


def test_a_bad_token_is_refused_on_both_verbs_and_opts_nobody_out(unsub_client):
    assert unsub_client.get(f"{UNSUBSCRIBE_PATH}?token=forged").status_code == 400
    assert unsub_client.post(f"{UNSUBSCRIBE_PATH}?token=forged").status_code == 400
    assert unsub_client.calls == []


def test_a_missing_signing_secret_is_a_clean_refusal_not_a_500(unsub_client, monkeypatch):
    def no_secret(token):
        raise RuntimeError("Unsubscribe tokens need SUBSCRIBER_JWT_SECRET")

    monkeypatch.setattr(unsub_router, "verify_unsubscribe_token", no_secret)
    assert unsub_client.get(f"{UNSUBSCRIBE_PATH}?token=good").status_code == 400
    assert unsub_client.post(f"{UNSUBSCRIBE_PATH}?token=good").status_code == 400


def test_the_post_is_rate_limited(unsub_client, monkeypatch):
    from fastapi import HTTPException

    def limited(*a, **k):
        raise HTTPException(status_code=429, detail="slow down")

    monkeypatch.setattr(unsub_router, "enforce_or_429", limited)
    assert unsub_client.post(f"{UNSUBSCRIBE_PATH}?token=good").status_code == 429
    assert unsub_client.calls == []


def test_a_failed_opt_out_asks_the_person_to_try_again_without_leaking_the_error(unsub_client, monkeypatch):
    def boom(db, **kw):
        raise RuntimeError("db down for mark.lee@example.com")

    monkeypatch.setattr(unsub_router, "propagate_opt_out", boom)
    response = unsub_client.post(f"{UNSUBSCRIBE_PATH}?token=good")
    assert response.status_code == 503 and "mark.lee@example.com" not in response.text


def test_the_signed_token_round_trips_through_the_real_verifier(monkeypatch):
    from pydantic import SecretStr

    from config.settings import get_settings
    from src.services.email_unsubscribe import verify_unsubscribe_token
    monkeypatch.setattr(get_settings(), "subscriber_jwt_secret", SecretStr("x" * 32), raising=False)
    assert verify_unsubscribe_token(mint_unsubscribe_token(LEAD)) == LEAD


def test_the_route_is_mounted_on_lending_api():
    from src.api.deps import get_db
    from src.lending.api import app
    app.dependency_overrides[get_db] = lambda: SimpleNamespace(commit=lambda: None, rollback=lambda: None)
    try:
        assert TestClient(app).get(f"{UNSUBSCRIBE_PATH}?token=forged").status_code == 400  # reached, token refused
    finally:
        app.dependency_overrides.pop(get_db, None)


# ── a graduated message sends only approved wording ─────────────────────────

def test_wording_key_ignores_names_numbers_case_and_spacing():
    a = standing_sends.wording_key({"body": "Hi Mark,  your $250,000 loan in Tampa is ready."}, ["Mark", "Tampa"])
    b = standing_sends.wording_key({"body": "hi SARAH, your $410000 loan in Miami is ready."}, ["Sarah", "Miami"])
    assert a == b


def test_any_other_change_of_words_changes_the_key():
    a = standing_sends.wording_key({"body": "Hi Mark, your loan is ready."}, ["Mark"])
    assert a != standing_sends.wording_key({"body": "Hi Mark, your loan is ready. Call now for a special rate."}, ["Mark"])
    assert a != standing_sends.wording_key({"body": "Hi Mark, your funding is ready."}, ["Mark"])


def test_subject_and_body_both_count():
    base = {"subject": "Your loan", "body": "Hi Mark"}
    assert standing_sends.wording_key(base) != standing_sends.wording_key({**base, "subject": "Rates are falling"})


class _Conn:
    def __init__(self, payloads, fail=False):
        self._payloads, self._fail, self.params = payloads, fail, []

    def execute(self, stmt, params):
        if self._fail:
            raise RuntimeError("db down")
        self.params.append(params)
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: self._payloads))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _ok(payloads, fail=False):
    return standing_sends.make_wording_ok(SimpleNamespace(connect=lambda: _Conn(payloads, fail)))


APPROVED = {"body": "Hi Mark, quick question about your Tampa project. Any questions?", "template_version": "LF-D2-SMS-v1",
            "contact_label": "Mark T., Tampa"}


def test_the_same_wording_with_other_names_matches_an_approved_card():
    assert _ok([APPROVED])("LF-D2-SMS-v1", "ghl_sms",
                           {"body": "Hi Sarah, quick question about your Miami project. Any questions?"},
                           ["Sarah", "Miami"]) is True


def test_reworded_text_does_not_match_so_it_gets_a_card():
    assert _ok([APPROVED])("LF-D2-SMS-v1", "ghl_sms", {"body": "Hi Sarah, act now for our lowest rate ever."},
                           ["Sarah"]) is False


def test_an_approved_card_without_a_contact_label_cannot_be_compared_so_it_fails_closed():
    old_card = {"body": APPROVED["body"], "template_version": "LF-D2-SMS-v1"}
    assert _ok([old_card])("LF-D2-SMS-v1", "ghl_sms",
                           {"body": "Hi Sarah, quick question about your Miami project. Any questions?"},
                           ["Sarah", "Miami"]) is False


def test_no_approved_card_yet_means_no_match():
    assert _ok([])("LF-D2-SMS-v1", "ghl_sms", {"body": "Hi Mark"}, ["Mark"]) is False


def test_an_unreadable_answer_fails_closed():
    assert _ok([APPROVED], fail=True)("LF-D2-SMS-v1", "ghl_sms", {"body": APPROVED["body"]}, ["Mark"]) is False


def test_an_empty_message_never_matches():
    assert _ok([{"body": ""}])("LF-D2-SMS-v1", "ghl_sms", {"body": ""}, []) is False


def test_the_wording_check_is_scoped_to_the_template_and_channel_and_to_person_approved_cards():
    conn = _Conn([APPROVED])
    standing_sends.make_wording_ok(SimpleNamespace(connect=lambda: conn))("LF-D2-SMS-v1", "ghl_sms", APPROVED, ["Mark"])
    assert conn.params[0]["template_version"] == "LF-D2-SMS-v1" and conn.params[0]["channel"] == "ghl_sms"
    assert conn.params[0]["auto_prefix"] == "auto:%"


def test_the_switch_is_off_by_default():
    from config.settings import get_settings
    assert get_settings().lending_graduated_approved_wording_only is False
