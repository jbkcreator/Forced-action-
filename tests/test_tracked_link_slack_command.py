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

from sqlalchemy import text as sa_text

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


# ---------------------------------------------------------------------------
# Borrower-identity matching (WI-1 follow-up, 2026-09-18) — name match,
# mailing-address disambiguation, and the transparent Slack reply.
# ---------------------------------------------------------------------------


def _insert_buyer_entity(db, name: str, mailing_address, purchase_count: int = 3, entity_type: str = "Individual") -> int:
    row = db.execute(
        sa_text(
            "INSERT INTO buyer_entities (canonical_name, entity_type, primary_mailing_address, "
            "confidence_score, verification_status, total_purchase_count, total_cash_volume) "
            "VALUES (:name, :etype, :addr, 90, 'verified', :count, 600000) RETURNING id"
        ),
        {"name": name, "etype": entity_type, "addr": mailing_address, "count": purchase_count},
    ).first()
    assert row is not None
    return row.id


def test_unique_name_match_binds_buyer_entity_and_is_transparent(fresh_db):
    name = f"{_TAG} SOLO SMITH"
    entity_id = _insert_buyer_entity(fresh_db, name, "611 S FT HARRISON AVE, CLEARWATER, FL, 33756")
    fresh_db.flush()

    reply = build_tracked_link_reply(fresh_db, f"partner {name} Referral | {name}", "josh")
    assert "Created:" in reply["text"]
    assert "Borrower recognized" in reply["text"]
    assert name in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] == entity_id


def test_natural_word_order_matches_county_last_first_format(fresh_db):
    """County records store an individual as 'LAST, FIRST' (verified against
    real samples — see tracked_links._name_variants docstring). Josh typing
    the name the natural way ('First Last') must still find it — this was a
    real gap: a plain exact match against canonical_name alone silently
    found nothing for the overwhelmingly common case."""
    stored_name = f"{_TAG}SMITH, {_TAG}JOHN"
    entity_id = _insert_buyer_entity(fresh_db, stored_name, "1 Test Way, Tampa, FL, 33602")
    fresh_db.flush()

    reply = build_tracked_link_reply(
        fresh_db, f"partner Referral | {_TAG}JOHN {_TAG}SMITH", "josh"
    )
    assert "Borrower recognized" in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] == entity_id


def test_mailing_address_disambiguation_tolerates_directional_mismatch(fresh_db):
    """A plain contiguous-substring check would miss '123 Main St' against a
    stored '123 N MAIN ST' — the directional sits in between and breaks
    contiguity. Token-set matching (order-independent, directionals
    dropped) must still resolve this — a real gap found and fixed
    2026-09-18."""
    name = f"{_TAG} DIRECTIONAL JONES"
    wrong_id = _insert_buyer_entity(fresh_db, name, "1 First St, Tampa, FL, 33602")
    right_id = _insert_buyer_entity(fresh_db, name, "456 N Main St, Tampa, FL, 33603")
    fresh_db.flush()

    reply = build_tracked_link_reply(
        fresh_db, f"partner Referral | {name} | 456 Main St", "josh"
    )
    assert "Borrower recognized" in reply["text"]
    assert "Disambiguated using mailing address" in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] == right_id
    assert link_row["buyer_entity_id"] != wrong_id


def test_joint_owner_name_matches_by_either_persons_name(fresh_db):
    """23.1% of buyer_entities rows join two people with 'AND' (checked
    empirically 2026-09-18) — Josh must be able to match on either
    person's own name, not only the exact full joined string."""
    joint_name = f"YIBO {_TAG}FEN AND LANJU {_TAG}KANG"
    entity_id = _insert_buyer_entity(fresh_db, joint_name, "1 Test Way, Tampa, FL, 33602")
    fresh_db.flush()

    reply = build_tracked_link_reply(fresh_db, f"partner Referral | Lanju {_TAG}KANG", "josh")
    assert "Borrower recognized" in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] == entity_id


def test_label_defaults_to_name_when_label_omitted(fresh_db):
    name = f"{_TAG} DEFAULT LABEL CO"
    _insert_buyer_entity(fresh_db, name, "1 Test Way, Tampa, FL, 33602")
    fresh_db.flush()

    reply = build_tracked_link_reply(fresh_db, f"partner | {name}", "josh")
    assert "Created:" in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT label FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["label"] == name


def test_no_label_and_no_name_is_rejected(fresh_db):
    reply = build_tracked_link_reply(fresh_db, "partner |", "josh")
    assert "Need a label or a borrower name" in reply["text"]


def test_name_not_found_is_generic_and_transparent(fresh_db):
    reply = build_tracked_link_reply(fresh_db, f"partner {_TAG} Co | {_TAG} Nobody Real", "josh")
    assert "Created:" in reply["text"]
    assert "No borrower found matching" in reply["text"]
    # Regression guard (2026-09-18): this message used to end with a
    # copy/pasted "; property/address not in our records" clause left over
    # from the address-matching feature — nonsensical here since this is a
    # pure NAME lookup miss, nothing to do with a property or address.
    assert "property" not in reply["text"].lower()
    assert "address" not in reply["text"].lower()

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] is None


def test_ambiguous_name_without_mailing_address_stays_unbound(fresh_db):
    name = f"{_TAG} AMBIGUOUS JONES"
    _insert_buyer_entity(fresh_db, name, "1 First St, Tampa, FL, 33602")
    _insert_buyer_entity(fresh_db, name, "2 Second St, Tampa, FL, 33602")
    fresh_db.flush()

    reply = build_tracked_link_reply(fresh_db, f"partner {_TAG} Co | {name}", "josh")
    assert "Created:" in reply["text"]
    assert "more than one borrower" in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] is None


def test_ambiguous_name_disambiguated_by_mailing_address(fresh_db):
    name = f"{_TAG} DISAMBIG JONES"
    _insert_buyer_entity(fresh_db, name, "611 S FT HARRISON AVE, CLEARWATER, FL, 33756")
    right_id = _insert_buyer_entity(fresh_db, name, "9916 DAFFODIL ST UNIT 54, PINELLAS PARK, FL, 33782")
    fresh_db.flush()

    reply = build_tracked_link_reply(
        fresh_db, f"partner {_TAG} Co | {name} | 9916 Daffodil St", "josh"
    )
    assert "Created:" in reply["text"]
    assert "Borrower recognized" in reply["text"]
    assert "Disambiguated using mailing address" in reply["text"]

    slug = reply["text"].split("/go/")[1].split()[0].strip()
    link_row = fresh_db.execute(
        sa_text("SELECT buyer_entity_id FROM tracked_links WHERE slug = :slug"), {"slug": slug}
    ).mappings().first()
    assert link_row["buyer_entity_id"] == right_id


def test_property_mailer_kind_no_longer_valid(fresh_db):
    """Removed 2026-09-18: physical mail campaigns are out of scope for this
    client and 'property_mailer' was never in the spec (only
    partner/source/campaign are named in forced-action-max-amendment-1-
    detail.md item 20) — it was an unconfirmed engineering addition."""
    reply = build_tracked_link_reply(fresh_db, f"property_mailer {_TAG} 123 Main St", "josh")
    assert "Unknown kind" in reply["text"]


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
    """The 3-second ack is bare (no payload) — the real reply, including a
    refusal, is delivered separately via response_url so the handler never
    races Slack's ack window with DB work (see WI-1 fix, 2026-09-18: local
    testing showed 'app did not respond' because the old code built the
    reply, including a DB roundtrip, before acking at all)."""
    monkeypatch.setattr(global_settings, "fa_max_slack_channel_relationships", _ALLOWED_CHANNEL)
    posted = {}
    monkeypatch.setattr(
        "src.utils.http_helpers.requests_post_with_retry",
        lambda url, **kw: posted.update(url=url, json=kw.get("json")) or SimpleNamespace(status_code=200, text="ok"),
    )

    client = MagicMock()
    request = SimpleNamespace(
        envelope_id="env-4", type="slash_commands",
        payload={"command": "/tracked-link", "text": f"partner {_TAG} wrong channel", "user_name": "josh",
                  "channel_id": "C_SOME_OTHER_CHANNEL", "response_url": "https://hooks.slack.test/commands/env-4"},
    )

    assert handle_tracked_link_socket_request(client, request) is True  # handled — we responded, just refused

    client.send_socket_mode_response.assert_called_once()
    ack = client.send_socket_mode_response.call_args[0][0]
    assert ack.envelope_id == "env-4"
    assert ack.payload is None

    assert posted["url"] == "https://hooks.slack.test/commands/env-4"
    assert "fa-max-relationships" in posted["json"]["text"]


def test_socket_handler_handles_tracked_link_command(monkeypatch):
    monkeypatch.setattr(global_settings, "fa_max_slack_channel_relationships", _ALLOWED_CHANNEL)
    posted = {}
    monkeypatch.setattr(
        "src.utils.http_helpers.requests_post_with_retry",
        lambda url, **kw: posted.update(url=url, json=kw.get("json")) or SimpleNamespace(status_code=200, text="ok"),
    )

    client = MagicMock()
    request = SimpleNamespace(
        envelope_id="env-3", type="slash_commands",
        payload={"command": "/tracked-link", "text": f"partner {_TAG} socket test", "user_name": "josh",
                  "channel_id": _ALLOWED_CHANNEL, "response_url": "https://hooks.slack.test/commands/env-3"},
    )

    assert handle_tracked_link_socket_request(client, request) is True

    client.send_socket_mode_response.assert_called_once()
    ack = client.send_socket_mode_response.call_args[0][0]
    assert ack.envelope_id == "env-3"
    assert ack.payload is None

    assert posted["url"] == "https://hooks.slack.test/commands/env-3"
    assert posted["json"]["text"].startswith("Created:")

    with get_db_context() as db:
        from sqlalchemy import text as sa_text
        db.execute(sa_text("DELETE FROM tracked_links WHERE created_by = 'slack:josh' AND label LIKE :pat"),
                   {"pat": f"%{_TAG}%"})
        db.commit()


def test_socket_handler_logs_and_drops_reply_when_response_url_missing(monkeypatch, caplog):
    """No response_url on the envelope means Slack gave us nothing to post
    the reply to — the handler must still ack (never leaves Slack hanging)
    and just log the drop instead of raising."""
    monkeypatch.setattr(global_settings, "fa_max_slack_channel_relationships", _ALLOWED_CHANNEL)

    client = MagicMock()
    request = SimpleNamespace(
        envelope_id="env-5", type="slash_commands",
        payload={"command": "/tracked-link", "text": f"partner {_TAG} no response url", "user_name": "josh",
                  "channel_id": _ALLOWED_CHANNEL},
    )

    with caplog.at_level("WARNING"):
        assert handle_tracked_link_socket_request(client, request) is True

    client.send_socket_mode_response.assert_called_once()
    assert "response_url" in caplog.text

    with get_db_context() as db:
        from sqlalchemy import text as sa_text
        db.execute(sa_text("DELETE FROM tracked_links WHERE created_by = 'slack:josh' AND label LIKE :pat"),
                   {"pat": f"%{_TAG}%"})
        db.commit()
