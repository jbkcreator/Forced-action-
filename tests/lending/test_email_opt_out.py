"""Opt-out mailbox listener: which messages count, what gets suppressed, and that nothing repeats.

No mailbox and no database: messages are built in memory and the opt-out recording is scripted.
"""
from __future__ import annotations

from contextlib import contextmanager
from email.message import EmailMessage
from types import SimpleNamespace

import pytest

from src.lending import email_opt_out
from src.lending.email_opt_out import EmailOptOut, parse_opt_out

OPTOUT = "optout@nextdeallending.com"
OWN = frozenset({"+17274369951", "+16562211100"})


def _raw(*, to: str = OPTOUT, sender: str = "Sam Lee <Sam.Lee@Example.com>", subject: str = "Unsubscribe",
         body: str = "Please remove me.", message_id: str = "<m1@example.com>", html: bool = False,
         **headers: str) -> bytes:
    message = EmailMessage()
    message["From"], message["To"], message["Subject"], message["Message-ID"] = sender, to, subject, message_id
    for name, value in headers.items():
        message[name.replace("_", "-")] = value
    if html:
        message.set_content(body, subtype="html")
    else:
        message.set_content(body)
    return message.as_bytes()


def _parse(raw: bytes) -> EmailOptOut | None:
    return parse_opt_out(raw, optout_address=OPTOUT, own_numbers=OWN)


def test_a_message_to_the_opt_out_address_is_an_opt_out():
    opt_out = _parse(_raw())
    assert opt_out is not None and opt_out.sender == "sam.lee@example.com" and opt_out.phones == ()


@pytest.mark.parametrize("headers", [
    {"to": "hello@nextdeallending.com", "Delivered_To": OPTOUT},       # arrived through the optout@ forward
    {"to": "hello@nextdeallending.com", "X_Original_To": OPTOUT},
    {"to": "someone@else.com", "Cc": "OptOut@NextDealLending.com"},
])
def test_the_opt_out_address_is_found_in_any_recipient_header(headers):
    assert _parse(_raw(**headers)) is not None


def test_other_mail_in_a_shared_inbox_is_ignored():
    assert _parse(_raw(to="hello@nextdeallending.com", subject="A2P campaign approved")) is None


@pytest.mark.parametrize("sender, headers", [
    ("MAILER-DAEMON@mail.example.com", {}),
    ("postmaster@example.com", {}),
    ("sam@example.com", {"Auto_Submitted": "auto-replied"}),
])
def test_bounces_and_auto_replies_are_not_opt_outs(sender, headers):
    assert _parse(_raw(sender=sender, **headers)) is None


def test_reply_to_cannot_redirect_the_opt_out_to_someone_else():
    opt_out = _parse(_raw(sender="sam@example.com", Reply_To="victim@example.com"))
    assert opt_out.sender == "sam@example.com"


def test_phone_numbers_in_the_message_are_suppressed_but_never_our_own():
    body = ("Stop texting 813-555-0123 and (727) 555-0188 please.\n"
            "> On Mon, Next Deal Lending (727) 436-9951 wrote: call us at +1 656.221.1100")
    assert _parse(_raw(body=body)).phones == ("+17275550188", "+18135550123")


def test_phone_numbers_in_the_subject_and_html_bodies_are_found():
    raw = _raw(subject="Remove 8135550123", body="<p>Also <b>813.555.0199</b></p>", html=True)
    assert _parse(raw).phones == ("+18135550123", "+18135550199")


def test_the_key_is_stable_for_the_same_message_and_short_enough_to_store():
    first, again = _parse(_raw()), _parse(_raw(subject="Different subject, same Message-ID"))
    assert first.key == again.key and len(email_opt_out.SOURCE_REF_PREFIX + first.key) <= 100
    assert _parse(_raw(message_id="<m2@example.com>")).key != first.key


@contextmanager
def _session():
    yield SimpleNamespace()


class FakeMailbox:
    """An inbox of raw messages; ``matching_ids`` filters by recipient like the IMAP search does."""

    def __init__(self, messages: list[bytes]) -> None:
        self.messages = messages
        self.full_fetches: list[bytes] = []

    def _message(self, number: bytes) -> bytes:
        return self.messages[int(number) - 1]

    def matching_ids(self, since) -> list[bytes]:
        return [str(i).encode() for i, raw in enumerate(self.messages, start=1) if OPTOUT.encode() in raw.lower()]

    def header_key(self, number: bytes) -> str:
        raw = self._message(number)
        return email_opt_out.header_key(raw.split(b"\n\n", 1)[0])

    def raw(self, number: bytes) -> bytes:
        self.full_fetches.append(number)
        return self._message(number)


class MemoryLog:
    """In-memory stand-in for lending.email_opt_out_messages."""

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def settled(self, keys):
        return {key for key in keys if key in self.rows and self.rows[key]["outcome"] != "failed"}

    def mark(self, key, outcome, *, event_id=None, error=None):
        row = self.rows.setdefault(key, {"attempts": 1})
        row.update(outcome=outcome, event_id=event_id)

    def mark_failed(self, key, error):
        row = self.rows.setdefault(key, {"attempts": 0})
        row["attempts"] += 1
        row["outcome"] = "failed"
        return row["attempts"]


def _recorder(monkeypatch):
    """record_opt_out scripted: a new id per new key, None for one already recorded."""
    recorded: dict[str, int] = {}

    def fake_record(db, opt_out):
        if opt_out.key in recorded:
            return None
        recorded[opt_out.key] = len(recorded) + 1
        return recorded[opt_out.key]

    monkeypatch.setattr(email_opt_out, "record_opt_out", fake_record)
    return recorded


def _sweep(mailbox, log, alerts, limit=email_opt_out.MAX_MESSAGES_PER_RUN):
    return email_opt_out.sweep(mailbox, log, since=None, optout_address=OPTOUT, own_numbers=OWN,
                               session_factory=_session, notify=alerts.append, limit=limit)


def test_each_new_opt_out_is_recorded_once_and_alerted_without_pii(monkeypatch):
    _recorder(monkeypatch)
    alerts: list[str] = []
    mailbox, log = FakeMailbox([_raw(body="Remove 813-555-0123"), _raw(to="hello@nextdeallending.com")]), MemoryLog()
    assert _sweep(mailbox, log, alerts).recorded == 1
    assert _sweep(mailbox, log, alerts).recorded == 0  # settled: not fetched again
    assert len(alerts) == 1 and "s***@example.com" in alerts[0] and "1 phone number" in alerts[0]
    assert "sam.lee@example.com" not in alerts[0].lower() and "555" not in alerts[0]
    assert mailbox.full_fetches == [b"1"]


def test_an_opt_out_followed_by_200_other_messages_is_still_recorded(monkeypatch):
    """Other mail never competes for the sweep: the search returns only opt-out mail."""
    _recorder(monkeypatch)
    others = [_raw(to="hello@nextdeallending.com", message_id=f"<other{i}@example.com>") for i in range(200)]
    mailbox, log = FakeMailbox([_raw(message_id="<optout@example.com>"), *others]), MemoryLog()
    assert _sweep(mailbox, log, []).recorded == 1


def test_a_backlog_larger_than_one_batch_is_cleared_over_later_sweeps(monkeypatch):
    """Settled messages are dropped before the batch is taken, so later opt-outs are never crowded out."""
    recorded = _recorder(monkeypatch)
    messages = [_raw(sender=f"user{i}@example.com", message_id=f"<m{i}@example.com>") for i in range(250)]
    mailbox, log = FakeMailbox(messages), MemoryLog()
    assert _sweep(mailbox, log, [], limit=200).recorded == 200
    assert _sweep(mailbox, log, [], limit=200).recorded == 50
    assert _sweep(mailbox, log, [], limit=200).examined == 0
    assert len(recorded) == 250


def test_a_failed_opt_out_is_retried_on_later_sweeps_then_alerted(monkeypatch):
    calls: list[str] = []

    def failing(db, opt_out):
        calls.append(opt_out.key)
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(email_opt_out, "record_opt_out", failing)
    monkeypatch.setattr(email_opt_out, "MAX_MESSAGE_ATTEMPTS", 3)
    alerts: list[str] = []
    mailbox, log = FakeMailbox([_raw()]), MemoryLog()
    for _ in range(4):
        _sweep(mailbox, log, alerts)
    assert len(calls) == 3  # retried until the limit, then settled as given up
    assert len(alerts) == 1 and "NOT recorded" in alerts[0] and "sam.lee@example.com" not in alerts[0].lower()
    assert next(iter(log.rows.values()))["outcome"] == "gave_up"


def test_a_failure_on_one_message_does_not_stop_the_rest(monkeypatch):
    calls: list[str] = []

    def flaky_record(db, opt_out):
        calls.append(opt_out.key)
        if len(calls) == 1:
            raise RuntimeError("database unavailable")
        return 7

    monkeypatch.setattr(email_opt_out, "record_opt_out", flaky_record)
    alerts: list[str] = []
    mailbox, log = FakeMailbox([_raw(), _raw(message_id="<m2@example.com>")]), MemoryLog()
    result = _sweep(mailbox, log, alerts)
    assert result.recorded == 1 and result.failed == 1 and len(alerts) == 1


def test_an_unreadable_message_is_alerted_once_and_not_retried(monkeypatch):
    _recorder(monkeypatch)
    monkeypatch.setattr(email_opt_out, "parse_opt_out", lambda raw, **kwargs: (_ for _ in ()).throw(ValueError()))
    alerts: list[str] = []
    mailbox, log = FakeMailbox([_raw()]), MemoryLog()
    _sweep(mailbox, log, alerts)
    _sweep(mailbox, log, alerts)
    assert len(alerts) == 1 and "could not be read" in alerts[0] and mailbox.full_fetches == [b"1"]


def test_the_header_key_matches_with_or_without_a_message_id():
    with_id = _raw(message_id="<m1@example.com>")
    assert email_opt_out.header_key(with_id.split(b"\n\n", 1)[0]) == email_opt_out.header_key(
        b"Message-ID: <m1@example.com>\r\n")
    without_id = b"From: sam@example.com\r\nTo: optout@nextdeallending.com\r\n"
    assert len(email_opt_out.header_key(without_id)) == 40


class ScriptedDb:
    """Answers the 'already recorded?' lookup; records each propagate_opt_out call."""

    def __init__(self, existing: bool = False) -> None:
        self.existing = existing

    def execute(self, statement, params):
        return SimpleNamespace(scalar=lambda: 1 if self.existing else None)


def test_the_sender_and_every_phone_are_propagated_with_their_own_keys(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(email_opt_out, "propagate_opt_out", lambda db, **kwargs: calls.append(kwargs) or 11)
    opt_out = EmailOptOut(key="abc", sender="sam@example.com", phones=("+18135550123", "+18135550199"))
    assert email_opt_out.record_opt_out(ScriptedDb(), opt_out) == 11
    assert [(c.get("email"), c.get("phone"), c["source_ref"]) for c in calls] == [
        ("sam@example.com", "+18135550123", "email:abc"), (None, "+18135550199", "email:abc:2")]
    assert all(c["channel"].value == "email" for c in calls)


def test_an_already_recorded_opt_out_writes_nothing(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(email_opt_out, "propagate_opt_out", lambda db, **kwargs: calls.append(kwargs))
    opt_out = EmailOptOut(key="abc", sender="sam@example.com", phones=())
    assert email_opt_out.record_opt_out(ScriptedDb(existing=True), opt_out) is None and calls == []


def test_without_mailbox_credentials_the_listener_does_nothing(monkeypatch):
    monkeypatch.setattr(email_opt_out, "get_settings", lambda: SimpleNamespace(
        lending_optout_imap_user="", lending_optout_imap_password=None))
    monkeypatch.setattr(email_opt_out, "ImapMailbox", lambda **kwargs: pytest.fail("mailbox was opened"))
    assert email_opt_out.run() == 0


def test_an_unreadable_mailbox_is_reported_not_raised(monkeypatch):
    from pydantic import SecretStr

    monkeypatch.setattr(email_opt_out, "get_settings", lambda: SimpleNamespace(
        lending_optout_imap_user="hello@nextdeallending.com", lending_optout_imap_password=SecretStr("x"),
        lending_optout_imap_host="imap.example.com", lending_optout_imap_port=993, lending_optout_address=OPTOUT))

    class Refusing:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            raise ConnectionRefusedError("imap down")

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(email_opt_out, "ImapMailbox", Refusing)
    assert email_opt_out.run() == 1
