"""LendingFlow sender identity: settings parsing, address checks, one report per problem. No database."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from src.lending import email_identity
from src.lending.email_identity import LendingEmailIdentity, lending_email_identity

SENDER = "lf-sender@nextdeallending.com"
BCC_A, BCC_B = "lf-bcc@nextdeallending.com", "lf-bcc-two@nextdeallending.com"


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(email_identity, "_warned", None)
    events: list[dict] = []
    monkeypatch.setattr(email_identity, "capture_error", lambda message, **kw: events.append(kw) or True)
    return events


def _settings(monkeypatch, sender, bcc):
    monkeypatch.setattr(email_identity, "get_settings",
                        lambda: SimpleNamespace(lending_lendingflow_email_from=sender, lending_lendingflow_email_bcc=bcc))


def test_sender_and_bcc_are_trimmed_lowercased_and_deduplicated_in_order(monkeypatch):
    _settings(monkeypatch, f"  {SENDER.upper()} ", f" {BCC_B} ,{BCC_A.upper()}, {BCC_B},")
    assert lending_email_identity() == LendingEmailIdentity(SENDER, (BCC_B, BCC_A))


@pytest.mark.parametrize("value", [
    "", "lf-sender", "@nextdeallending.com", "lf-sender@", "lf-sender@localhost", "lf-sender@domain.",
    "a@b@c.com", f"{SENDER},{BCC_A}", f"{SENDER};{BCC_A}", f"Sender <{SENDER}>", "lf sender@nextdeallending.com",
])
def test_anything_but_one_bare_address_is_refused_as_the_sender(monkeypatch, value):
    _settings(monkeypatch, value, BCC_A)
    assert lending_email_identity() is None


@pytest.mark.parametrize("bcc", ["", " , ", f"{BCC_A}, not-an-address", f"Bcc <{BCC_A}>", f"{BCC_A};{BCC_B}"])
def test_an_empty_or_invalid_bcc_list_is_refused(monkeypatch, bcc):
    _settings(monkeypatch, SENDER, bcc)
    assert lending_email_identity() is None


def test_a_missing_setting_is_logged_and_sent_to_sentry_once_per_problem(monkeypatch, caplog, _fresh):
    _settings(monkeypatch, None, BCC_A)
    with caplog.at_level(logging.ERROR, logger="src.lending.email_identity"):
        assert lending_email_identity() is None
        assert lending_email_identity() is None
    assert [r.getMessage() for r in caplog.records].count(
        "[lending-email] no email to a LendingFlow lead will be sent; missing or invalid: "
        "LENDING_LENDINGFLOW_EMAIL_FROM") == 1
    assert len(_fresh) == 1 and _fresh[0]["tags"]["missing_settings"] == "LENDING_LENDINGFLOW_EMAIL_FROM"

    _settings(monkeypatch, None, "")
    assert lending_email_identity() is None
    assert len(_fresh) == 2  # a different problem is reported again


def test_nothing_is_reported_when_both_are_set(monkeypatch, caplog, _fresh):
    _settings(monkeypatch, SENDER, BCC_A)
    with caplog.at_level(logging.ERROR, logger="src.lending.email_identity"):
        assert lending_email_identity() is not None
    assert caplog.records == [] and _fresh == []
