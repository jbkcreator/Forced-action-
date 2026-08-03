"""
Tests for src.services.relay.__main__.cmd_setup_email_channel (RELAY-v2.2 R2).

CLONE-v2.2 / CL3 regression (PR #195 review): before the venture_config fix,
a non-default venture with no relay_instantly_sender_email of its own would
silently resolve venture #1's RELAY_INSTANTLY_SENDER_EMAIL from settings,
letting this "no sender email configured" guard pass and create a campaign
under the wrong venture's from-address. Now that
src.utils.venture_config._from_row() never falls back to settings for a
non-default venture's Instantly identity, this guard is the real fail-closed
backstop.
"""
from __future__ import annotations

from types import SimpleNamespace

from src.services import instantly_service
from src.services.relay import __main__ as relay_main
from src.utils import venture_config


def test_fails_closed_with_no_sender_email(monkeypatch, capsys):
    monkeypatch.setattr(
        venture_config, "get_venture_config",
        lambda key: SimpleNamespace(relay_instantly_sender_email=None),
    )

    code = relay_main.cmd_setup_email_channel("venture_two")

    assert code == 2
    assert "no relay_instantly_sender_email configured" in capsys.readouterr().err


def test_proceeds_and_finds_existing_campaign_when_sender_email_is_set(monkeypatch):
    monkeypatch.setattr(
        venture_config, "get_venture_config",
        lambda key: SimpleNamespace(relay_instantly_sender_email="hello@venture-two.example"),
    )
    monkeypatch.setattr(
        instantly_service, "list_campaigns",
        lambda: [{"name": "Relay Passthrough (RELAY-v2.2 R2) — venture_two", "id": "camp-existing"}],
    )

    code = relay_main.cmd_setup_email_channel("venture_two")

    assert code == 0
