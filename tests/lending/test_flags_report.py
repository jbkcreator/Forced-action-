"""P0.5: the lending feature flag table covers every flag, shows the live value, and never changes a flag."""
from __future__ import annotations

from types import SimpleNamespace

from config.settings import AppSettings
from src.lending import flags_report


def _settings(**on):
    values = {flag.env.lower(): False for flag in flags_report.FLAGS}
    values.update({name.lower(): True for name in on})
    return SimpleNamespace(**values)


def test_every_lending_enabled_or_approved_setting_is_in_the_table():
    assert flags_report.unlisted_flags() == []


def test_every_listed_flag_is_a_real_boolean_setting():
    for flag in flags_report.FLAGS:
        field = AppSettings.model_fields.get(flag.env.lower())
        assert field is not None, flag.env
        assert field.annotation is bool, flag.env


def test_the_flags_the_brief_names_that_do_not_exist_are_not_invented():
    names = {flag.env for flag in flags_report.FLAGS}
    assert "LENDING_AUTO_DISPATCH_ENABLED" not in names        # no such setting in the code
    assert "LENDING_AFTER_HOURS_ENABLED" not in names           # the real one is LENDING_AFTER_HOURS_INTAKE_ENABLED
    assert {"LENDING_AFTER_HOURS_INTAKE_ENABLED", "LENDING_SMS_CARRIER_APPROVED", "LENDING_PREQUAL_PDF_ENABLED"} <= names


def test_the_table_shows_live_values_and_marks_a_flag_changed_from_its_default():
    table = flags_report.markdown_table(_settings(LENDING_SOFT_APPROVAL_ENABLED=True))
    assert "| `LENDING_SOFT_APPROVAL_ENABLED` | ON (changed) | off |" in table
    assert "| `LENDING_SMS_CARRIER_APPROVED` | off | off |" in table
    assert table.splitlines()[0] == f"Lending feature flags in production: 1 on, {len(flags_report.FLAGS) - 1} off"
    assert len([line for line in table.splitlines() if line.startswith("| `")]) == len(flags_report.FLAGS)


def test_printing_the_table_posts_nothing(monkeypatch, capsys):
    posted = []
    monkeypatch.setattr(flags_report, "get_settings", lambda: _settings())
    monkeypatch.setattr(flags_report, "_post", lambda *a: posted.append(a))
    assert flags_report.main([]) == 0
    assert "| Flag | Status | Default | What it does |" in capsys.readouterr().out
    assert posted == []


def test_post_goes_to_cora_channel_as_a_code_block(monkeypatch):
    from pydantic import SecretStr

    posted = []
    fake = _settings()
    fake.lending_cora_channel, fake.lending_cora_slack_bot_token = "C_CORA", SecretStr("xoxb-cora")
    monkeypatch.setattr(flags_report, "get_settings", lambda: fake)
    monkeypatch.setattr(flags_report, "_post", lambda text, channel, token: posted.append((channel, token)))
    assert flags_report.main(["--post"]) == 0
    assert posted == [("C_CORA", "xoxb-cora")]


def test_post_without_a_channel_or_token_posts_nothing_and_fails(monkeypatch):
    fake = _settings()
    fake.lending_cora_channel, fake.lending_cora_slack_bot_token = "", None
    monkeypatch.setattr(flags_report, "get_settings", lambda: fake)
    monkeypatch.setattr(flags_report, "_post", lambda *a: (_ for _ in ()).throw(AssertionError("posted")))
    assert flags_report.main(["--post"]) == 1
