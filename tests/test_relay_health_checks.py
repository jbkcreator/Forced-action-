"""
WP-T2-1 go-live review (2026-09) — relay --health's new DNS/config checks.

DNS lookups are faked (monkeypatching dns.resolver.resolve) rather than
hitting live DNS in unit tests — a real end-to-end run against
forcedactionleads.com's actual DNS was performed manually during
production-execution (see the task's evidence write-up) and found a real
v=DMARC1 record and a genuinely missing SPF record, proving the live path
works; these tests cover the logic's branches deterministically.
"""
from __future__ import annotations

from types import SimpleNamespace

from src.services.relay import __main__ as relay_main


class _FakeRdata:
    def __init__(self, text: str):
        self.strings = [text.encode("utf-8")]


def _fake_resolver(records_by_fqdn: dict[str, list[str]]):
    def _resolve(fqdn, rdtype, lifetime=5.0):
        if fqdn not in records_by_fqdn:
            import dns.resolver as real_resolver
            raise real_resolver.NXDOMAIN()
        return [_FakeRdata(text) for text in records_by_fqdn[fqdn]]
    return _resolve


def test_check_spf_verified(monkeypatch):
    import dns.resolver
    monkeypatch.setattr(
        dns.resolver, "resolve",
        _fake_resolver({"example.com": ["v=spf1 include:_spf.instantly.ai ~all"]}),
    )
    result = relay_main.check_spf("example.com")
    assert result["state"] == "verified"
    assert "v=spf1" in result["detail"]


def test_check_spf_missing_when_no_txt_records(monkeypatch):
    import dns.resolver
    monkeypatch.setattr(dns.resolver, "resolve", _fake_resolver({}))
    result = relay_main.check_spf("example.com")
    assert result["state"] == "missing"


def test_check_spf_missing_when_txt_records_exist_but_no_spf(monkeypatch):
    import dns.resolver
    monkeypatch.setattr(
        dns.resolver, "resolve",
        _fake_resolver({"example.com": ["google-site-verification=abc123"]}),
    )
    result = relay_main.check_spf("example.com")
    assert result["state"] == "missing"


def test_check_dmarc_verified_at_underscore_dmarc_subdomain(monkeypatch):
    import dns.resolver
    monkeypatch.setattr(
        dns.resolver, "resolve",
        _fake_resolver({"_dmarc.example.com": ["v=DMARC1; p=quarantine;"]}),
    )
    result = relay_main.check_dmarc("example.com")
    assert result["state"] == "verified"


def test_check_dmarc_missing(monkeypatch):
    import dns.resolver
    monkeypatch.setattr(dns.resolver, "resolve", _fake_resolver({}))
    result = relay_main.check_dmarc("example.com")
    assert result["state"] == "missing"


def test_check_dkim_is_honestly_unknown_never_a_false_verified():
    """DKIM's selector is provider-assigned — this check must never claim
    'verified' since it has no reliable way to locate the record."""
    result = relay_main.check_dkim("example.com")
    assert result["state"] == "unknown"


def test_check_cron_registered_verified(tmp_path):
    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        "40 6 * * * run.sh src.tasks.fa_max_send_health_monitor\n"
        "*/5 * * * * run.sh src.tasks.fa_max_exceptions_alert_drain\n",
        encoding="utf-8",
    )
    result = relay_main.check_cron_registered(
        ["src.tasks.fa_max_send_health_monitor", "src.tasks.fa_max_exceptions_alert_drain"],
        crontab_path=str(crontab),
    )
    assert result["state"] == "verified"


def test_check_cron_registered_missing_one(tmp_path):
    crontab = tmp_path / "crontab.txt"
    crontab.write_text("40 6 * * * run.sh src.tasks.fa_max_send_health_monitor\n", encoding="utf-8")
    result = relay_main.check_cron_registered(
        ["src.tasks.fa_max_send_health_monitor", "src.tasks.fa_max_exceptions_alert_drain"],
        crontab_path=str(crontab),
    )
    assert result["state"] == "missing"
    assert "fa_max_exceptions_alert_drain" in result["detail"]


def test_check_cron_registered_unreadable_file():
    result = relay_main.check_cron_registered(["x"], crontab_path="/nonexistent/path/crontab.txt")
    assert result["state"] == "missing"


def test_check_go_live_flags_closed_by_default():
    settings = SimpleNamespace(
        fa_max_relay_send_mode="fake",
        fa_max_10dlc_registered=False,
        fa_max_send_backlog_release_confirmed=False,
    )
    result = relay_main.check_go_live_flags(settings)
    assert result["state"] == "closed"


def test_check_go_live_flags_flags_any_open_gate():
    settings = SimpleNamespace(
        fa_max_relay_send_mode="live",
        fa_max_10dlc_registered=False,
        fa_max_send_backlog_release_confirmed=False,
    )
    result = relay_main.check_go_live_flags(settings)
    assert result["state"] == "open"
    assert "fa_max_relay_send_mode=live" in result["detail"]


def test_check_go_live_flags_reports_every_open_gate_not_just_the_first():
    settings = SimpleNamespace(
        fa_max_relay_send_mode="live",
        fa_max_10dlc_registered=True,
        fa_max_send_backlog_release_confirmed=True,
    )
    result = relay_main.check_go_live_flags(settings)
    assert result["state"] == "open"
    assert "fa_max_relay_send_mode=live" in result["detail"]
    assert "fa_max_10dlc_registered=true" in result["detail"]
    assert "fa_max_send_backlog_release_confirmed=true" in result["detail"]


# ---------------------------------------------------------------------------
# Code-review finding (2026-09): --health's exit code used to flip to
# failing whenever go-live gates were open -- meaning a routine post-launch
# diagnostic would report "unhealthy" for the correct, intentional live
# state. check_go_live_readiness is the actual separate go/no-go verdict.
# ---------------------------------------------------------------------------

def _all_verified_kwargs(**overrides):
    base = dict(
        mailbox_configured=True, spf_state="verified", dmarc_state="verified",
        cron_state="verified", exceptions_channel_configured=True, send_mode="fake",
        backlog_release_confirmed=True,
    )
    base.update(overrides)
    return base


def test_readiness_ready_when_everything_automatable_passes():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs())
    assert result["state"] == "ready"


def test_readiness_not_ready_when_spf_unverified():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(spf_state="missing"))
    assert result["state"] == "not_ready"
    assert "SPF" in result["detail"]


def test_readiness_not_ready_when_mailbox_unconfigured():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(mailbox_configured=False))
    assert result["state"] == "not_ready"


def test_readiness_not_ready_when_cron_not_registered():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(cron_state="missing"))
    assert result["state"] == "not_ready"


def test_readiness_not_ready_when_exceptions_channel_unconfigured():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(exceptions_channel_configured=False))
    assert result["state"] == "not_ready"


def test_readiness_reports_every_blocker_not_just_the_first():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(
        mailbox_configured=False, spf_state="missing", cron_state="missing",
    ))
    assert result["state"] == "not_ready"
    assert "mailbox" in result["detail"]
    assert "SPF" in result["detail"]
    assert "cron" in result["detail"]


def test_readiness_already_live_when_send_mode_is_live_even_if_other_checks_would_fail():
    """Once live, re-litigating readiness is meaningless -- and must not
    report 'not_ready' for a launch that already happened, which is exactly
    the confusing state the old --health exit-code coupling produced."""
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(
        send_mode="live", spf_state="missing", mailbox_configured=False,
    ))
    assert result["state"] == "already_live"


def test_readiness_already_live_takes_priority_over_everything_else():
    result = relay_main.check_go_live_readiness(
        mailbox_configured=False, spf_state="missing", dmarc_state="missing",
        cron_state="missing", exceptions_channel_configured=False, send_mode="live",
        backlog_release_confirmed=True,
    )
    assert result["state"] == "already_live"


def test_readiness_not_already_live_when_only_10dlc_confirmed_with_mode_still_fake():
    """Code-review finding (second round, 2026-09): the first version of
    check_go_live_readiness took check_go_live_flags()'s aggregate "open"
    state, which is true if ANY single gate is set -- including
    fa_max_10dlc_registered alone, a legitimate pre-launch prep step
    (docs/fa-max-go-live.md Step 3) that happens BEFORE fa_max_relay_
    send_mode is ever flipped to "live" (Step 5). That version reported
    "already_live" here and skipped every real check. Only send_mode
    itself is the actual master switch."""
    result = relay_main.check_go_live_readiness(
        mailbox_configured=False, spf_state="missing", dmarc_state="missing",
        cron_state="missing", exceptions_channel_configured=False,
        send_mode="fake",  # 10DLC confirmation is a separate, out-of-band fact this function never sees
        backlog_release_confirmed=False,
    )
    assert result["state"] == "not_ready"
    assert result["state"] != "already_live"


def test_readiness_not_already_live_when_mode_live_but_backlog_not_confirmed():
    """Code-review finding (third round, 2026-09): a second bug in this same
    function. guards.py's evaluate() requires BOTH fa_max_relay_send_mode
    == "live" AND fa_max_send_backlog_release_confirmed before any FA Max
    item dispatches (two sequential DEFER checks) -- send_mode alone does
    not make the venture live. Reproduced: send_mode="live" with
    backlog_release_confirmed=False and every other check missing
    incorrectly reported "already_live" before this fix, when in reality
    Relay is deferring every item and this is a real misconfiguration
    (someone flipped mode without the deliberate backlog-release step)."""
    result = relay_main.check_go_live_readiness(
        mailbox_configured=False, spf_state="missing", dmarc_state="missing",
        cron_state="missing", exceptions_channel_configured=False,
        send_mode="live", backlog_release_confirmed=False,
    )
    assert result["state"] != "already_live"
    assert result["state"] == "not_ready"
    assert "backlog_release_confirmed" in result["detail"]


def test_readiness_mode_live_backlog_unconfirmed_still_reports_other_blockers():
    """The mode/backlog mismatch must be reported ALONGSIDE the other
    automatable blockers, not instead of them -- both are real problems."""
    result = relay_main.check_go_live_readiness(
        mailbox_configured=False, spf_state="verified", dmarc_state="verified",
        cron_state="verified", exceptions_channel_configured=True,
        send_mode="live", backlog_release_confirmed=False,
    )
    assert result["state"] == "not_ready"
    assert "backlog_release_confirmed" in result["detail"]
    assert "mailbox" in result["detail"]


def test_readiness_reports_ready_not_already_live_when_mode_is_fake_and_all_else_passes():
    result = relay_main.check_go_live_readiness(**_all_verified_kwargs(send_mode="fake"))
    assert result["state"] == "ready"
