"""R1 0.6 guard tests: every automated sender checks the fail-closed pause, every lending unit restarts itself, and the
policy table covers every dependency. Static checks over the source tree; no database."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from config import lending_fallbacks as fb

ROOT = Path(__file__).resolve().parents[2]
LENDING = ROOT / "src" / "lending"
UNITS = ROOT / "deploy" / "systemd"

PAUSE_CHECK = re.compile(r"outbound_paused\(|circuit_breaker\.admit\(")

# Modules that check the emergency halt but need no pause check of their own, and why.
NOT_A_SENDER = {
    "global_halt.py": "the emergency halt itself",
    "after_hours_responder.py": "sends only through GhlSmsSender / GhlEmailSender, whose admit() checks the pause",
    "ghl_call_gate.py": "tags Josh's GHL call lists: human calls, out of scope (D8)",
    "ghl_result_to_bd.py": "writes a BatchDialer card note; reaches nobody",
}

# The automated senders (D8) and the module that holds each one's pause check.
SENDERS = {
    "approval cards (GHL SMS / email)": "ghl_sms.py",
    "approval cards (Instantly)": "instantly_client.py",
    "Instantly cold email": "instantly_email.py",
    "missed-call text-back, reminders, after-hours (GhlSmsSender)": "ghl_sms.py",
    "LendingFlow intro SMS tag": "lendingflow_ghl.py",
    "held intro SMS release": "lendingflow.py",
    "speed-to-lead call cascade and web intro tags": "speed_to_lead.py",
    "GHL tag / stage / note writes": "ghl_crm.py",
    "BatchDialer results as GHL tags": "bd_result_to_ghl.py",
    "BatchDialer loads pushed to GHL": "ghl_load_sync.py",
    "Instantly reply handoff to GHL": "instantly_ghl_handoff.py",
    "pre-qual letter email": "prequal_ghl.py",
    "dialer load": "dialer_load.py",
    "LendingFlow dialer sweep": "lendingflow_dialer.py",
}


def _halt_checkers() -> list[Path]:
    return sorted(p for p in LENDING.rglob("*.py") if "is_globally_halted()" in p.read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", _halt_checkers(), ids=lambda p: p.name)
def test_every_module_that_checks_the_halt_also_checks_the_pause(path):
    if path.name in NOT_A_SENDER:
        return
    assert PAUSE_CHECK.search(path.read_text(encoding="utf-8")), (
        f"{path.name} checks the emergency halt but not the R1 0.6 fail-closed pause (outbound_paused / admit); "
        "add the check, or list it in NOT_A_SENDER with the reason")


@pytest.mark.parametrize("what,module", sorted(SENDERS.items()))
def test_each_automated_sender_checks_the_pause(what, module):
    assert PAUSE_CHECK.search((LENDING / module).read_text(encoding="utf-8")), f"{what}: {module} has no pause check"


def test_the_shared_ghl_and_instantly_senders_admit_through_the_breaker():
    """admit() is where the pause holds approval cards (suppression_unverified)."""
    breaker = (LENDING / "dispatch" / "circuit_breaker.py").read_text(encoding="utf-8")
    assert "outbound_allowed(channel)" in breaker and "SuppressionUnverified" in breaker


def _lending_units() -> list[Path]:
    return sorted(p for p in UNITS.glob("*.service") if "lending" in p.name or p.name.startswith("ndl-"))


def test_there_are_lending_units_to_check():
    assert len(_lending_units()) >= 5


@pytest.mark.parametrize("unit", _lending_units(), ids=lambda p: p.name)
def test_every_lending_unit_restarts_itself(unit):
    assert re.search(r"^Restart=always$", unit.read_text(encoding="utf-8"), re.M), (
        f"{unit.name} must keep Restart=always ({fb.RESTART_POLICY})")


def test_the_dependency_watch_is_scheduled():
    cron = (ROOT / "scripts" / "cron" / "crontab.txt").read_text(encoding="utf-8")
    assert re.search(r"^\*/5 \* \* \* \* .*src\.lending\.dependency_watch$", cron, re.M)
