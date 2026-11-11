"""P0.4: every deploy installs, restarts and health-checks Cora. Cora's unit was missing from deploy.sh's
LENDING_UNITS, so deploys never cycled her and she ran stale code (silent from about Oct 28)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEPLOY_SH = (ROOT / "deploy.sh").read_text(encoding="utf-8")


def _lending_units() -> list[str]:
    match = re.search(r"^LENDING_UNITS=\(([^)]*)\)", DEPLOY_SH, re.MULTILINE)
    assert match, "LENDING_UNITS array not found in deploy.sh"
    return match.group(1).split()


def test_cora_is_in_the_lending_units_every_deploy_cycles():
    assert "fa-lending-cora" in _lending_units()


def test_every_lending_unit_has_its_unit_file_in_the_repo():
    for unit in _lending_units():
        assert (ROOT / "deploy" / "systemd" / f"{unit}.service").is_file(), unit


def test_lending_units_are_installed_restarted_and_health_checked():
    loops = re.findall(r'for unit in "\$\{LENDING_UNITS\[@\]\}"; do(.*?)\ndone', DEPLOY_SH, re.DOTALL)
    bodies = "\n".join(loops)
    assert 'systemctl enable "$unit"' in bodies
    assert 'systemctl restart "$unit"' in bodies
    assert 'systemctl is-active --quiet "$unit"' in bodies


def test_cora_unit_restarts_itself_and_runs_the_cora_agent():
    unit = (ROOT / "deploy" / "systemd" / "fa-lending-cora.service").read_text(encoding="utf-8")
    assert "ExecStart=/root/Forced-action-/.venv/bin/python -m src.lending.cora_agent" in unit
    assert "Restart=always" in unit
