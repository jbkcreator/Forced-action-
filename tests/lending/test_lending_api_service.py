"""Lending webhooks are served by the lending-api app only, never by fa-api."""
from __future__ import annotations

import ast
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.lending.api import app

FA_API_MAIN = Path(__file__).resolve().parents[2] / "src" / "api" / "main.py"


def _paths(application) -> set[str]:
    return set(application.openapi()["paths"])   # not app.routes: newer FastAPI wraps included routers


def test_lending_app_serves_the_lending_webhooks_and_health():
    paths = _paths(app)
    assert "/webhooks/lending/ghl-opt-out" in paths
    assert "/api/lending/web-leads" in paths
    assert "/health" in paths


def test_lending_app_serves_the_booking_reply_and_consent_webhooks():
    paths = _paths(app)
    for path in ("/ghl-appointment", "/booking-confirmed", "/confirmation-task-complete", "/ghl-nurture",
                 "/booking-gate-failed", "/ghl-reply", "/ghl-text-consent"):
        assert f"/webhooks/lending{path}" in paths, path


def test_health():
    assert TestClient(app).get("/health").json() == {"status": "ok"}


def test_fa_api_mounts_no_lending_router():
    tree = ast.parse(FA_API_MAIN.read_text(encoding="utf-8"))
    lending_imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                       and node.module and ("lending" in node.module)]
    assert lending_imports == [], [node.module for node in lending_imports]
    assert "/webhooks/lending" not in FA_API_MAIN.read_text(encoding="utf-8")


def test_lending_app_does_not_load_fa_api():
    code = "import sys, src.lending.api; sys.exit(1 if 'src.api.main' in sys.modules else 0)"
    assert subprocess.run([sys.executable, "-c", code], cwd=FA_API_MAIN.parents[2]).returncode == 0


ROUTED = "location /webhooks/lending/ {\n    proxy_pass http://127.0.0.1:8010;\n}\n"
VERIFY_SCRIPT = FA_API_MAIN.parents[2] / "deploy" / "verify_lending_routing.sh"


def _routing_check(tmp_path: Path, nginx_dump: str | None) -> int:
    """Run the deploy pre-flight against a stub `nginx -T` that prints ``nginx_dump`` (None = nginx broken)."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    dump = tmp_path / "dump.conf"
    dump.write_text(nginx_dump or "", encoding="utf-8")
    stub = tmp_path / "nginx"
    stub.write_text(f'#!/bin/bash\n{"exit 1" if nginx_dump is None else f"cat {dump.as_posix()}"}\n', encoding="utf-8")
    stub.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path.as_posix()}{os.pathsep}{os.environ['PATH']}"}
    return subprocess.run([bash, VERIFY_SCRIPT.as_posix()], env=env, capture_output=True).returncode


def test_deploy_preflight_passes_when_nginx_routes_lending_to_8010(tmp_path):
    assert _routing_check(tmp_path, "server {\n" + ROUTED + "location /webhooks/ { proxy_pass http://127.0.0.1:8000; }\n}\n") == 0


@pytest.mark.parametrize("dump", [
    "server {\n    location /webhooks/ { proxy_pass http://127.0.0.1:8000; }\n}\n",
    "server {\n    # location /webhooks/lending/ {\n    #     proxy_pass http://127.0.0.1:8010;\n    # }\n}\n",
    "location /webhooks/lending/ {\n    proxy_pass http://127.0.0.1:8000;\n}\n",
    None,
], ids=["block-missing", "block-commented-out", "wrong-upstream", "nginx-failing"])
def test_deploy_preflight_fails_when_lending_route_is_not_active(tmp_path, dump):
    assert _routing_check(tmp_path, dump) != 0


def test_deploy_script_hard_gates_on_routing_and_lending_api_health():
    deploy = (FA_API_MAIN.parents[2] / "deploy.sh").read_text(encoding="utf-8")
    assert 'verify_lending_routing.sh" || fail' in deploy
    assert "127.0.0.1:8010/health" in deploy and "|| fail" in deploy.split("127.0.0.1:8010/health", 1)[1].splitlines()[1]
    assert deploy.index("verify_lending_routing.sh") < deploy.index('systemctl restart fa-api || fail')
