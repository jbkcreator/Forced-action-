"""Lending webhooks are served by the lending-api app only, never by fa-api."""
from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from src.lending.api import app

FA_API_MAIN = Path(__file__).resolve().parents[2] / "src" / "api" / "main.py"


def _paths(application) -> set[str]:
    return {route.path for route in application.routes}


def test_lending_app_serves_the_lending_webhooks_and_health():
    paths = _paths(app)
    assert "/webhooks/lending/ghl-opt-out" in paths
    assert "/health" in paths


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
