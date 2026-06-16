"""CLI tests for src.tasks.financing_intent_sweep (Sprint S1).

Spawns a subprocess for each test so we exercise the full CLI entry point.
All tests run with --dry-run to avoid writing permanent rows to the DB.
DATABASE_URL must be configured; tests skip otherwise.
"""
from __future__ import annotations

import json
import subprocess
import sys

import pytest


def _cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "src.tasks.financing_intent_sweep", *args],
        capture_output=True,
        text=True,
    )


def _requires_db():
    """Skip if DATABASE_URL is not reachable."""
    try:
        from config.settings import get_settings
        from sqlalchemy import create_engine, text
        engine = create_engine(str(get_settings().database_url))
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"DATABASE_URL not reachable: {exc}")


# ── CLI integration tests ──────────────────────────────────────────────────────


def test_cli_dry_run_exits_zero():
    _requires_db()
    result = _cli("--dry-run", "--limit", "1")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "scored" in payload
    assert payload["new"] == 0
    assert payload["updated"] == 0


def test_cli_county_filter():
    _requires_db()
    result = _cli("--dry-run", "--county-id", "hillsborough", "--limit", "10")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert isinstance(payload.get("checked"), int)
    assert isinstance(payload.get("scored"), int)


def test_cli_limit():
    _requires_db()
    result = _cli("--dry-run", "--limit", "5")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["scored"] <= 5


def test_cli_rescore_all_flag_accepted():
    _requires_db()
    result = _cli("--dry-run", "--rescore-all", "--limit", "1")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "scored" in payload
    assert "by_tier" in payload
