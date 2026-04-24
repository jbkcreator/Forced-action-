"""
Stage 1 smoke test — run on the server to verify infrastructure is up.

Checks (each prints OK/FAIL with a short reason):
  1. Postgres reachable + query works + Phase 2B schema tables exist
  2. Redis reachable (real, local) — round-trip set/get
  3. FastAPI /  endpoint responds
  4. Agents checkpoint store tables exist (migration ran)
  5. Admin JWT issues successfully
  6. Sandbox mode is enabled (TWILIO_SANDBOX=true or REDIS_SANDBOX=true)
  7. Sandbox dispatch-event endpoint accepts a known event type

Exits 0 if all pass, non-zero on first failure.

Config source:
  This script reads config from the same place the API + agents processes
  read from — config.settings.AppSettings. That means it uses the env file
  you configured at /etc/forced-action/env (or your local .env) through
  the unified settings layer, no ad-hoc env-var setting required.

Usage:
	# on the server, from /opt/forced-action
	sudo -u forcedaction -E .venv/bin/python scripts/stage1/smoke_test.py

Override config for a one-off run:
	# each setting can still be overridden via env for quick debugging
	APP_BASE_URL=http://127.0.0.1:8000 .venv/bin/python scripts/stage1/smoke_test.py
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

# Allow script to be run from repo root without PYTHONPATH setup
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
	sys.path.insert(0, str(_ROOT))

import requests
from sqlalchemy import text

from config.settings import get_settings
from src.core.database import db as _db_mgr
from src.core.redis_client import redis_available, reset_client_cache, rget, rset, get_redis


# ──────────────────────────────────────────────────────────────────────────────
# Pretty-printers
# ──────────────────────────────────────────────────────────────────────────────

def _pass(name: str, detail: str = "") -> None:
	print(f"  OK    {name}" + (f"  [{detail}]" if detail else ""))


def _fail(name: str, detail: str) -> None:
	print(f"  FAIL  {name}  [{detail}]")
	sys.exit(1)


def _section(title: str) -> None:
	print(f"\n[{title}]")


# ──────────────────────────────────────────────────────────────────────────────
# Config resolution
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_config():
	"""
	Return a flat dict of the values this script needs, pulled from the
	shared AppSettings instance. Fail loudly if any required value is
	missing or still carries a REDACTED placeholder.
	"""
	settings = get_settings()

	base_url = (settings.app_base_url or "").rstrip("/")
	if not base_url:
		_fail("config", "app_base_url is empty — check APP_BASE_URL in env")

	admin_username = settings.admin_username
	if not admin_username:
		_fail("config", "admin_username is empty — check ADMIN_USERNAME in env")

	pwd_secret = settings.admin_password
	admin_password = pwd_secret.get_secret_value() if pwd_secret else None
	if not admin_password or "REDACTED" in admin_password:
		_fail("config", "admin_password is unset or still REDACTED — fill it in /etc/forced-action/env")

	if not settings.twilio_sandbox and not settings.redis_sandbox:
		_fail(
			"config",
			"sandbox mode off — set TWILIO_SANDBOX=true (and/or REDIS_SANDBOX=true) in env",
		)

	return {
		"base_url": base_url,
		"admin_username": admin_username,
		"admin_password": admin_password,
		"twilio_sandbox": settings.twilio_sandbox,
		"redis_sandbox": settings.redis_sandbox,
		"redis_url": settings.redis_url,
		"database_url": settings.database_url,
	}


# ──────────────────────────────────────────────────────────────────────────────
# Individual checks
# ──────────────────────────────────────────────────────────────────────────────

def check_postgres() -> None:
	try:
		with _db_mgr.session_scope() as s:
			result = s.execute(text("SELECT 1")).scalar()
			if result != 1:
				_fail("postgres", f"unexpected SELECT 1 result: {result!r}")

		with _db_mgr.session_scope() as s:
			count = s.execute(text(
				"SELECT count(*) FROM information_schema.tables "
				"WHERE table_name IN ('agent_decisions', 'sandbox_outbox', 'subscribers')"
			)).scalar()
			if count < 3:
				_fail("postgres schema", f"expected 3 core tables, found {count}")
		_pass("postgres", "3 core tables present")
	except Exception as exc:
		_fail("postgres", str(exc))


def check_redis(cfg: dict) -> None:
	# settings-driven sandbox flag means the lazy singleton picks the right
	# client; we still reset the cache in case the earlier check polluted it.
	reset_client_cache()
	if not redis_available():
		_fail(
			"redis",
			"redis_available() returned False — check REDIS_URL (and REDIS_SANDBOX if intended)",
		)

	key = f"smoke:test:{uuid.uuid4().hex[:6]}"
	if not rset(key, "hello", ttl_seconds=60):
		_fail("redis rset", "rset returned False")
	if rget(key) != "hello":
		_fail("redis rget", "round-trip value mismatch")

	# Make it clear whether we're on real Redis or fakeredis
	detail = "fakeredis (sandbox)" if cfg["redis_sandbox"] else f"live at {cfg['redis_url']}"
	_pass("redis", detail)


def check_fastapi(base_url: str) -> None:
	try:
		r = requests.get(f"{base_url}/", timeout=5)
	except Exception as exc:
		_fail("fastapi", f"connection error: {exc}")
	if r.status_code != 200:
		_fail("fastapi", f"GET / returned {r.status_code}")
	_pass("fastapi", "GET / returned 200")


def check_checkpoint_store() -> None:
	try:
		with _db_mgr.session_scope() as s:
			count = s.execute(text(
				"SELECT count(*) FROM information_schema.tables "
				"WHERE table_name IN ('checkpoints', 'checkpoint_writes')"
			)).scalar()
			if count < 2:
				_fail(
					"agents checkpoint store",
					"checkpoint tables missing — run `.venv/bin/python -m scripts.run_agents --migrate`",
				)
		_pass("agents checkpoint store")
	except Exception as exc:
		_fail("agents checkpoint store", str(exc))


def check_admin_login(cfg: dict) -> str:
	try:
		r = requests.post(
			f"{cfg['base_url']}/api/admin/login",
			json={"username": cfg["admin_username"], "password": cfg["admin_password"]},
			timeout=5,
		)
	except Exception as exc:
		_fail("admin login", f"connection error: {exc}")
	if r.status_code != 200:
		_fail("admin login", f"{r.status_code} {r.text[:200]}")
	token = r.json().get("access_token")
	if not token:
		_fail("admin login", "no access_token in response")
	_pass("admin login", f"user={cfg['admin_username']}")
	return token


def check_sandbox_enabled(cfg: dict, token: str) -> None:
	r = requests.get(
		f"{cfg['base_url']}/api/admin/sandbox/outbox?limit=1",
		headers={"Authorization": f"Bearer {token}"},
		timeout=5,
	)
	if r.status_code == 503:
		_fail(
			"sandbox mode",
			"endpoints report disabled at runtime — settings.twilio_sandbox / redis_sandbox must be true",
		)
	if r.status_code != 200:
		_fail("sandbox outbox", f"{r.status_code} {r.text[:200]}")
	active = []
	if cfg["twilio_sandbox"]:
		active.append("twilio")
	if cfg["redis_sandbox"]:
		active.append("redis")
	_pass("sandbox mode", f"active: {'+'.join(active)}")


def check_dispatch_endpoint(cfg: dict, token: str) -> None:
	# Use subscriber_id=None + bogus event_type so the supervisor drops cleanly
	# and returns 200. We're testing the *endpoint wiring*, not a real graph.
	r = requests.post(
		f"{cfg['base_url']}/api/admin/sandbox/dispatch-event",
		headers={"Authorization": f"Bearer {token}"},
		json={
			"event_type": "smoke_test_unknown_event",
			"subscriber_id": None,
			"payload": {},
		},
		timeout=10,
	)
	if r.status_code != 200:
		_fail("dispatch-event", f"{r.status_code} {r.text[:200]}")
	body = r.json()
	if body.get("outcome") != "dropped_unknown_event":
		_fail(
			"dispatch-event routing",
			f"expected 'dropped_unknown_event', got {body.get('outcome')!r}",
		)
	_pass("dispatch-event", "supervisor drop path verified")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
	cfg = _resolve_config()

	print(f"Stage 1 smoke test — target: {cfg['base_url']}")
	print(f"  sandbox_twilio={cfg['twilio_sandbox']}  sandbox_redis={cfg['redis_sandbox']}")

	_section("infrastructure")
	check_postgres()
	check_redis(cfg)
	check_fastapi(cfg["base_url"])
	check_checkpoint_store()

	_section("admin + sandbox")
	token = check_admin_login(cfg)
	check_sandbox_enabled(cfg, token)
	check_dispatch_endpoint(cfg, token)

	print("\nAll checks passed. Stage 1 infrastructure is ready for narratives.")
	return 0


if __name__ == "__main__":
	sys.exit(main())
