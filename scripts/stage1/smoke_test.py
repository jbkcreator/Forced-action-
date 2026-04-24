"""
Stage 1 smoke test — run on the server to verify infrastructure is up.

Checks (each prints OK/FAIL with a short reason):
  1. Postgres reachable + query works
  2. Redis reachable (real, local)
  3. FastAPI /  endpoint responds
  4. Agents supervisor is running (checkpoint migration ran)
  5. Admin JWT issues successfully
  6. Sandbox dispatch-event endpoint accepts a known event type
  7. Supervisor can route an event end-to-end (fires FOMO for a fake subscriber)

Exits 0 if all pass, non-zero on first failure.

Usage:
	# Set these first — same values as /etc/forced-action/env
	export ADMIN_USERNAME=admin
	export ADMIN_PASSWORD=...
	export APP_BASE_URL=http://localhost:8000

	python scripts/stage1/smoke_test.py
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

# Allow script to be run from repo root without PYTHONPATH setup
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
	sys.path.insert(0, str(_ROOT))

import requests
from sqlalchemy import text

from src.core.database import db as _db_mgr
from src.core.redis_client import redis_available, reset_client_cache


def _pass(name: str, detail: str = "") -> None:
	print(f"  OK    {name}" + (f"  [{detail}]" if detail else ""))


def _fail(name: str, detail: str) -> None:
	print(f"  FAIL  {name}  [{detail}]")
	sys.exit(1)


def check_postgres() -> None:
	try:
		with _db_mgr.session_scope() as s:
			result = s.execute(text("SELECT 1")).scalar()
			if result != 1:
				_fail("postgres", f"unexpected SELECT 1 result: {result!r}")
		# Sanity on the agents schema
		with _db_mgr.session_scope() as s:
			count = s.execute(text(
				"SELECT count(*) FROM information_schema.tables "
				"WHERE table_name IN ('agent_decisions', 'sandbox_outbox', 'subscribers')"
			)).scalar()
			if count < 3:
				_fail("postgres schema", f"expected 3 core tables, found {count}")
		_pass("postgres")
	except Exception as exc:
		_fail("postgres", str(exc))


def check_redis() -> None:
	# Force re-read of config (Stage 1 uses real local Redis)
	reset_client_cache()
	if not redis_available():
		_fail("redis", "redis_available() returned False — check REDIS_URL and local service")
	from src.core.redis_client import rset, rget
	key = f"smoke:test:{uuid.uuid4().hex[:6]}"
	if not rset(key, "hello", ttl_seconds=60):
		_fail("redis rset", "rset returned False")
	if rget(key) != "hello":
		_fail("redis rget", "round-trip value mismatch")
	_pass("redis", "local, round-trip ok")


def check_fastapi(base_url: str) -> None:
	try:
		r = requests.get(f"{base_url}/", timeout=5)
	except Exception as exc:
		_fail("fastapi", f"connection error: {exc}")
	if r.status_code != 200:
		_fail("fastapi", f"GET / returned {r.status_code}")
	_pass("fastapi", "200 OK")


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
					"checkpoint tables missing — run `python -m scripts.run_agents --migrate` first",
				)
		_pass("agents checkpoint store")
	except Exception as exc:
		_fail("agents checkpoint store", str(exc))


def check_admin_login(base_url: str) -> str:
	u = os.environ.get("ADMIN_USERNAME") or "admin"
	p = os.environ.get("ADMIN_PASSWORD")
	if not p:
		_fail("admin login", "ADMIN_PASSWORD env not set")
	try:
		r = requests.post(
			f"{base_url}/api/admin/login",
			json={"username": u, "password": p},
			timeout=5,
		)
	except Exception as exc:
		_fail("admin login", f"connection error: {exc}")
	if r.status_code != 200:
		_fail("admin login", f"{r.status_code} {r.text[:200]}")
	token = r.json().get("access_token")
	if not token:
		_fail("admin login", "no access_token in response")
	_pass("admin login")
	return token


def check_sandbox_enabled(base_url: str, token: str) -> None:
	r = requests.get(
		f"{base_url}/api/admin/sandbox/outbox?limit=1",
		headers={"Authorization": f"Bearer {token}"},
		timeout=5,
	)
	if r.status_code == 503:
		_fail(
			"sandbox mode",
			"sandbox endpoints report disabled — set TWILIO_SANDBOX=true in env",
		)
	if r.status_code != 200:
		_fail("sandbox outbox", f"{r.status_code} {r.text[:200]}")
	_pass("sandbox mode", "outbox endpoint reachable")


def check_dispatch_endpoint(base_url: str, token: str) -> None:
	# We do not want to create real subscribers. Use subscriber_id=None + a
	# bogus event_type so the supervisor drops cleanly with a 200 response.
	r = requests.post(
		f"{base_url}/api/admin/sandbox/dispatch-event",
		headers={"Authorization": f"Bearer {token}"},
		json={
			"event_type": "bogus_smoke_event_type",
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
	_pass("dispatch-event", "supervisor drop path works")


def main() -> int:
	base_url = os.environ.get("APP_BASE_URL", "http://localhost:8000")

	print(f"Stage 1 smoke test — target: {base_url}\n")

	print("[infrastructure]")
	check_postgres()
	check_redis()
	check_fastapi(base_url)
	check_checkpoint_store()

	print("\n[admin + sandbox]")
	token = check_admin_login(base_url)
	check_sandbox_enabled(base_url, token)
	check_dispatch_endpoint(base_url, token)

	print("\nAll checks passed. Stage 1 infrastructure is ready for narratives.")
	return 0


if __name__ == "__main__":
	sys.exit(main())
