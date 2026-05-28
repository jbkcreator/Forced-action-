"""
Stage 1 narrative helpers — composable Python functions for the server runbook.

Run from the server with the same venv the services use:
	source /opt/forced-action/.venv/bin/activate
	python scripts/stage1/narrative_helpers.py <narrative_name> [args]

Or import and call directly from a REPL:
	from scripts.stage1.narrative_helpers import *
	token = login("admin", "PASSWORD")
	seed_result = seed_scenario_subscriber(token, name="Mike", vertical="roofing")

Every helper prints a short status line so live narratives produce readable log output.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

# Allow running from repo root
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
	sys.path.insert(0, str(_ROOT))

import requests

from config.settings import get_settings


def _base_url() -> str:
	"""Resolved base URL from settings (APP_BASE_URL env → AppSettings.app_base_url)."""
	return (get_settings().app_base_url or "http://localhost:8000").rstrip("/")


def _admin_creds() -> tuple[str, str]:
	"""Return (username, password) from AppSettings. Raises if password missing."""
	s = get_settings()
	username = s.admin_username or "admin"
	pwd_secret = s.admin_password
	password = pwd_secret.get_secret_value() if pwd_secret else None
	if not password:
		raise RuntimeError(
			"admin_password not set — fill ADMIN_PASSWORD in /etc/forced-action/env"
		)
	return username, password


# ──────────────────────────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────────────────────────

def login(username: Optional[str] = None, password: Optional[str] = None) -> str:
	"""
	Exchange admin creds for a bearer token. Reads from settings.py by default;
	explicit args still override so you can impersonate a different admin in
	a REPL session without touching env.
	"""
	default_user, default_pwd = _admin_creds()
	u = username or default_user
	p = password or default_pwd
	r = requests.post(f"{_base_url()}/api/admin/login",
					  json={"username": u, "password": p}, timeout=10)
	r.raise_for_status()
	token = r.json()["access_token"]
	print(f"[auth] logged in as {u}")
	return token


def _headers(token: str) -> Dict[str, str]:
	return {"Authorization": f"Bearer {token}"}


# ──────────────────────────────────────────────────────────────────────────────
# Subscriber seeding — real DB inserts, so scenarios operate on real rows
# ──────────────────────────────────────────────────────────────────────────────

def seed_scenario_subscriber(
	*,
	name: str = "Stage1 Scenario User",
	vertical: str = "roofing",
	county_id: str = "hillsborough",
	tier: str = "free",
	has_saved_card: bool = False,
	status: str = "active",
	phone: Optional[str] = None,
	opt_in: bool = True,
) -> Dict[str, Any]:
	"""
	Insert a scenario subscriber + opt-in row directly into Postgres.
	Returns the new subscriber id + phone for subsequent narrative steps.

	Why direct DB writes instead of the HTTP signup flow: Stage 1 narratives
	exercise single graphs, not the full signup funnel. A direct seed gives
	us a predictable starting state in < 1 second.
	"""
	from src.core.database import db
	from src.core.models import Subscriber, SmsOptIn

	suffix = uuid.uuid4().hex[:8]
	email = f"scenario_{suffix}@example.test"
	stripe_cust = f"cus_stage1_{suffix}"
	final_phone = phone or f"+15551{suffix[:7]}"

	with db.session_scope() as s:
		sub = Subscriber(
			stripe_customer_id=stripe_cust,
			tier=tier,
			vertical=vertical,
			county_id=county_id,
			email=email,
			name=name,
			status=status,
			has_saved_card=has_saved_card,
			event_feed_uuid=str(uuid.uuid4()),
			referral_code=f"REF{suffix[:5].upper()}",
		)
		s.add(sub)
		s.flush()
		sid = sub.id

		if opt_in:
			s.add(SmsOptIn(
				phone=final_phone,
				subscriber_id=sid,
				keyword_used="YES",
				source="double_opt_in",
				opt_in_message="stage1 narrative seed",
			))
		s.flush()

	print(f"[seed] subscriber_id={sid} email={email} phone={final_phone}")
	return {"subscriber_id": sid, "email": email, "phone": final_phone,
			"stripe_customer_id": stripe_cust}


# ──────────────────────────────────────────────────────────────────────────────
# CLI dispatcher for shell usage
# ──────────────────────────────────────────────────────────────────────────────

def _cli() -> int:
	parser = argparse.ArgumentParser(description="Stage 1 narrative helpers")
	sub = parser.add_subparsers(dest="cmd", required=True)

	sub.add_parser("login")

	seed = sub.add_parser("seed")
	seed.add_argument("--name", default="Stage1 Scenario User")
	seed.add_argument("--vertical", default="roofing")
	seed.add_argument("--tier", default="free")

	args = parser.parse_args()
	token = login()

	if args.cmd == "login":
		print(token)
	elif args.cmd == "seed":
		result = seed_scenario_subscriber(
			name=args.name, vertical=args.vertical, tier=args.tier,
		)
		print(json.dumps(result, indent=2))

	return 0


if __name__ == "__main__":
	sys.exit(_cli())
