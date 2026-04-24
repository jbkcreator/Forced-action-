"""
Stage 1 narrative helpers — composable Python functions for the server runbook.

Run from the server with the same venv the services use:
	source /opt/forced-action/.venv/bin/activate
	python scripts/stage1/narrative_helpers.py <narrative_name> [args]

Or import and call directly from a REPL:
	from scripts.stage1.narrative_helpers import *
	token = login("admin", "PASSWORD")
	seed_result = seed_scenario_subscriber(token, name="Mike", vertical="roofing")
	dispatch_abandonment_wave1(token, seed_result["subscriber_id"])
	print_outbox(token, seed_result["subscriber_id"])

Every helper prints a short status line so live narratives produce readable log output.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

# Allow running from repo root
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
	sys.path.insert(0, str(_ROOT))

import requests


BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:8000")


# ──────────────────────────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────────────────────────

def login(username: Optional[str] = None, password: Optional[str] = None) -> str:
	"""Exchange admin creds for a bearer token. Pulls from env if not passed."""
	u = username or os.environ.get("ADMIN_USERNAME", "admin")
	p = password or os.environ.get("ADMIN_PASSWORD")
	if not p:
		raise RuntimeError("ADMIN_PASSWORD not set in env and not passed explicitly")
	r = requests.post(f"{BASE_URL}/api/admin/login",
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
# Event dispatch
# ──────────────────────────────────────────────────────────────────────────────

def dispatch_event(
	token: str,
	event_type: str,
	subscriber_id: Optional[int] = None,
	payload: Optional[Dict[str, Any]] = None,
	decision_id: Optional[str] = None,
) -> Dict[str, Any]:
	body = {
		"event_type": event_type,
		"subscriber_id": subscriber_id,
		"payload": payload or {},
	}
	if decision_id:
		body["decision_id"] = decision_id
	r = requests.post(f"{BASE_URL}/api/admin/sandbox/dispatch-event",
					  headers=_headers(token), json=body, timeout=30)
	r.raise_for_status()
	result = r.json()
	print(f"[dispatch] {event_type} → outcome={result['outcome']}  "
		   f"graph={result.get('graph_name')}  reason={result['reason']}")
	return result


def dispatch_abandonment_wave1(
	token: str, subscriber_id: int, zip_code: str = "33647",
	vertical: str = "roofing",
) -> Dict[str, Any]:
	"""Fire wave1 as if the user just hit the 12-min no-payment threshold."""
	return dispatch_event(token, "wall_session_abandoned", subscriber_id, {
		"zip_code": zip_code, "vertical": vertical,
		"minutes_elapsed": 12, "wall_countdown_minutes": 3,
	})


def dispatch_abandonment_wave2(
	token: str, subscriber_id: int, decision_id: str,
) -> Dict[str, Any]:
	"""Click-no-complete follow-up, under the same decision_id as wave1."""
	return dispatch_event(token, "abandonment_click_no_complete", subscriber_id, {
		"lead_tier_viewed": "Gold", "wall_countdown_minutes": 1,
	}, decision_id=decision_id)


def dispatch_fomo(
	token: str, subscriber_id: int, zip_code: str = "33647",
	vertical: str = "roofing", competitor_subscriber_id: int = 0,
) -> Dict[str, Any]:
	"""Competitor-acted-on-Gold-lead event in a non-locked ZIP."""
	return dispatch_event(token, "competitor_acted_on_lead", subscriber_id, {
		"competitor_event_id": str(uuid.uuid4()),
		"lead_id": 999,
		"zip_code": zip_code, "vertical": vertical,
		"competitor_subscriber_id": competitor_subscriber_id,
		"lead_tier": "Gold",
	})


def dispatch_retention(
	token: str, subscriber_id: int, tier: str = "wallet",
) -> Dict[str, Any]:
	return dispatch_event(token, "retention_summary_due", subscriber_id, {
		"tier": tier,
	})


# ──────────────────────────────────────────────────────────────────────────────
# Outbox inspection
# ──────────────────────────────────────────────────────────────────────────────

def read_outbox(
	token: str, subscriber_id: Optional[int] = None,
	campaign: Optional[str] = None, limit: int = 20,
) -> list:
	params: Dict[str, Any] = {"limit": limit}
	if subscriber_id is not None:
		params["subscriber_id"] = subscriber_id
	if campaign:
		params["campaign"] = campaign
	r = requests.get(f"{BASE_URL}/api/admin/sandbox/outbox",
					 headers=_headers(token), params=params, timeout=10)
	r.raise_for_status()
	return r.json()


def print_outbox(token: str, subscriber_id: int) -> None:
	rows = read_outbox(token, subscriber_id=subscriber_id, limit=20)
	print(f"[outbox] {len(rows)} rows for subscriber {subscriber_id}")
	for row in rows:
		print(f"   - [{row['created_at']}] "
			   f"campaign={row['campaign']}  "
			   f"deliver={row['would_have_delivered']}  "
			   f"allowed={row['compliance_allowed']}")
		print(f"     body: {row['body'][:120]}")


# ──────────────────────────────────────────────────────────────────────────────
# Webhook simulators
# ──────────────────────────────────────────────────────────────────────────────

def simulate_inbound_sms(token: str, from_number: str, body: str) -> Dict[str, Any]:
	r = requests.post(f"{BASE_URL}/api/admin/sandbox/simulate-inbound",
					  headers=_headers(token),
					  json={"from_number": from_number, "body": body}, timeout=10)
	r.raise_for_status()
	result = r.json()
	print(f"[inbound] from={from_number} body={body!r} → handled={result.get('handled')}")
	return result


def simulate_storm(token: str, event_name: str = "Severe Thunderstorm Warning") -> Dict[str, Any]:
	r = requests.post(f"{BASE_URL}/api/admin/sandbox/simulate-nws-alert",
					  headers=_headers(token),
					  json={"properties": {"event": event_name,
										    "areaDesc": "Hillsborough, FL"}},
					  timeout=10)
	r.raise_for_status()
	result = r.json()
	print(f"[nws] event={event_name} → {result.get('result', {}).get('event')}")
	return result


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

	disp = sub.add_parser("dispatch")
	disp.add_argument("event_type")
	disp.add_argument("subscriber_id", type=int)
	disp.add_argument("--payload", default="{}")

	outbox = sub.add_parser("outbox")
	outbox.add_argument("subscriber_id", type=int)

	inbound = sub.add_parser("inbound")
	inbound.add_argument("from_number")
	inbound.add_argument("body")

	storm = sub.add_parser("storm")
	storm.add_argument("--event", default="Severe Thunderstorm Warning")

	args = parser.parse_args()
	token = login()

	if args.cmd == "login":
		print(token)
	elif args.cmd == "seed":
		result = seed_scenario_subscriber(
			name=args.name, vertical=args.vertical, tier=args.tier,
		)
		print(json.dumps(result, indent=2))
	elif args.cmd == "dispatch":
		payload = json.loads(args.payload)
		result = dispatch_event(token, args.event_type, args.subscriber_id, payload)
		print(json.dumps(result, indent=2, default=str))
	elif args.cmd == "outbox":
		print_outbox(token, args.subscriber_id)
	elif args.cmd == "inbound":
		simulate_inbound_sms(token, args.from_number, args.body)
	elif args.cmd == "storm":
		simulate_storm(token, args.event)

	return 0


if __name__ == "__main__":
	sys.exit(_cli())
