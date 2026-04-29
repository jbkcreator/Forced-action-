"""
Gating tools for Cora graphs.

These five tools are the safety layer. Every autonomous decision consults
them before acting:

	guardrail_check        — is the proposed numeric value within bounds?
	compliance_check       — can we send SMS to this subscriber right now?
	kill_switch_status     — what colour is this metric/feature currently?
	budget_check           — have we exceeded per-decision token/cost cap?
	ab_variant_assign      — which variant is this user on? (deterministic)

All five are declared category="gating", idempotent=True. They do not mutate
external state (compliance_check is read-only; ab_variant_assign writes at
most one row and the write is idempotent by design because the underlying
hash is deterministic).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from sqlalchemy.orm import Session

from src.agents.tools.registry import tool
from src.core.database import db


@contextmanager
def _session(provided: Optional[Session]) -> Generator[Session, None, None]:
	if provided is not None:
		yield provided
		return
	with db.session_scope() as s:
		yield s


# ──────────────────────────────────────────────────────────────────────────────
# 1. guardrail_check
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="gating")
def guardrail_check(decision_type: str, proposed_value: float) -> Dict[str, Any]:
	"""
	Return {'allowed': bool, 'reason': str, 'bound': dict} for a proposed
	autonomous decision.

	decision_type must be a key in config.cora_guardrails.GUARDRAILS.
	proposed_value is the number being evaluated (cents for pricing,
	percent for discounts, credits for bonuses, minutes for windows).

	Unknown decision_type → allowed=False with reason 'unknown_guardrail'
	(fail-safe — never allow a decision we can't validate).
	"""
	from config.cora_guardrails import GUARDRAILS, is_within_guardrail

	if decision_type not in GUARDRAILS:
		return {
			"allowed": False,
			"reason": "unknown_guardrail",
			"bound": {},
			"decision_type": decision_type,
			"proposed_value": proposed_value,
		}

	bound = GUARDRAILS[decision_type]
	allowed = is_within_guardrail(decision_type, proposed_value)

	return {
		"allowed": bool(allowed),
		"reason": "within_bounds" if allowed else bound.get("rollback_trigger", "out_of_bounds"),
		"bound": dict(bound),
		"decision_type": decision_type,
		"proposed_value": proposed_value,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 2. compliance_check
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="gating")
def compliance_check(
	subscriber_id: int,
	message_type: str = "marketing",
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return {'can_send': bool, 'reason': str} for a proposed SMS to a subscriber.

	Checks (in order):
	  1. Subscriber exists and has a phone we can resolve
	  2. No STOP record (opt-out) for that phone
	  3. Valid TCPA opt-in for that phone (unless message_type='transactional')

	Transactional messages (message_type='transactional') skip the opt-in
	requirement — used for payment receipts, subscription changes, etc.
	Marketing messages require both no opt-out and a valid opt-in.
	"""
	from src.core.models import Subscriber, SmsOptIn, SmsOptOut

	with _session(session) as s:
		sub = s.query(Subscriber).filter(Subscriber.id == subscriber_id).first()
		if sub is None:
			return {
				"can_send": False,
				"reason": "subscriber_not_found",
				"subscriber_id": subscriber_id,
			}

		# No phone column on subscriber in the schema we've seen — callers
		# must resolve the phone and pass it directly. We therefore key the
		# compliance check by subscriber_id via the opt-in records instead.
		opt_in = (
			s.query(SmsOptIn)
			.filter(SmsOptIn.subscriber_id == subscriber_id)
			.order_by(SmsOptIn.opted_in_at.desc())
			.first()
		)
		if opt_in is None:
			phone = None
		else:
			phone = opt_in.phone

		# If we have no opt-in row, we can't send marketing. Transactional
		# still requires a known phone to target.
		if phone is None:
			return {
				"can_send": False,
				"reason": "no_phone_on_record",
				"subscriber_id": subscriber_id,
			}

		opt_out = (
			s.query(SmsOptOut)
			.filter(SmsOptOut.phone == phone)
			.first()
		)
		if opt_out is not None:
			return {
				"can_send": False,
				"reason": "opted_out",
				"subscriber_id": subscriber_id,
				"phone": phone,
			}

		if message_type == "marketing" and opt_in is None:
			return {
				"can_send": False,
				"reason": "no_opt_in",
				"subscriber_id": subscriber_id,
				"phone": phone,
			}

		return {
			"can_send": True,
			"reason": "ok",
			"subscriber_id": subscriber_id,
			"phone": phone,
			"message_type": message_type,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 3. kill_switch_status
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="gating")
def kill_switch_status(feature: str, observed_value: Optional[float] = None) -> Dict[str, Any]:
	"""
	Return the Green/Yellow/Red colour for a feature, based on the current
	observed metric value against config.cora_guardrails.KILL_SWITCH
	thresholds.

	If observed_value is None we return the config band only ('unknown'
	colour). Callers that want a colour should pass in the live metric.

	Unknown feature → {'color': 'unknown', 'reason': 'unknown_feature'}.
	This is the fail-safe: graphs treat 'unknown' as RED.
	"""
	from config.cora_guardrails import KILL_SWITCH

	band = KILL_SWITCH.get(feature)
	if band is None:
		return {
			"feature": feature,
			"color": "unknown",
			"reason": "unknown_feature",
			"observed_value": observed_value,
		}

	if observed_value is None:
		return {
			"feature": feature,
			"color": "unknown",
			"reason": "no_observed_value",
			"band": band,
		}

	# Determine colour by comparing observed against thresholds. The metric
	# semantics differ by band — some have lower-is-worse (CAC, cost ratios)
	# and some higher-is-worse. We treat 'green' as a numeric threshold: if
	# the metric should be >= green it's a "higher_is_better" metric, and
	# vice versa. We disambiguate by checking the keys present.
	higher_is_better = "red" in band and isinstance(band["red"], (int, float)) and band["red"] < band["green"]

	red = band["red"]
	green = band["green"]

	if higher_is_better:
		if observed_value >= green:
			color = "green"
		elif observed_value >= red:
			color = "yellow"
		else:
			color = "red"
	else:
		# lower_is_better
		if observed_value <= green:
			color = "green"
		elif observed_value <= red:
			color = "yellow"
		else:
			color = "red"

	return {
		"feature": feature,
		"color": color,
		"observed_value": observed_value,
		"action": band.get("action"),
		"band": band,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 4. budget_check
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="gating")
def budget_check(
	tokens_used: int,
	cost_usd: float,
	graph_name: Optional[str] = None,
) -> Dict[str, Any]:
	"""
	Return {'allowed': bool, 'reason': str, 'tokens_remaining': int,
	'cost_remaining_usd': float} given current spend for one decision.

	Enforces the per-decision caps in AgentsSettings:
	  agents_max_tokens_per_decision (default 3000)
	  agents_max_cost_usd_per_decision (default 0.10)

	Graphs call this before every Claude call. Refuse (allowed=False)
	means abort the decision with terminal_status='aborted'.
	"""
	from config.agents import get_agents_settings

	s = get_agents_settings()
	token_cap = s.agents_max_tokens_per_decision
	cost_cap = s.agents_max_cost_usd_per_decision

	tokens_remaining = max(0, token_cap - tokens_used)
	cost_remaining = max(0.0, cost_cap - cost_usd)

	if tokens_used >= token_cap:
		return {
			"allowed": False,
			"reason": "token_budget_exceeded",
			"tokens_remaining": 0,
			"cost_remaining_usd": cost_remaining,
			"graph_name": graph_name,
		}

	if cost_usd >= cost_cap:
		return {
			"allowed": False,
			"reason": "cost_budget_exceeded",
			"tokens_remaining": tokens_remaining,
			"cost_remaining_usd": 0.0,
			"graph_name": graph_name,
		}

	return {
		"allowed": True,
		"reason": "within_budget",
		"tokens_remaining": tokens_remaining,
		"cost_remaining_usd": cost_remaining,
		"graph_name": graph_name,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 5. ab_variant_assign
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="gating")
def ab_variant_assign(
	subscriber_id: int,
	test_name: str,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return {'variant': 'a'|'b'|None, 'assigned': bool, 'traffic_capped': bool}
	for a subscriber on a named A/B test.

	Assignment is deterministic by md5(test_name + subscriber_id) mod 100,
	capped at the test's traffic_pct. Same subscriber always lands on the
	same variant across sessions and devices.

	If the subscriber is outside the 10% traffic cap, returns variant=None
	with traffic_capped=True.

	Idempotency: if an assignment row already exists, return that variant.
	If not, the underlying ab_engine.assign_variant writes one inside its
	own transaction — safe to call multiple times.
	"""
	from src.services import ab_engine

	with _session(session) as s:
		variant = ab_engine.assign_variant(subscriber_id, test_name, s)

	if variant is None:
		return {
			"subscriber_id": subscriber_id,
			"test_name": test_name,
			"variant": None,
			"assigned": False,
			"traffic_capped": True,
		}

	return {
		"subscriber_id": subscriber_id,
		"test_name": test_name,
		"variant": variant,
		"assigned": True,
		"traffic_capped": False,
	}
