"""
Read tools for Cora graphs.

These are the 12 read-only queries graphs use to assemble decision context.
Every tool here is a thin wrapper over existing platform services or a
straightforward Postgres query. No side effects. No external API calls
beyond the database and Redis (which is already platform-shared).

Pattern:
  - Every tool accepts an optional SQLAlchemy Session. If not provided,
	the tool opens its own read-only session via db.session_scope().
  - Every tool returns a dict (or list of dicts) so the shape is stable
	across graph versions. No ORM objects leak out.
  - Every tool handles the "no data yet" case by returning empty dicts /
	sensible defaults rather than raising.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

LEARNING_CARD_CACHE_TTL = 8 * 86400  # 8 days — outlives weekly Sunday refresh


def _learning_card_key(card_type: str) -> str:
    return f"learning_card:{card_type}"

from src.agents.tools.registry import tool
from src.core.database import db
from src.core.models import (
	DealOutcome,
	LearningCard,
	MessageOutcome,
	Subscriber,
	UserSegment,
	WalletBalance,
	SmsOptIn,
	SmsOptOut,
	AbAssignment,
)


@contextmanager
def _session(provided: Optional[Session]) -> Generator[Session, None, None]:
	"""Use a caller-provided session or open a new scoped one."""
	if provided is not None:
		yield provided
		return
	with db.session_scope() as s:
		yield s


# ──────────────────────────────────────────────────────────────────────────────
# 1. Subscriber profile
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_subscriber_profile(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return a subscriber's profile as a flat dict. Empty dict if not found."""
	with _session(session) as s:
		sub = s.query(Subscriber).filter(Subscriber.id == subscriber_id).first()
		if sub is None:
			return {}
		return {
			"id": sub.id,
			"tier": sub.tier,
			"status": sub.status,
			"vertical": sub.vertical,
			"county_id": sub.county_id,
			"founding_member": sub.founding_member,
			"email": sub.email,
			"name": sub.name,
			"phone": getattr(sub, "phone", None),
			"has_saved_card": sub.has_saved_card,
			"auto_mode_enabled": sub.auto_mode_enabled,
			"referral_code": sub.referral_code,
			"wallet_opt_out": bool(getattr(sub, "wallet_opt_out", False)),
			"missed_lead_count": int(getattr(sub, "missed_lead_count", 0) or 0),
			"created_at": sub.created_at.isoformat() if sub.created_at else None,
			"billing_date": sub.billing_date.isoformat() if sub.billing_date else None,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 2. Segment and revenue signal score
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_segment_and_score(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return the subscriber's current bucket, 0–100 revenue signal score, and fa037 freshness fields."""
	with _session(session) as s:
		seg = (
			s.query(UserSegment)
			.filter(UserSegment.subscriber_id == subscriber_id)
			.first()
		)
		if seg is None:
			return {
				"segment": "new",
				"revenue_signal_score": 0,
				"revenue_signal_band": None,
				"last_significant_action_at": None,
				"revenue_signal_last_action": None,
				"classified_at": None,
				"reason": None,
			}
		return {
			"segment": seg.segment,
			"revenue_signal_score": int(seg.revenue_signal_score or 0),
			# fa037 explainability fields (nullable until score-update event writes them)
			"revenue_signal_band": getattr(seg, "revenue_signal_band", None),
			"last_significant_action_at": (
				seg.last_significant_action_at.isoformat()
				if getattr(seg, "last_significant_action_at", None)
				else None
			),
			"revenue_signal_last_action": getattr(seg, "revenue_signal_last_action", None),
			"classified_at": seg.last_classified_at.isoformat() if seg.last_classified_at else None,
			"reason": seg.classification_reason,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 3. Wallet state
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_wallet_state(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return the subscriber's wallet tier, balance, usage, and auto-reload state."""
	with _session(session) as s:
		w = (
			s.query(WalletBalance)
			.filter(WalletBalance.subscriber_id == subscriber_id)
			.first()
		)
		if w is None:
			return {
				"enrolled": False,
				"tier": None,
				"credits_remaining": 0,
				"credits_used_total": 0,
				"auto_reload_enabled": False,
				"last_reload_at": None,
			}
		return {
			"enrolled": True,
			"tier": w.wallet_tier,
			"credits_remaining": w.credits_remaining,
			"credits_used_total": w.credits_used_total,
			"auto_reload_enabled": w.auto_reload_enabled,
			"last_reload_at": w.last_reload_at.isoformat() if w.last_reload_at else None,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 4. ZIP activity
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_zip_activity(
	zip_code: str,
	vertical: Optional[str] = None,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Live activity snapshot for a ZIP — active urgency-window count plus
	recent message-send volume from Cora into this ZIP's subscribers.

	The urgency count comes from Redis (via urgency_engine.get_active_count)
	and degrades gracefully when Redis is down.
	"""
	from src.services import urgency_engine

	active_viewers = urgency_engine.get_active_count(zip_code)

	with _session(session) as s:
		# Recent outbound messages to subscribers who match this ZIP's vertical.
		# This is a coarse signal — enough for graphs to reason about ZIP heat.
		cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
		q = (
			s.query(func.count(MessageOutcome.id))
			.join(Subscriber, Subscriber.id == MessageOutcome.subscriber_id)
			.filter(MessageOutcome.sent_at >= cutoff)
		)
		if vertical:
			q = q.filter(Subscriber.vertical == vertical)
		recent_message_count = int(q.scalar() or 0)

	return {
		"zip": zip_code,
		"vertical": vertical,
		"active_viewers": active_viewers,
		"messages_last_24h": recent_message_count,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 5. Lead pool
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_lead_pool(
	zip_code: str,
	vertical: Optional[str] = None,
	min_score: int = 0,
	limit: int = 25,
	session: Optional[Session] = None,
) -> List[Dict[str, Any]]:
	"""
	Return scored leads available in a ZIP. Filtered by vertical and minimum
	score. Results ordered by score descending.
	"""
	from src.core.models import DistressScore, Property

	with _session(session) as s:
		q = (
			s.query(DistressScore, Property)
			.join(Property, Property.id == DistressScore.property_id)
			.filter(Property.zip == zip_code)
			.filter(DistressScore.final_cds_score >= min_score)
		)
		rows = q.order_by(DistressScore.final_cds_score.desc()).limit(limit).all()

		return [
			{
				"property_id": prop.id,
				"address": prop.address,
				"zip": prop.zip,
				"score": float(score.final_cds_score or 0),
				"tier": score.lead_tier,
				"urgency_level": score.urgency_level,
				"scored_at": score.score_date.isoformat() if score.score_date else None,
			}
			for score, prop in rows
		]


# ──────────────────────────────────────────────────────────────────────────────
# 6. Competition status
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_competition_status(
	zip_code: str,
	vertical: Optional[str] = None,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Competitive snapshot for a ZIP: current lock holder (if any), active
	wallet users targeting this ZIP, and a heuristic "temperature" value.
	"""
	from src.core.models import ZipTerritory

	with _session(session) as s:
		lock_q = s.query(ZipTerritory).filter(ZipTerritory.zip_code == zip_code)
		if vertical:
			lock_q = lock_q.filter(ZipTerritory.vertical == vertical)
		lock_q = lock_q.filter(ZipTerritory.status == "locked")
		lock = lock_q.first()

		wallet_count_q = (
			s.query(func.count(WalletBalance.id))
			.join(Subscriber, Subscriber.id == WalletBalance.subscriber_id)
			.filter(Subscriber.status == "active")
		)
		if vertical:
			wallet_count_q = wallet_count_q.filter(Subscriber.vertical == vertical)
		active_wallet_count = int(wallet_count_q.scalar() or 0)

	lock_holder_id = getattr(lock, "subscriber_id", None) if lock else None

	return {
		"zip": zip_code,
		"vertical": vertical,
		"lock_holder_subscriber_id": lock_holder_id,
		"is_locked": lock_holder_id is not None,
		"active_wallet_users_in_vertical": active_wallet_count,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 7. Recent messages (to avoid repetition)
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_recent_messages(
	subscriber_id: int,
	hours: int = 72,
	session: Optional[Session] = None,
) -> List[Dict[str, Any]]:
	"""Return recent outbound messages for a subscriber. Used to prevent repetition."""
	with _session(session) as s:
		cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
		rows = (
			s.query(MessageOutcome)
			.filter(MessageOutcome.subscriber_id == subscriber_id)
			.filter(MessageOutcome.sent_at >= cutoff)
			.order_by(MessageOutcome.sent_at.desc())
			.limit(50)
			.all()
		)
		return [
			{
				"id": r.id,
				"message_type": r.message_type,
				"template_id": r.template_id,
				"variant_id": r.variant_id,
				"channel": r.channel,
				"sent_at": r.sent_at.isoformat(),
				"conversion_type": r.conversion_type,
			}
			for r in rows
		]


# ──────────────────────────────────────────────────────────────────────────────
# 8. Latest learning card
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_learning_card(
	card_type: str = "general",
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return the most recent learning card of the given type. Empty dict if
	no card exists yet (Sunday job hasn't run).

	Card types: message_perf, deal_pattern, ab_result, churn_signal,
	pricing_test, general.

	Read path is Redis-fronted (key=learning_card:{card_type}, TTL 8d) to
	keep the decision_hierarchy hot path under the M5 <100 ms SLA. Cache
	misses fall through to Postgres and backfill the key; Redis failures
	degrade silently to a direct DB read.
	"""
	from src.core.redis_client import rget, rset

	key = _learning_card_key(card_type)
	cached = rget(key)
	if cached:
		try:
			return json.loads(cached)
		except (ValueError, TypeError) as exc:
			logger.warning("learning_card cache decode failed for %s: %s", key, exc)

	with _session(session) as s:
		card = (
			s.query(LearningCard)
			.filter(LearningCard.card_type == card_type)
			.order_by(LearningCard.card_date.desc())
			.first()
		)
		if card is None:
			return {}
		payload = {
			"card_date": card.card_date.isoformat(),
			"card_type": card.card_type,
			"summary_text": card.summary_text,
			"data": card.data_json or {},
			"action_taken": card.action_taken,
		}
	rset(key, json.dumps(payload), ttl_seconds=LEARNING_CARD_CACHE_TTL)
	return payload


# ──────────────────────────────────────────────────────────────────────────────
# 9. Guardrail lookup
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_guardrail(name: str) -> Dict[str, Any]:
	"""
	Return the numeric bound config for a named guardrail.

	Valid names are the keys in config/cora_guardrails.py GUARDRAILS.
	Returns an empty dict for unknown names (graphs treat absence as
	"fall back to conservative default").
	"""
	from config.cora_guardrails import GUARDRAILS

	return dict(GUARDRAILS.get(name, {}))


# ──────────────────────────────────────────────────────────────────────────────
# 10. A/B variant assignment (read — determinism-dependent lookup)
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_ab_variant(
	subscriber_id: int,
	test_name: str,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return the already-assigned A/B variant for this (subscriber, test).
	Empty dict if the subscriber isn't in the test yet. Assignment itself is
	a gating tool — see ab_variant_assign in gating_tools.
	"""
	from src.core.models import AbTest

	with _session(session) as s:
		row = (
			s.query(AbAssignment)
			.join(AbTest, AbTest.id == AbAssignment.test_id)
			.filter(AbAssignment.subscriber_id == subscriber_id)
			.filter(AbTest.test_name == test_name)
			.first()
		)
		if row is None:
			return {}
		return {
			"subscriber_id": subscriber_id,
			"test_name": test_name,
			"variant": row.variant,
			"outcome": row.outcome,
			"assigned_at": row.created_at.isoformat() if row.created_at else None,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 11. Deal history
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_deal_history(
	subscriber_id: int,
	limit: int = 20,
	session: Optional[Session] = None,
) -> List[Dict[str, Any]]:
	"""Return the subscriber's reported deals, newest first."""
	with _session(session) as s:
		rows = (
			s.query(DealOutcome)
			.filter(DealOutcome.subscriber_id == subscriber_id)
			.order_by(DealOutcome.created_at.desc())
			.limit(limit)
			.all()
		)
		return [
			{
				"id": r.id,
				"deal_bucket": r.deal_size_bucket,
				"deal_amount": float(r.deal_amount or 0),
				"deal_date": r.deal_date.isoformat() if r.deal_date else None,
				"days_to_close": r.days_to_close,
				"lead_source": r.lead_source,
				"created_at": r.created_at.isoformat() if r.created_at else None,
			}
			for r in rows
		]


# ──────────────────────────────────────────────────────────────────────────────
# 12. Opt-in check
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def check_opt_in(
	phone: str,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return TCPA opt-in status for a phone number. Mirrors what the
	compliance gate does, but exposed as a read tool so graphs can decide
	early whether to spend tokens composing a message we cannot send.
	"""
	with _session(session) as s:
		opt_in = (
			s.query(SmsOptIn)
			.filter(SmsOptIn.phone == phone)
			.order_by(SmsOptIn.opted_in_at.desc())
			.first()
		)
		opt_out = (
			s.query(SmsOptOut)
			.filter(SmsOptOut.phone == phone)
			.order_by(SmsOptOut.opted_out_at.desc())
			.first()
		)

		has_optin = opt_in is not None
		has_optout = opt_out is not None
		# Opt-out always wins, regardless of opt-in date.
		can_send = has_optin and not has_optout

		return {
			"phone": phone,
			"has_opt_in": has_optin,
			"has_opt_out": has_optout,
			"can_send_marketing": can_send,
			"opt_in_keyword": opt_in.keyword_used if opt_in else None,
			"opt_in_source": opt_in.source if opt_in else None,
			"opt_in_at": opt_in.opted_in_at.isoformat() if opt_in and opt_in.opted_in_at else None,
			"opt_out_keyword": opt_out.keyword_used if opt_out else None,
			"opt_out_at": opt_out.opted_out_at.isoformat() if opt_out and opt_out.opted_out_at else None,
		}


@tool(category="read")
def get_subscriber_territories(
	subscriber_id: int,
	status: str = "locked",
	session: Optional[Session] = None,
) -> list:
	"""Return ZIP codes for a subscriber's territories in the given status (default: locked)."""
	from src.core.models import ZipTerritory
	with _session(session) as s:
		rows = (
			s.query(ZipTerritory.zip_code)
			.filter(
				ZipTerritory.subscriber_id == subscriber_id,
				ZipTerritory.status == status,
			)
			.all()
		)
		return [r[0] for r in rows]


# ──────────────────────────────────────────────────────────────────────────────
# 13. Attribution context (Stage 8)
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_attribution_context(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return attribution and revenue signal context for a subscriber.

	Reads:
	  - Latest score fields from subscribers
	  - Last 5 conversion_attribution_events
	  - Last 5 deal_outcomes
	  - Active (locked) zip_territories

	Returns a dict safe for merging into Cora personalization context.
	All DB access is raw SQL.
	"""
	from sqlalchemy import text as sa_text

	with _session(session) as s:
		# Latest score state.
		sub_row = s.execute(sa_text("""
			SELECT revenue_signal_score, revenue_signal_band,
			       revenue_signal_breakdown, revenue_signal_updated_at
			FROM subscribers WHERE id = :sub_id
		"""), {"sub_id": subscriber_id}).mappings().first()

		score = int(sub_row["revenue_signal_score"]) if sub_row else 0
		band = sub_row["revenue_signal_band"] if sub_row else "low"
		breakdown = sub_row["revenue_signal_breakdown"] if sub_row else {}

		# Last 5 conversion attribution events.
		attr_rows = s.execute(sa_text("""
			SELECT conversion_type, occurred_at, zip_code, wallet_tier,
			       lock_status, deal_size_bucket, revenue_amount
			FROM conversion_attribution_events
			WHERE subscriber_id = :sub_id
			ORDER BY occurred_at DESC
			LIMIT 5
		"""), {"sub_id": subscriber_id}).mappings().all()

		recent_conversions = [
			{
				"conversion_type": r["conversion_type"],
				"occurred_at": r["occurred_at"].isoformat() if r["occurred_at"] else None,
				"zip_code": r["zip_code"],
			}
			for r in attr_rows
		]
		recent_conversion_types = [r["conversion_type"] for r in attr_rows[:3]]

		# Last 5 deal outcomes.
		deal_rows = s.execute(sa_text("""
			SELECT deal_size_bucket, deal_date, deal_amount
			FROM deal_outcomes
			WHERE subscriber_id = :sub_id
			ORDER BY deal_date DESC NULLS LAST
			LIMIT 5
		"""), {"sub_id": subscriber_id}).mappings().all()

		deal_history = [
			{
				"deal_size_bucket": r["deal_size_bucket"],
				"deal_date": r["deal_date"].isoformat() if r["deal_date"] else None,
				"deal_amount": float(r["deal_amount"]) if r["deal_amount"] else None,
			}
			for r in deal_rows
		]

		# Active locked territories.
		zip_rows = s.execute(sa_text("""
			SELECT zip_code FROM zip_territories
			WHERE subscriber_id = :sub_id AND status = 'locked'
			ORDER BY locked_at DESC
		"""), {"sub_id": subscriber_id}).mappings().all()

		lock_zips = [r["zip_code"] for r in zip_rows]

		return {
			"revenue_signal_score": score,
			"revenue_signal_band": band,
			"revenue_signal_breakdown": breakdown or {},
			"recent_conversions": recent_conversions,
			"recent_conversion_types": recent_conversion_types,
			"lock_zips": lock_zips,
			"deal_history": deal_history,
		}
